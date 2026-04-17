// ScriptFlow Worker v2 - auto-deploy trigger
console.log('[worker] PIAPI_API_KEY length:', process.env.PIAPI_API_KEY?.length)
console.log('[worker] PIAPI_API_KEY first 8:', process.env.PIAPI_API_KEY?.slice(0, 8))

const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const MAX_RETRIES = 3
const RAILWAY_FFMPEG_URL = process.env.RAILWAY_FFMPEG_URL || 'https://scriptflow-video-merge-production.up.railway.app'

// BUG 4: Timeout protection on all API calls
const fetchWithTimeout = async (url, options = {}, ms = 10000) => {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), ms)
  try {
    const res = await fetch(url, { ...options, signal: controller.signal })
    clearTimeout(timer)
    return res
  } catch (e) {
    clearTimeout(timer)
    throw e
  }
}

async function pollShots() {
  // Stage 1: Submit Kling tasks for pending shots with no kling_task_id
  const { data: pendingShots } = await supabase
    .from('movie_shots')
    .select('*')
    .in('status', ['pending', 'submitted'])
    .is('kling_task_id', null)
    .limit(10)

  if (pendingShots && pendingShots.length > 0) {
    console.log('[worker] Stage 1: found', pendingShots.length, 'shots missing kling_task_id')
    for (const shot of pendingShots) {
      try {
        const scenePrompt = shot.scene_description || shot.description || shot.scene || 'cinematic empty scene, dramatic lighting, no people, no humans'
        const klingRes = await fetchWithTimeout('https://api.piapi.ai/api/v1/task', {
          method: 'POST',
          headers: {
            'x-api-key': process.env.PIAPI_API_KEY,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            model: 'kling',
            task_type: 'video_generation',
            input: {
              prompt: scenePrompt,
              negative_prompt: 'people, humans, faces',
              aspect_ratio: '9:16',
              duration: shot.duration || 10
            },
            config: {
              webhook_config: {
                endpoint: `${process.env.NEXT_PUBLIC_APP_URL}/api/webhook/piapi`,
                secret: ''
              }
            }
          })
        }, 15000)
        const klingData = await klingRes.json()
        const klingTaskId = klingData?.data?.task_id
        if (klingTaskId) {
          await supabase.from('movie_shots')
            .update({ kling_task_id: klingTaskId, status: 'submitted', updated_at: new Date().toISOString() })
            .eq('id', shot.id)
          console.log('[worker] Kling submitted for shot:', shot.shot_index, 'task_id:', klingTaskId)
        } else {
          console.warn('[worker] Kling submission returned no task_id for shot:', shot.shot_index, JSON.stringify(klingData).slice(0, 200))
        }
      } catch (e) {
        console.error('[worker] Kling submission error for shot:', shot.shot_index, e.message)
      }
    }
  }

  // BUG 5: Daily render limit check
  const today = new Date().toISOString().split('T')[0]
  const { count: todayRenders } = await supabase
    .from('movie_shots')
    .select('*', { count: 'exact', head: true })
    .eq('status', 'merging')
    .gte('created_at', today)

  if (todayRenders > 100) {
    console.warn('[worker] Daily render limit reached (100), pausing submissions')
    return
  }

  // BUG 5: Fetch shots grouped by movie, process max 3 per movie per cycle
  const { data: shots } = await supabase
    .from('movie_shots')
    .select('*')
    .in('status', ['submitted', 'processing', 'merging'])
    .order('movie_id')
    .limit(60)

  if (!shots || shots.length === 0) {
    // Still run stuck-shot recovery even if no active shots
  } else {
    // Group by movie_id, cap at 3 shots per movie
    const shotsByMovie = {}
    for (const shot of shots) {
      const mid = shot.movie_id ?? 'unknown'
      if (!shotsByMovie[mid]) shotsByMovie[mid] = []
      if (shotsByMovie[mid].length < 3) shotsByMovie[mid].push(shot)
    }
    const cappedShots = Object.values(shotsByMovie).flat()

    console.log('[worker] polling', cappedShots.length, 'shots across', Object.keys(shotsByMovie).length, 'movies')

    // Poll OmniHuman status
    for (const shot of cappedShots) {
      if (!shot.omni_task_id) continue

      try {
        const res = await fetchWithTimeout('https://api.piapi.ai/api/v1/task/' + shot.omni_task_id, {
          headers: { 'x-api-key': process.env.PIAPI_API_KEY }
        })
        const data = await res.json()
        console.log('[worker] PiAPI raw response:', JSON.stringify(data).slice(0, 500))
        const status = data?.data?.status
        const videoUrl = data?.data?.output?.video

        console.log('[worker] shot', shot.shot_index, 'omni status:', status)

        if ((status === 'completed' || status === 'success') && videoUrl) {
          await supabase.from('movie_shots')
            .update({ omni_video_url: videoUrl, status: 'processing', updated_at: new Date().toISOString() })
            .eq('id', shot.id)
          await supabase.from('omnihuman_jobs')
            .update({ status: 'completed', result_video_url: videoUrl })
            .eq('task_id', shot.omni_task_id)
          console.log('[worker] OmniHuman done for shot', shot.shot_index)
        }
      } catch (e) {
        console.error('[worker] error polling shot', shot.id, e.message)
      }
    }

    // Poll Kling status for shots with kling_task_id
    for (const shot of cappedShots) {
      if (!shot.kling_task_id) continue

      try {
        const res = await fetchWithTimeout('https://api.piapi.ai/api/v1/task/' + shot.kling_task_id, {
          headers: { 'x-api-key': process.env.PIAPI_API_KEY }
        })
        const data = await res.json()
        const status = data?.data?.status
        const videoUrl = data?.data?.output?.works?.[0]?.resource?.resource

        console.log('[worker] shot', shot.shot_index, 'kling status:', status)

        if ((status === 'completed' || status === 'success') && videoUrl) {
          await supabase.from('movie_shots')
            .update({ kling_scene_url: videoUrl, updated_at: new Date().toISOString() })
            .eq('id', shot.id)
          console.log('[worker] Kling done for shot', shot.shot_index)
        }
      } catch (e) {
        console.error('[worker] kling poll error:', shot.id, e.message)
      }
    }

    // Check if shots are ready, trigger FFmpeg merge
    // Face shots: only need omni done
    const { data: faceShots } = await supabase
      .from('movie_shots')
      .select('*')
      .eq('status', 'processing')
      .eq('shot_type', 'face')
      .not('omni_video_url', 'is', null)

    // Scene shots: only need kling done
    const { data: sceneShots } = await supabase
      .from('movie_shots')
      .select('*')
      .eq('status', 'processing')
      .eq('shot_type', 'scene')
      .not('kling_scene_url', 'is', null)

    const readyShots = [...(faceShots ?? []), ...(sceneShots ?? [])]

    for (const shot of readyShots) {
      // BUG 2: Permanently fail shots with too many retries
      if ((shot.retry_count ?? 0) >= MAX_RETRIES) {
        await supabase.from('movie_shots')
          .update({ status: 'failed', updated_at: new Date().toISOString() })
          .eq('id', shot.id)
        console.log('[worker] Shot permanently failed after max retries:', shot.shot_index)
        continue
      }

      // BUG 6: Optimistic locking - only proceed if status hasn't changed
      const { data: locked } = await supabase
        .from('movie_shots')
        .update({ status: 'merging', updated_at: new Date().toISOString() })
        .eq('id', shot.id)
        .eq('status', shot.status)
        .select()

      if (!locked || locked.length === 0) {
        console.log('[worker] Shot already being processed by another worker, skipping:', shot.shot_index)
        continue
      }

      console.log('[worker] Ready for shot', shot.shot_index, 'type:', shot.shot_type, '- triggering FFmpeg merge')

      // For face shots: merge omni video + kling audio
      // For scene shots: use kling video directly
      try {
        const mergeRes = await fetchWithTimeout(
          RAILWAY_FFMPEG_URL + '/merge-videos',
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              videoUrl: shot.shot_type === 'face' ? shot.omni_video_url : shot.kling_scene_url,
              audioUrl: shot.audio_url ?? null
            })
          },
          30000
        )
        const mergeData = await mergeRes.json()
        if (mergeData.outputUrl) {
          await supabase.from('movie_shots')
            .update({ final_shot_url: mergeData.outputUrl, status: 'done', updated_at: new Date().toISOString() })
            .eq('id', shot.id)
          console.log('[worker] Shot merged via FFmpeg:', shot.shot_index)
        } else {
          console.error('[worker] FFmpeg merge no outputUrl:', JSON.stringify(mergeData))
          // Reset to processing so it can be retried
          const newRetryCount = (shot.retry_count ?? 0) + 1
          await supabase.from('movie_shots')
            .update({ status: 'processing', retry_count: newRetryCount, updated_at: new Date().toISOString() })
            .eq('id', shot.id)
        }
      } catch (e) {
        console.error('[worker] FFmpeg merge error:', e.message)
        // Reset to processing so it can be retried
        const newRetryCount = (shot.retry_count ?? 0) + 1
        await supabase.from('movie_shots')
          .update({ status: 'processing', retry_count: newRetryCount, updated_at: new Date().toISOString() })
          .eq('id', shot.id)
      }
    }
  }

  // Stage 5: Assemble complete movies
  // BUG 3: Also fetch shots with 'failed' status so we can detect terminal state
  const { data: terminalShots } = await supabase
    .from('movie_shots')
    .select('movie_id')
    .in('status', ['done', 'failed'])

  if (terminalShots && terminalShots.length > 0) {
    const movieIds = [...new Set(terminalShots.map(s => s.movie_id))]

    for (const movieId of movieIds) {
      // Check if all shots for this movie are in a terminal state (done or failed)
      const { data: allShots } = await supabase
        .from('movie_shots')
        .select('*')
        .eq('movie_id', movieId)
        .order('shot_index')

      // BUG 3: Accept 'failed' as terminal state so one bad shot doesn't block the movie
      const allTerminal = allShots?.every(s => ['done', 'failed', 'final_complete'].includes(s.status))
      if (!allTerminal) continue

      // BUG 5: Warn if movie has been stuck for >30 minutes
      const oldestUpdated = allShots?.reduce((oldest, s) => {
        const t = new Date(s.updated_at ?? s.created_at ?? 0).getTime()
        return t < oldest ? t : oldest
      }, Date.now())
      const stuckMinutes = (Date.now() - oldestUpdated) / 60000
      if (stuckMinutes > 30) {
        console.warn('[worker] WARNING: Movie stuck for', Math.round(stuckMinutes), 'minutes:', movieId)
      }

      // Check if movie already being assembled
      const { data: existingMovie } = await supabase
        .from('movies')
        .select('*')
        .eq('id', movieId)
        .single()

      if (existingMovie?.status === 'rendering' || existingMovie?.status === 'complete') continue

      // BUG 3: Only use successful shots in the timeline, skip failed ones
      const successShots = allShots?.filter(s => s.status === 'done')
      const failedCount = allShots?.filter(s => s.status === 'failed').length ?? 0

      if (!successShots || successShots.length === 0) {
        console.log('[worker] Movie has no successful shots, skipping assembly:', movieId)
        continue
      }

      console.log('[worker] Assembling movie:', movieId, 'shots:', successShots.length, '(skipping', failedCount, 'failed)')

      // Mark movie as rendering before calling FFmpeg
      const { error: movieError } = await supabase.from('movies').upsert({
        id: movieId,
        status: 'rendering',
        total_shots: allShots.length,
        updated_at: new Date().toISOString()
      }, { onConflict: 'id' })

      if (movieError) {
        console.error('[worker] Movie upsert error:', movieError.message)
        continue
      }

      // Build list of final shot URLs in order
      const clips = successShots.map((shot) => ({
        asset: { type: 'video', src: shot.final_shot_url }
      }))

      try {
        const concatRes = await fetchWithTimeout(
          RAILWAY_FFMPEG_URL + '/concat-videos',
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              videoUrls: clips.map(c => c.asset.src),
              outputName: `movie_${movieId}`
            })
          },
          120000
        )
        const concatData = await concatRes.json()
        if (concatData.outputUrl) {
          await supabase.from('movies')
            .update({ status: 'complete', final_video_url: concatData.outputUrl })
            .eq('id', movieId)
          // Sync movie_shots status after movie completes
          await supabase.from('movie_shots')
            .update({ status: 'final_complete' })
            .eq('movie_id', movieId)
            .eq('status', 'done')
          console.log('[worker] Movie complete via FFmpeg:', movieId)
        } else {
          console.error('[worker] FFmpeg concat no outputUrl:', JSON.stringify(concatData))
          await supabase.from('movies')
            .update({ status: 'failed', updated_at: new Date().toISOString() })
            .eq('id', movieId)
        }
      } catch (e) {
        console.error('[worker] FFmpeg concat error:', e.message)
        await supabase.from('movies')
          .update({ status: 'failed', updated_at: new Date().toISOString() })
          .eq('id', movieId)
      }
    }
  }

  // BUG 7: Reset shots stuck in 'submitted' for >15 minutes
  const fifteenMinutesAgo = new Date(Date.now() - 15 * 60 * 1000).toISOString()
  const { data: stuckSubmitted, error: stuckSubmittedErr } = await supabase
    .from('movie_shots')
    .select('id, shot_index, retry_count')
    .eq('status', 'submitted')
    .lt('updated_at', fifteenMinutesAgo)

  if (stuckSubmitted && stuckSubmitted.length > 0) {
    console.warn('[worker] Found', stuckSubmitted.length, 'shots stuck in submitted >15min, resetting...')
    for (const stuck of stuckSubmitted) {
      const newRetry = (stuck.retry_count ?? 0) + 1
      if (newRetry >= MAX_RETRIES) {
        await supabase.from('movie_shots')
          .update({ status: 'failed', retry_count: newRetry, updated_at: new Date().toISOString() })
          .eq('id', stuck.id)
        console.log('[worker] Stuck submitted shot permanently failed:', stuck.shot_index)
      } else {
        await supabase.from('movie_shots')
          .update({ status: 'pending', omni_task_id: null, kling_task_id: null, retry_count: newRetry, updated_at: new Date().toISOString() })
          .eq('id', stuck.id)
        console.log('[worker] Reset stuck submitted shot:', stuck.shot_index, '(retry', newRetry, ')')
      }
    }
  }
  if (stuckSubmittedErr) console.error('[worker] Stuck submitted query error:', stuckSubmittedErr.message)

  // BUG 7: Reset shots stuck in 'processing' for >20 minutes
  const twentyMinutesAgo = new Date(Date.now() - 20 * 60 * 1000).toISOString()
  const { data: stuckProcessing, error: stuckProcessingErr } = await supabase
    .from('movie_shots')
    .select('id, shot_index')
    .eq('status', 'processing')
    .lt('updated_at', twentyMinutesAgo)

  if (stuckProcessing && stuckProcessing.length > 0) {
    console.warn('[worker] Found', stuckProcessing.length, 'shots stuck in processing >20min, resetting to submitted...')
    const stuckIds = stuckProcessing.map(s => s.id)
    await supabase.from('movie_shots')
      .update({ status: 'submitted', updated_at: new Date().toISOString() })
      .in('id', stuckIds)
  }
  if (stuckProcessingErr) console.error('[worker] Stuck processing query error:', stuckProcessingErr.message)
}

async function main() {
  console.log('[worker] Starting ScriptFlow Worker...')
  console.log('[worker] Supabase URL:', process.env.NEXT_PUBLIC_SUPABASE_URL?.slice(0, 30))
  console.log('[worker] PIAPI key present:', !!process.env.PIAPI_API_KEY)
  console.log('[worker] Railway FFmpeg URL:', RAILWAY_FFMPEG_URL)

  while (true) {
    try {
      console.log('[worker] polling...')
      await pollShots()
      console.log('[worker] poll complete')
    } catch (e) {
      console.error('[worker] FATAL ERROR:', e.message)
      console.error('[worker] Stack:', e.stack)
    }
    await new Promise(r => setTimeout(r, 5000))
  }
}

main()
// redeploy Fri Apr 17 14:54:14 +07 2026
// v3 Fri Apr 17 15:02:45 +07 2026
