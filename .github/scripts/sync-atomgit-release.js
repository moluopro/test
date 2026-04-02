#!/usr/bin/env node

const { Readable } = require('node:stream')

if (!process.env.GITHUB_TOKEN && process.env.GH_TOKEN) {
  process.env.GITHUB_TOKEN = process.env.GH_TOKEN
}

const dryRun = ['1', 'true', 'yes'].includes((process.env.DRY_RUN || '').trim().toLowerCase())

const requiredEnv = ['GITHUB_TOKEN']

if (!dryRun) {
  requiredEnv.push('ATOMGIT_TOKEN')
}

for (const name of requiredEnv) {
  if (!process.env[name]) {
    console.error(`[atomgit-sync] missing env: ${name}`)
    process.exit(1)
  }
}

const GITHUB_API_BASE = 'https://api.github.com'
const ATOMGIT_API_BASE = (process.env.ATOMGIT_API_BASE || 'https://api.atomgit.com').replace(
  /\/+$/,
  ''
)

const githubOwner = process.env.GITHUB_OWNER || 'clickclaw'
const githubRepo = process.env.GITHUB_REPO || 'clickclaw'
const atomgitOwner = process.env.ATOMGIT_OWNER || 'clickclaw'
const atomgitRepo = process.env.ATOMGIT_REPO || 'clickclaw'
const maxSyncFiles = (() => {
  const parsed = Number(process.env.MAX_SYNC_FILES || '100')
  if (!Number.isFinite(parsed) || parsed <= 0) return 100
  return Math.floor(parsed)
})()

function logDryRun(message) {
  console.log(`[atomgit-sync][dry-run] ${message}`)
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) return 'unknown'

  const units = ['B', 'KB', 'MB', 'GB']
  let value = bytes
  let unitIndex = 0

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex += 1
  }

  return `${value.toFixed(value >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`
}

function createProgressTracker(kind, name, totalBytes) {
  let lastPercentBucket = -1
  let lastLoggedBytes = 0

  return (processedBytes) => {
    if (!Number.isFinite(processedBytes)) return

    if (Number.isFinite(totalBytes) && totalBytes > 0) {
      const percent = Math.min(100, Math.floor((processedBytes / totalBytes) * 100))
      const bucket = percent

      if (bucket > lastPercentBucket) {
        lastPercentBucket = bucket
        console.log(
          `[atomgit-sync] ${kind} ${name}: ${percent}% (${formatBytes(processedBytes)}/${formatBytes(totalBytes)})`
        )
      }
      return
    }

    const step = 5 * 1024 * 1024
    if (processedBytes - lastLoggedBytes >= step) {
      lastLoggedBytes = processedBytes
      console.log(`[atomgit-sync] ${kind} ${name}: ${formatBytes(processedBytes)}`)
    }
  }
}

async function requestJson(url, options = {}, { allow404 = false } = {}) {
  const response = await fetch(url, options)
  if (allow404 && response.status === 404) return null

  if (!response.ok) {
    const text = await response.text()
    if (allow404) {
      try {
        const payload = JSON.parse(text)
        const message = String(payload?.error_message || '')
        if (message.includes('Release Not Found') || message.includes('404')) {
          return null
        }
      } catch {
        // ignore json parse error and throw the original response error below
      }
    }
    throw new Error(`${options.method || 'GET'} ${url} failed: ${response.status} ${text}`)
  }

  if (response.status === 204) return null
  return response.json()
}

function githubHeaders(extra = {}) {
  return {
    Accept: 'application/vnd.github+json',
    Authorization: `Bearer ${process.env.GITHUB_TOKEN}`,
    'X-GitHub-Api-Version': '2022-11-28',
    ...extra,
  }
}

function atomgitHeaders(extra = {}) {
  return {
    Accept: 'application/json',
    Authorization: `Bearer ${process.env.ATOMGIT_TOKEN}`,
    'PRIVATE-TOKEN': process.env.ATOMGIT_TOKEN,
    ...extra,
  }
}

function buildAtomGitApiUrl(path, query = {}) {
  const url = new URL(`${ATOMGIT_API_BASE}${path}`)
  const accessToken = process.env.ATOMGIT_TOKEN

  if (accessToken) {
    url.searchParams.set('access_token', accessToken)
  }

  for (const [key, value] of Object.entries(query)) {
    if (value == null || value === '') continue
    url.searchParams.set(key, String(value))
  }

  return url.toString()
}

async function getGithubReleasesOldestFirst() {
  const releases = []
  const perPage = 100

  for (let page = 1; ; page += 1) {
    const url = `${GITHUB_API_BASE}/repos/${encodeURIComponent(githubOwner)}/${encodeURIComponent(githubRepo)}/releases?per_page=${perPage}&page=${page}`
    const chunk = await requestJson(url, {
      headers: githubHeaders(),
    })

    if (!Array.isArray(chunk) || chunk.length === 0) {
      break
    }

    releases.push(...chunk)

    if (chunk.length < perPage) {
      break
    }
  }

  releases.sort((left, right) => {
    const leftTime = Date.parse(left?.published_at || left?.created_at || 0) || 0
    const rightTime = Date.parse(right?.published_at || right?.created_at || 0) || 0
    return leftTime - rightTime
  })

  return releases
}

async function getAtomGitRelease(tag) {
  if (dryRun) {
    logDryRun(`would query AtomGit release by tag: ${tag}`)
    return null
  }

  const url = buildAtomGitApiUrl(
    `/api/v5/repos/${encodeURIComponent(atomgitOwner)}/${encodeURIComponent(atomgitRepo)}/releases/${encodeURIComponent(tag)}`
  )
  return requestJson(
    url,
    {
      headers: atomgitHeaders(),
    },
    { allow404: true }
  )
}

function buildReleasePayload(release) {
  return {
    tag_name: release.tag_name,
    target_commitish: release.target_commitish,
    name: release.name,
    body: release.body || '',
    draft: Boolean(release.draft),
    prerelease: Boolean(release.prerelease),
  }
}

async function createOrUpdateAtomGitRelease(githubRelease) {
  const releaseTag = githubRelease.tag_name
  const existing = await getAtomGitRelease(releaseTag)
  const payload = buildReleasePayload(githubRelease)

  if (dryRun) {
    logDryRun(
      `${existing ? 'would update' : 'would create'} AtomGit release ${releaseTag} with name "${payload.name || ''}"`
    )
    return {
      tag_name: payload.tag_name,
      name: payload.name,
      body: payload.body,
      assets: [],
    }
  }

  if (!existing) {
    const url = buildAtomGitApiUrl(
      `/api/v5/repos/${encodeURIComponent(atomgitOwner)}/${encodeURIComponent(atomgitRepo)}/releases`
    )
    const created = await requestJson(url, {
      method: 'POST',
      headers: atomgitHeaders({
        'Content-Type': 'application/json',
      }),
      body: JSON.stringify(payload),
    })
    console.log(`[atomgit-sync] created release ${releaseTag}`)
    return created
  }

  const url = buildAtomGitApiUrl(
    `/api/v5/repos/${encodeURIComponent(atomgitOwner)}/${encodeURIComponent(atomgitRepo)}/releases/${encodeURIComponent(releaseTag)}`
  )
  const updated = await requestJson(url, {
    method: 'PATCH',
    headers: atomgitHeaders({
      'Content-Type': 'application/json',
    }),
    body: JSON.stringify(payload),
  })
  console.log(`[atomgit-sync] updated release ${releaseTag}`)
  return updated
}

function readUploadUrlPayload(payload) {
  if (!payload) return null
  if (typeof payload === 'string') return payload
  if (typeof payload.upload_url === 'string') return payload.upload_url
  if (typeof payload.url === 'string') return payload.url
  if (typeof payload.href === 'string') return payload.href
  return null
}

function normalizeUploadHeaders(headers) {
  if (!headers || typeof headers !== 'object') return {}

  const normalized = {}
  for (const [key, value] of Object.entries(headers)) {
    if (value == null) continue
    normalized[key] = String(value)
  }
  return normalized
}

function applyAssetName(uploadUrl, fileName) {
  if (!uploadUrl.includes('{')) {
    const joinChar = uploadUrl.includes('?') ? '&' : '?'
    return `${uploadUrl}${joinChar}name=${encodeURIComponent(fileName)}`
  }

  return uploadUrl.replace(/\{\?name(?:,label)?\}/, `?name=${encodeURIComponent(fileName)}`)
}

async function getAtomGitUploadUrl(fileName) {
  const releaseTag = (process.env.RELEASE_TAG || '').trim()
  if (!releaseTag) {
    throw new Error('missing RELEASE_TAG when requesting AtomGit upload url')
  }

  if (dryRun) {
    logDryRun(`would request AtomGit upload url for release ${releaseTag} and file ${fileName}`)
    return {
      url: 'https://example.invalid/upload',
      headers: {
        'Content-Type': 'application/octet-stream',
      },
    }
  }

  const url = buildAtomGitApiUrl(
    `/api/v5/repos/${encodeURIComponent(atomgitOwner)}/${encodeURIComponent(atomgitRepo)}/releases/${encodeURIComponent(releaseTag)}/upload_url`,
    {
      file_name: fileName,
    }
  )
  const payload = await requestJson(url, {
    headers: atomgitHeaders(),
  })
  const uploadUrl = readUploadUrlPayload(payload)

  if (!uploadUrl) {
    throw new Error(`unexpected AtomGit upload_url payload: ${JSON.stringify(payload)}`)
  }

  return {
    url: uploadUrl,
    headers: normalizeUploadHeaders(payload.headers),
  }
}

async function downloadGithubAsset(asset) {
  const response = await fetch(asset.url, {
    headers: githubHeaders({
      Accept: 'application/octet-stream',
    }),
    redirect: 'follow',
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`download GitHub asset ${asset.name} failed: ${response.status} ${text}`)
  }

  const totalBytes = Number(response.headers.get('content-length') || asset.size || 0)
  const reportProgress = createProgressTracker('download', asset.name, totalBytes)

  if (!response.body) {
    const arrayBuffer = await response.arrayBuffer()
    const buffer = Buffer.from(arrayBuffer)
    reportProgress(buffer.byteLength)
    return buffer
  }

  const reader = response.body.getReader()
  const chunks = []
  let processedBytes = 0

  while (true) {
    const { done, value } = await reader.read()
    if (done) break

    const chunk = Buffer.from(value)
    chunks.push(chunk)
    processedBytes += chunk.byteLength
    reportProgress(processedBytes)
  }

  if (
    processedBytes > 0 &&
    (!Number.isFinite(totalBytes) || totalBytes <= 0 || processedBytes !== totalBytes)
  ) {
    reportProgress(processedBytes)
  }

  return Buffer.concat(chunks)
}

async function uploadAtomGitAsset(uploadTarget, asset, buffer) {
  if (dryRun) {
    logDryRun(
      `would upload asset ${asset.name} (${buffer.byteLength} bytes) to ${uploadTarget.url}`
    )
    return
  }

  const finalUrl = uploadTarget.url
  const reportProgress = createProgressTracker('upload', asset.name, buffer.byteLength)
  const baseHeaders = {
    ...uploadTarget.headers,
    'Content-Length': String(buffer.byteLength),
  }

  if (!baseHeaders['Content-Type'] && !baseHeaders['content-type']) {
    baseHeaders['Content-Type'] = asset.content_type || 'application/octet-stream'
  }

  let processedBytes = 0
  const stream = Readable.from(
    (function* generateChunks() {
      const chunkSize = 1024 * 1024
      for (let offset = 0; offset < buffer.byteLength; offset += chunkSize) {
        const chunk = buffer.subarray(offset, Math.min(offset + chunkSize, buffer.byteLength))
        processedBytes += chunk.byteLength
        reportProgress(processedBytes)
        yield chunk
      }
    })()
  )

  const response = await fetch(finalUrl, {
    method: 'PUT',
    headers: baseHeaders,
    body: stream,
    duplex: 'half',
  })

  if (response.ok || response.status === 409) {
    if (response.status === 409) {
      console.log(`[atomgit-sync] asset exists, skipped: ${asset.name}`)
    } else {
      console.log(`[atomgit-sync] uploaded asset: ${asset.name}`)
    }
    return
  }

  const text = await response.text()
  throw new Error(`PUT upload failed for ${asset.name}: ${response.status} ${text}`)
}

function normalizeAssetSize(asset) {
  const candidates = [asset?.size, asset?.file_size, asset?.content_length, asset?.filesize]
  for (const value of candidates) {
    const numberValue = Number(value)
    if (Number.isFinite(numberValue) && numberValue >= 0) {
      return numberValue
    }
  }
  return null
}

function listExistingAssetsByName(release) {
  const assets = Array.isArray(release?.assets) ? release.assets : []
  const byName = new Map()

  for (const asset of assets) {
    if (!asset?.name) continue
    byName.set(asset.name, asset)
  }

  return byName
}

function collectAssetIdCandidates(asset) {
  const ids = [asset?.id, asset?.asset_id, asset?.file_id, asset?.attach_file_id]
  return ids
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value > 0)
}

async function deleteAtomGitAsset(asset) {
  const releaseTag = (process.env.RELEASE_TAG || '').trim()
  if (!releaseTag) {
    throw new Error(`missing RELEASE_TAG when deleting AtomGit asset ${asset?.name || 'unknown'}`)
  }

  const fileName = asset?.name || 'unknown'
  const idCandidates = collectAssetIdCandidates(asset)

  if (dryRun) {
    if (idCandidates.length > 0) {
      logDryRun(`would delete AtomGit asset before re-upload: ${fileName} (ids: ${idCandidates.join(', ')})`)
    } else {
      logDryRun(`would delete AtomGit asset before re-upload: ${fileName} (id unavailable)`)
    }
    return
  }

  if (idCandidates.length === 0) {
    throw new Error(`cannot delete existing AtomGit asset ${fileName}: missing asset id`)
  }

  const releaseId = Number(asset?.release_id)
  const deletePaths = []

  for (const id of idCandidates) {
    deletePaths.push(
      `/api/v5/repos/${encodeURIComponent(atomgitOwner)}/${encodeURIComponent(atomgitRepo)}/releases/${encodeURIComponent(releaseTag)}/assets/${id}`
    )
    if (Number.isFinite(releaseId) && releaseId > 0) {
      deletePaths.push(
        `/api/v5/repos/${encodeURIComponent(atomgitOwner)}/${encodeURIComponent(atomgitRepo)}/releases/${releaseId}/assets/${id}`
      )
      deletePaths.push(
        `/api/v5/repos/${encodeURIComponent(atomgitOwner)}/${encodeURIComponent(atomgitRepo)}/releases/${releaseId}/attach_files/${id}`
      )
    }
  }

  const uniquePaths = [...new Set(deletePaths)]
  let lastError = null

  for (const path of uniquePaths) {
    const url = buildAtomGitApiUrl(path)
    const response = await fetch(url, {
      method: 'DELETE',
      headers: atomgitHeaders(),
    })

    if (response.ok || response.status === 204) {
      console.log(`[atomgit-sync] deleted AtomGit asset: ${fileName}`)
      return
    }

    const text = await response.text()
    lastError = new Error(`DELETE ${url} failed: ${response.status} ${text}`)
  }

  throw lastError || new Error(`failed to delete AtomGit asset: ${fileName}`)
}

async function syncAssets(githubRelease, atomgitRelease, syncBudget) {
  if (!Array.isArray(githubRelease.assets) || githubRelease.assets.length === 0) {
    console.log('[atomgit-sync] no GitHub release assets, skip upload')
    return
  }

  let syncedCount = 0
  const existingAssetsByName = listExistingAssetsByName(atomgitRelease)
  console.log(`[atomgit-sync] max files per run: ${syncBudget.max}, remaining: ${syncBudget.remaining}`)

  for (const asset of githubRelease.assets) {
    if (syncBudget.remaining <= 0) {
      console.log(
        `[atomgit-sync] reached max sync limit (${syncBudget.max}), remaining assets will be handled in next run`
      )
      break
    }

    const existingAsset = existingAssetsByName.get(asset.name)

    if (existingAsset) {
      const sourceSize = normalizeAssetSize(asset)
      const targetSize = normalizeAssetSize(existingAsset)

      if (
        Number.isFinite(sourceSize) &&
        Number.isFinite(targetSize) &&
        sourceSize === targetSize
      ) {
        console.log(
          `[atomgit-sync] asset already present with same size, skipped: ${asset.name} (${formatBytes(sourceSize)})`
        )
        continue
      }

      console.log(
        `[atomgit-sync] asset size mismatch, re-upload: ${asset.name} (source=${formatBytes(sourceSize)} target=${formatBytes(targetSize)})`
      )
      await deleteAtomGitAsset(existingAsset)
      existingAssetsByName.delete(asset.name)
    }

    const uploadTarget = await getAtomGitUploadUrl(asset.name)

    if (dryRun) {
      logDryRun(
        `would sync asset ${asset.name} (${asset.size ?? 'unknown'} bytes) to ${uploadTarget.url}`
      )
      continue
    }

    console.log(`[atomgit-sync] downloading asset: ${asset.name}`)
    const buffer = await downloadGithubAsset(asset)
    await uploadAtomGitAsset(uploadTarget, asset, buffer)
    syncedCount += 1
    syncBudget.remaining -= 1
    syncBudget.synced += 1
  }

  console.log(`[atomgit-sync] synced files this run: ${syncedCount}`)
}

async function main() {
  const syncBudget = {
    max: maxSyncFiles,
    remaining: maxSyncFiles,
    synced: 0,
  }

  if (dryRun) {
    logDryRun(`starting sync preview for all releases of ${githubOwner}/${githubRepo}`)
  }

  const githubReleases = await getGithubReleasesOldestFirst()

  if (!Array.isArray(githubReleases) || githubReleases.length === 0) {
    console.log('[atomgit-sync] no GitHub releases found, nothing to sync')
    return
  }

  console.log(`[atomgit-sync] releases queued: ${githubReleases.length}`)

  let processedReleases = 0
  for (const githubRelease of githubReleases) {
    process.env.RELEASE_TAG = githubRelease.tag_name
    console.log(`[atomgit-sync] syncing release ${githubRelease.tag_name}`)

    const atomgitRelease = await createOrUpdateAtomGitRelease(githubRelease)
    await syncAssets(githubRelease, atomgitRelease, syncBudget)
    processedReleases += 1

    if (syncBudget.remaining <= 0) {
      break
    }
  }

  console.log(
    `[atomgit-sync] sync completed: releases=${processedReleases}, files=${syncBudget.synced}, remaining_quota=${syncBudget.remaining}`
  )
}

main().catch((error) => {
  console.error(`[atomgit-sync] ${error.stack || error.message}`)
  process.exit(1)
})
