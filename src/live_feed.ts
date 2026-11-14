import { DurableObject } from "cloudflare:workers"
import type { Env } from "./index"
import { buildLiveFile, type Feed } from "./live"

const FEED_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates?format=json"
// The NTA allows each key one call every 60 seconds.
const INTERVAL_MS = 61_000
const DEFAULT_ROUTE_PREFIX = "1_"

export const LIVE_KEY = "live/dublin-bus.json"
export const LIVE_STATUS_KEY = "live/status.json"

interface LiveStatus {
  since: string
  runs: number
  failures: number
  lastRunAt?: string
  lastSuccessAt?: string
  lastError?: string
  // Wall time including the download. CPU time is only shown in the Cloudflare dashboard.
  lastDurationMs?: number
  maxDurationMs?: number
  feedTime?: number
  feedBytes?: number
  feedTrips?: number
  trips?: number
  updates?: number
  outputBytes?: number
}

// Fetches the NTA feed about once a minute and publishes the Dublin Bus part of it to R2.
// One object does this for everyone, so phones never call the NTA and never see the key.
export class LiveFeed extends DurableObject<Env> {
  // Starts the alarm chain if it is not running. The cron trigger calls this as a watchdog.
  async fetch(): Promise<Response> {
    if ((await this.ctx.storage.getAlarm()) === null) await this.ctx.storage.setAlarm(Date.now())
    return new Response("ok")
  }

  async alarm(): Promise<void> {
    const startedAt = Date.now()
    // Book the next run first, so a failure below cannot break the chain.
    await this.ctx.storage.setAlarm(startedAt + INTERVAL_MS)

    const status = (await this.ctx.storage.get<LiveStatus>("status")) ?? {
      since: new Date(startedAt).toISOString(),
      runs: 0,
      failures: 0
    }
    status.runs += 1
    status.lastRunAt = new Date(startedAt).toISOString()

    try {
      const published = await this.publish()
      Object.assign(status, published)
      status.lastSuccessAt = new Date().toISOString()
      delete status.lastError
    } catch (err: any) {
      // The last good file stays in place; its feedTime tells readers how old it is.
      status.failures += 1
      status.lastError = String(err?.message || err)
    }

    status.lastDurationMs = Date.now() - startedAt
    status.maxDurationMs = Math.max(status.maxDurationMs ?? 0, status.lastDurationMs)
    await this.ctx.storage.put("status", status)
    await this.env.GTFS.put(LIVE_STATUS_KEY, JSON.stringify(status), {
      httpMetadata: { contentType: "application/json" }
    })
  }

  private async publish(): Promise<Partial<LiveStatus>> {
    if (!this.env.NTA_API) throw new Error("NTA_API secret is not set")

    const response = await fetch(FEED_URL, { headers: { "x-api-key": this.env.NTA_API } })
    if (!response.ok) throw new Error(`NTA feed returned HTTP ${response.status}`)
    const text = await response.text()

    const { file, stats } = buildLiveFile(
      JSON.parse(text) as Feed,
      this.env.LIVE_ROUTE_PREFIX || DEFAULT_ROUTE_PREFIX
    )
    const body = JSON.stringify(file)
    await this.env.GTFS.put(LIVE_KEY, body, { httpMetadata: { contentType: "application/json" } })

    return {
      feedTime: file.feedTime,
      feedBytes: text.length,
      feedTrips: stats.feedTrips,
      trips: stats.trips,
      updates: stats.updates,
      outputBytes: body.length
    }
  }
}
