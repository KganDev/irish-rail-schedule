import { DurableObject } from "cloudflare:workers"
import type { Env } from "./index"
import { buildLiveFile, type Feed, type VehicleFeed } from "./live"

const FEED_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates?format=json"
const VEHICLES_URL = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles?format=json"
// The NTA allows each key one call to each feed every 60 seconds.
const INTERVAL_MS = 61_000
// Dublin Bus, then Go-Ahead Ireland's city and commuter routes. These are the operators
// in the bus timetable; see GTFS_URLS_DEFAULT in scripts/bus_json_builder.py.
const DEFAULT_ROUTE_PREFIXES = ["1_", "3_", "03C_"]

export const LIVE_KEY = "live/dublin-bus.json"
export const LIVE_STATUS_KEY = "live/status.json"
// Apps read these files straight from the bucket's public address, so each is stored with
// the headers it should be sent with. The Worker sends the same time when asked instead.
export const LIVE_TTL = 20
const LIVE_HTTP = { contentType: "application/json", cacheControl: `public, max-age=${LIVE_TTL}` }

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
  vehicles?: number
  // Set when delays were published but bus positions could not be read.
  vehiclesError?: string
  outputBytes?: number
}

// Fetches the NTA feed about once a minute and publishes the Dublin city bus part of it to R2.
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
    await this.env.GTFS.put(LIVE_STATUS_KEY, JSON.stringify(status), { httpMetadata: LIVE_HTTP })
  }

  private async publish(): Promise<Partial<LiveStatus>> {
    if (!this.env.NTA_API) throw new Error("NTA_API secret is not set")

    const headers = { "x-api-key": this.env.NTA_API }
    const [response, vehicles] = await Promise.all([
      fetch(FEED_URL, { headers }),
      // Delays are still worth publishing when positions fail, so this never throws.
      this.vehicles(headers)
    ])
    if (!response.ok) throw new Error(`NTA feed returned HTTP ${response.status}`)
    const text = await response.text()

    const { file, stats } = buildLiveFile(
      JSON.parse(text) as Feed,
      this.routePrefixes(),
      vehicles.feed
    )
    const body = JSON.stringify(file)
    await this.env.GTFS.put(LIVE_KEY, body, { httpMetadata: LIVE_HTTP })

    return {
      feedTime: file.feedTime,
      feedBytes: text.length,
      feedTrips: stats.feedTrips,
      trips: stats.trips,
      updates: stats.updates,
      vehicles: stats.vehicles,
      vehiclesError: vehicles.error,
      outputBytes: body.length
    }
  }

  // LIVE_ROUTE_PREFIX can name other operators, separated by commas.
  private routePrefixes(): string[] {
    const chosen = (this.env.LIVE_ROUTE_PREFIX ?? "").split(",").map(prefix => prefix.trim())
    const prefixes = chosen.filter(prefix => prefix !== "")
    return prefixes.length ? prefixes : DEFAULT_ROUTE_PREFIXES
  }

  private async vehicles(headers: Record<string, string>): Promise<{ feed?: VehicleFeed; error?: string }> {
    try {
      const response = await fetch(VEHICLES_URL, { headers })
      if (!response.ok) return { error: `NTA positions returned HTTP ${response.status}` }
      return { feed: (await response.json()) as VehicleFeed }
    } catch (err: any) {
      return { error: String(err?.message || err) }
    }
  }
}
