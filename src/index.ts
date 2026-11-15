import { LIVE_KEY, LIVE_STATUS_KEY, LIVE_TTL } from "./live_feed"

export { LiveFeed } from "./live_feed"

export interface Env {
  GTFS: R2Bucket
  // The live feed is optional: without these the Worker only serves the timetable.
  LIVE?: DurableObjectNamespace
  NTA_API?: string
  LIVE_ROUTE_PREFIX?: string
}

const ONE_YEAR = 31536000
const SHORT_60 = 60
const SHORT_30 = 30

const JSON_FILES = new Set([
  "stops.json",
  "routes.json",
  "trips.json",
  "stop_times.json",
  "calendar.json",
  "calendar_dates.json",
  "agencies.json"
])

const BUS_FILES = new Set(["stops.json", "routes.json", "calendar.json"])

function cors(h = new Headers()) {
  h.set("Access-Control-Allow-Origin", "*")
  h.set("Vary", "Origin")
  return h
}

function withCommon(h: Headers, contentType = "application/json; charset=utf-8") {
  h.set("Content-Type", contentType)
  return h
}

async function serveR2Object(
  request: Request,
  env: Env,
  key: string,
  ttlSeconds: number,
  immutable: boolean,
  ctx: ExecutionContext
): Promise<Response> {
  const cache = caches.default
  const cacheKey = new Request(new URL(`https://edge-cache/${key}`).toString(), { method: "GET" })
  const cached = await cache.match(cacheKey)
  if (cached && request.method !== "HEAD") {
    const inm = request.headers.get("If-None-Match")
    if (inm && cached.headers.get("ETag") && inm === cached.headers.get("ETag")) {
      return new Response(null, { status: 304, headers: cached.headers })
    }
    return cached
  }
  const obj = await env.GTFS.get(key)
  if (!obj) {
    return new Response(JSON.stringify({ error: "not found", key }), { status: 404, headers: withCommon(cors()) })
  }
  const metaType = obj.httpMetadata?.contentType || "application/json; charset=utf-8"
  const h = withCommon(cors(), metaType)
  const etag = obj.httpEtag
  if (etag) h.set("ETag", etag)
  const inm = request.headers.get("If-None-Match")
  if (etag && inm && inm === etag) {
    return new Response(null, { status: 304, headers: h })
  }
  const cc = `public, max-age=${ttlSeconds}${immutable ? ", immutable" : ""}`
  h.set("Cache-Control", cc)
  const res = request.method === "HEAD" ? new Response(null, { status: 200, headers: h }) : new Response(obj.body, { status: 200, headers: h })
  if (request.method === "GET") ctx.waitUntil(cache.put(cacheKey, res.clone()))
  return res
}

// Bus timetable files are stored gzipped and sent exactly as stored. They skip the edge
// cache above: a gzipped body read back from it was gzipped a second time when tested
// locally. Each version is immutable, so apps keep their own copy.
async function serveGzippedObject(request: Request, env: Env, key: string): Promise<Response> {
  const obj = await env.GTFS.get(key)
  if (!obj) {
    return new Response(JSON.stringify({ error: "not found", key }), { status: 404, headers: withCommon(cors()) })
  }
  const h = withCommon(cors(), obj.httpMetadata?.contentType || "application/json; charset=utf-8")
  h.set("Cache-Control", `public, max-age=${ONE_YEAR}, immutable`)
  const etag = obj.httpEtag
  if (etag) h.set("ETag", etag)
  if (etag && request.headers.get("If-None-Match") === etag) return new Response(null, { status: 304, headers: h })
  const encoding = obj.httpMetadata?.contentEncoding
  if (encoding) h.set("Content-Encoding", encoding)
  if (request.method === "HEAD") return new Response(null, { status: 200, headers: h })
  return new Response(obj.body, { status: 200, headers: h, encodeBody: encoding ? "manual" : "automatic" })
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      const url = new URL(request.url)
      if (url.pathname === "/__health") return new Response("ok", { status: 200, headers: cors() })
      if (url.pathname === "/latest.json") return serveR2Object(request, env, "latest.json", SHORT_60, false, ctx)
      if (url.pathname === "/status.json") return serveR2Object(request, env, "status.json", SHORT_30, false, ctx)
      if (url.pathname === "/windows.json") return serveR2Object(request, env, "windows.json", 3600, false, ctx)
      if (url.pathname === "/live/dublin-bus.json") return serveR2Object(request, env, LIVE_KEY, LIVE_TTL, false, ctx)
      if (url.pathname === "/live/status.json") return serveR2Object(request, env, LIVE_STATUS_KEY, LIVE_TTL, false, ctx)
      const m = url.pathname.match(/^\/gtfs\/([A-Za-z0-9-]+)\/([a-z_]+\.json)$/)
      if (m) {
        const [, ver, file] = m
        if (!JSON_FILES.has(file)) return new Response(JSON.stringify({ error: "invalid file" }), { status: 400, headers: withCommon(cors()) })
        return serveR2Object(request, env, `gtfs/${ver}/${file}`, ONE_YEAR, true, ctx)
      }
      if (url.pathname === "/bus/latest.json") return serveR2Object(request, env, "bus/latest.json", SHORT_60, false, ctx)
      if (url.pathname === "/bus/status.json") return serveR2Object(request, env, "bus/status.json", SHORT_30, false, ctx)
      const bus = url.pathname.match(/^\/bus\/([A-Za-z0-9-]+)\/(?:([a-z_]+\.json)|(shards\/\d{1,6}\.json)|(trips\/[A-Za-z0-9]{1,12}\.json))$/)
      if (bus) {
        const [, ver, file, shard, trips] = bus
        if (file && !BUS_FILES.has(file)) return new Response(JSON.stringify({ error: "invalid file" }), { status: 400, headers: withCommon(cors()) })
        return serveGzippedObject(request, env, `bus/${ver}/${file ?? shard ?? trips}`)
      }
      return new Response(JSON.stringify({ error: "not found" }), { status: 404, headers: withCommon(cors()) })
    } catch (err: any) {
      return new Response(JSON.stringify({ error: "internal", detail: String(err?.message || err) }), { status: 500, headers: withCommon(cors()) })
    }
  },
  // The live feed keeps itself running with alarms. This restarts it if that chain ever stops.
  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    if (!env.LIVE) return
    ctx.waitUntil(env.LIVE.get(env.LIVE.idFromName("dublin-bus")).fetch("https://live/ensure"))
  }
} satisfies ExportedHandler<Env>
