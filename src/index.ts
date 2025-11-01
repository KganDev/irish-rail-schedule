export interface Env {
  GTFS: R2Bucket
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

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      const url = new URL(request.url)
      if (url.pathname === "/__health") return new Response("ok", { status: 200, headers: cors() })
      if (url.pathname === "/latest.json") return serveR2Object(request, env, "latest.json", SHORT_60, false, ctx)
      if (url.pathname === "/status.json") return serveR2Object(request, env, "status.json", SHORT_30, false, ctx)
      if (url.pathname === "/windows.json") return serveR2Object(request, env, "windows.json", 3600, false, ctx)
      const m = url.pathname.match(/^\/gtfs\/([A-Za-z0-9-]+)\/([a-z_]+\.json)$/)
      if (m) {
        const [, ver, file] = m
        if (!JSON_FILES.has(file)) return new Response(JSON.stringify({ error: "invalid file" }), { status: 400, headers: withCommon(cors()) })
        return serveR2Object(request, env, `gtfs/${ver}/${file}`, ONE_YEAR, true, ctx)
      }
      return new Response(JSON.stringify({ error: "not found" }), { status: 404, headers: withCommon(cors()) })
    } catch (err: any) {
      return new Response(JSON.stringify({ error: "internal", detail: String(err?.message || err) }), { status: 500, headers: withCommon(cors()) })
    }
  }
} satisfies ExportedHandler<Env>
