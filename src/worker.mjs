export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, If-None-Match",
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    if (path === "/status")  return serveKey(env, "status.json",  { ttl: 30,  cors });
    if (path === "/latest.json") return serveKey(env, "latest.json", { ttl: 60,  cors });

    if (path.startsWith("/gtfs/")) {
      const key = path.replace(/^\//, "");
      return serveKey(env, key, { immutable: true, ttl: 31536000, cors });
    }

    return new Response("Not found", { status: 404, headers: cors });
  }
}

async function serveKey(env, key, { immutable=false, ttl=300, cors={} } = {}) {
  const obj = await env.DATA.get(key);
  if (!obj) return new Response("Not found", { status: 404, headers: cors });

  const etag = obj.httpEtag;
  const reqETag = etag && (new Headers(cors)).get("If-None-Match");
  if (reqETag && reqETag === etag) {
    return new Response(null, { status: 304, headers: { ...cors, ETag: etag } });
  }

  const headers = {
    ...cors,
    "Content-Type": key.endsWith(".json") ? "application/json; charset=utf-8" : "application/octet-stream",
    "Cache-Control": immutable ? "public, max-age=31536000, immutable" : `public, max-age=${ttl}`,
    "ETag": etag,
  };
  return new Response(obj.body, { headers });
}