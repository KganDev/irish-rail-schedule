// src/worker.mjs
export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    if (url.pathname === "/__health") {
      return new Response(JSON.stringify({ ok: true }), {
        headers: { "content-type": "application/json", ...corsHeaders() }
      });
    }

    if (url.pathname === "/latest.json" || url.pathname === "/status.json") {
      return serveObject(req, env.DATA, url.pathname.slice(1), { ttl: 60, immutable: false });
    }

    const m = url.pathname.match(/^\/gtfs\/([A-Za-z0-9-]+)\/([a-z_]+\.json)$/);
    if (m) {
      const key = `gtfs/${m[1]}/${m[2]}`;
      return serveObject(req, env.DATA, key, { ttl: 31536000, immutable: true });
    }

    return new Response("Not found", { status: 404, headers: corsHeaders() });
  }
};

function corsHeaders() {
  const h = new Headers();
  h.set("Access-Control-Allow-Origin", "*");
  h.set("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS");
  h.set("Access-Control-Allow-Headers", "If-None-Match, Content-Type");
  h.set("Access-Control-Max-Age", "86400");
  return h;
}

async function serveObject(req, bucket, key, { ttl, immutable }) {
  const obj = await bucket.get(key);
  if (!obj) return new Response("Not found", { status: 404, headers: corsHeaders() });

  const etag = obj.httpEtag || obj.etag || null;
  const reqETag = req.headers.get("If-None-Match");

  if (req.method === "HEAD") {
    return new Response(null, { status: 200, headers: headersFor(obj, { ttl, immutable, etag }) });
  }

  if (reqETag && etag && stripW(reqETag) === stripW(etag)) {
    return new Response(null, { status: 304, headers: headersFor(obj, { ttl, immutable, etag }) });
  }

  return new Response(obj.body, { headers: headersFor(obj, { ttl, immutable, etag }) });
}

function headersFor(obj, { ttl, immutable, etag }) {
  const h = corsHeaders();
  const cc = immutable ? `public, max-age=${ttl}, immutable` : `public, max-age=${ttl}`;
  h.set("Cache-Control", cc);
  h.set("Content-Type", obj.httpMetadata?.contentType || "application/json");
  if (etag) h.set("ETag", etag);
  return h;
}

function stripW(tag) {
  return tag.replace(/^W\//, "").replace(/"/g, "");
}
