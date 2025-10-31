export interface Env {
  GTFS_R2: R2Bucket;
}

function keyFromPath(pathname: string): string {
  const key = pathname.replace(/^\/+/, "");
  return key === "" ? "latest.json" : key;
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const key = keyFromPath(url.pathname);

    try {
      const obj = await env.GTFS_R2.get(key);
      if (!obj) {
        return new Response(JSON.stringify({ error: "Not Found", key }), {
          status: 404,
          headers: { "content-type": "application/json; charset=utf-8" },
        });
      }

      const etag = obj.httpEtag;
      const inm = req.headers.get("if-none-match");
      if (etag && inm && inm === etag) {
        return new Response(null, {
          status: 304,
          headers: { ETag: etag },
        });
      }

      const headers = new Headers();
      obj.writeHttpMetadata(headers); 
      if (etag) headers.set("ETag", etag);
      if (!headers.has("content-type") && key.endsWith(".json")) {
        headers.set("content-type", "application/json; charset=utf-8");
      }

      return new Response(obj.body, { status: 200, headers });
    } catch (err) {
      return new Response(
        JSON.stringify({
          error: "Internal error",
          message: (err as Error)?.message ?? String(err),
          key,
        }),
        { status: 500, headers: { "content-type": "application/json; charset=utf-8" } }
      );
    }
  },
} satisfies ExportedHandler<Env>;
