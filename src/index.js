export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const respondFromR2 = async (key, cacheSeconds = 86400) => {
      const obj = await env.GTFS_R2.get(key);
      if (!obj) return new Response("Not found", { status: 404 });
      return new Response(obj.body, {
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": `public, max-age=${cacheSeconds}`,
          "access-control-allow-origin": "*",
        },
      });
    };

    if (url.pathname === "/latest.json")  return respondFromR2("latest.json", 60);
    if (url.pathname === "/status.json")  return respondFromR2("status.json", 30);
    if (url.pathname === "/windows.json") return respondFromR2("windows.json", 3600);

    const m = url.pathname.match(/^\/gtfs\/([A-Za-z0-9-]+)\/([a-z_]+\.json)$/);
    if (m) {
      const [, version, file] = m;
      const key = `gtfs/${version}/${file}`;
      return respondFromR2(key, 31536000); 
    }

    return new Response("Not Found", { status: 404 });
  }
};
