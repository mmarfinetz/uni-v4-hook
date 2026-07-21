const crypto = require("crypto");

function storageConfig() {
  const url = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
  if (!url || !token) return null;
  return { url: url.replace(/\/+$/, ""), token };
}

function header(req, name) {
  return String(req.headers[name.toLowerCase()] || "");
}

function json(res, status, body) {
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.status(status).send(JSON.stringify(body, null, 2));
}

async function redisCommand(config, command) {
  const path = command.map((part) => encodeURIComponent(String(part))).join("/");
  const response = await fetch(`${config.url}/${path}`, {
    headers: { Authorization: `Bearer ${config.token}` },
    cache: "no-store",
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok || body.error) {
    throw new Error(body.error || `redis command failed: ${response.status}`);
  }
  return body.result;
}

function submittedToken(req) {
  const auth = header(req, "authorization");
  if (auth.toLowerCase().startsWith("bearer ")) return auth.slice(7).trim();
  try {
    const url = new URL(req.url, `https://${header(req, "host") || "localhost"}`);
    return url.searchParams.get("token") || "";
  } catch {
    return "";
  }
}

function tokenMatches(expected, submitted) {
  if (!expected || !submitted) return false;
  const a = Buffer.from(expected);
  const b = Buffer.from(submitted);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function hgetallToObject(value) {
  if (!value) return {};
  if (!Array.isArray(value) && typeof value === "object") return value;
  const out = {};
  for (let i = 0; i < value.length; i += 2) out[value[i]] = Number(value[i + 1]);
  return out;
}

function topEntries(obj, limit = 25) {
  return Object.entries(obj)
    .map(([key, value]) => ({ key, value: Number(value) }))
    .sort((a, b) => b.value - a.value || a.key.localeCompare(b.key))
    .slice(0, limit);
}

function validDay(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value || "") ? value : new Date().toISOString().slice(0, 10);
}

async function handler(req, res) {
  if (req.method !== "GET") return json(res, 405, { ok: false, error: "method_not_allowed" });
  if (!tokenMatches(process.env.VISITOR_ADMIN_TOKEN, submittedToken(req))) {
    return json(res, 401, { ok: false, error: "unauthorized" });
  }

  const config = storageConfig();
  if (!config) return json(res, 503, { ok: false, error: "visitor_storage_not_configured" });

  const url = new URL(req.url, `https://${header(req, "host") || "localhost"}`);
  const day = validDay(url.searchParams.get("day"));

  try {
    const [daily, paths, referrers, rejected, uniqueVisitors, events] = await Promise.all([
      redisCommand(config, ["HGETALL", `visits:daily:${day}`]),
      redisCommand(config, ["HGETALL", `visits:paths:${day}`]),
      redisCommand(config, ["HGETALL", `visits:referrers:${day}`]),
      redisCommand(config, ["HGETALL", `visits:rejected:${day}`]),
      redisCommand(config, ["PFCOUNT", `visits:unique:${day}`]),
      redisCommand(config, ["LRANGE", "visits:events", 0, 100]),
    ]);

    return json(res, 200, {
      ok: true,
      day,
      uniqueVisitors: Number(uniqueVisitors || 0),
      totals: hgetallToObject(daily),
      rejected: hgetallToObject(rejected),
      topPaths: topEntries(hgetallToObject(paths)),
      topReferrers: topEntries(hgetallToObject(referrers)),
      recentEvents: (events || []).map((row) => {
        try {
          return JSON.parse(row);
        } catch {
          return null;
        }
      }).filter(Boolean),
    });
  } catch {
    return json(res, 500, { ok: false, error: "visitor_storage_read_failed" });
  }
}

module.exports = handler;
