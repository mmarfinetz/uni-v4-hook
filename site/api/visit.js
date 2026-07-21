const crypto = require("crypto");

const MAX_BODY_BYTES = 8192;
const MAX_EVENTS = 5000;
const RATE_LIMIT_PER_MINUTE = 40;
const ALLOWED_EVENTS = new Set(["pageview", "wallet_connect", "leave"]);
const BOT_UA =
  /(bot|crawler|spider|slurp|bingpreview|facebookexternalhit|discordbot|telegrambot|whatsapp|curl|wget|python-requests|go-http-client|httpclient|headless|phantomjs|selenium|playwright|puppeteer|axios|node-fetch|undici)/i;

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
  res.status(status).send(JSON.stringify(body));
}

function empty(res, status = 204) {
  res.setHeader("Cache-Control", "no-store");
  res.status(status).end();
}

async function readJson(req) {
  if (req.body && typeof req.body === "object") return req.body;
  if (typeof req.body === "string") return JSON.parse(req.body || "{}");

  let raw = "";
  for await (const chunk of req) {
    raw += chunk;
    if (raw.length > MAX_BODY_BYTES) throw new Error("request body too large");
  }
  return raw ? JSON.parse(raw) : {};
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

async function redisPipeline(config, commands) {
  const response = await fetch(`${config.url}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(commands),
    cache: "no-store",
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok || body.error) {
    throw new Error(body.error || `redis pipeline failed: ${response.status}`);
  }
  return body;
}

function cleanText(value, max = 160) {
  return String(value || "")
    .replace(/[\u0000-\u001f\u007f]/g, "")
    .slice(0, max);
}

function cleanEvent(value) {
  const event = cleanText(value, 32).toLowerCase().replace(/[^a-z0-9_:-]/g, "");
  return ALLOWED_EVENTS.has(event) ? event : "pageview";
}

function visitorIp(req) {
  return header(req, "x-forwarded-for").split(",")[0].trim() || req.socket?.remoteAddress || "";
}

function hashValue(value) {
  if (!value) return "";
  const salt =
    process.env.VISITOR_HASH_SALT ||
    process.env.VERCEL_PROJECT_PRODUCTION_URL ||
    process.env.VERCEL_URL ||
    "local-dev";
  return crypto.createHash("sha256").update(`${salt}:${value}`).digest("hex").slice(0, 32);
}

function referrerHost(referrer) {
  try {
    return cleanText(new URL(referrer).host, 120);
  } catch {
    return "";
  }
}

function uaFamily(userAgent) {
  const ua = userAgent.toLowerCase();
  if (ua.includes("edg/")) return "edge";
  if (ua.includes("chrome/") || ua.includes("crios/")) return "chrome";
  if (ua.includes("firefox/") || ua.includes("fxios/")) return "firefox";
  if (ua.includes("safari/")) return "safari";
  return ua ? "other" : "unknown";
}

function originAllowed(req) {
  const origin = header(req, "origin");
  if (!origin) return true;
  try {
    return new URL(origin).host === header(req, "host");
  } catch {
    return false;
  }
}

function classify(req, payload) {
  const reasons = [];
  const userAgent = header(req, "user-agent");
  const language = header(req, "accept-language");

  if (!originAllowed(req)) reasons.push("cross_origin");
  if (!userAgent || BOT_UA.test(userAgent)) reasons.push("bot_user_agent");
  if (payload.webdriver === true) reasons.push("webdriver");
  if (!language) reasons.push("missing_language");
  if (!payload.clientId || !payload.tz || !payload.screen) reasons.push("missing_browser_signals");

  return reasons;
}

function normalizeRecord(req, payload) {
  const now = Date.now();
  const path = cleanText(payload.path, 240) || "/";
  const event = cleanEvent(payload.event);
  const ipHash = hashValue(visitorIp(req));
  const visitorHash = hashValue(cleanText(payload.clientId, 80));
  const ua = header(req, "user-agent");

  return {
    ts: new Date(now).toISOString(),
    event,
    path: path.startsWith("/") ? path : "/",
    referrerHost: referrerHost(payload.referrer),
    visitorHash,
    ipHash,
    uaFamily: uaFamily(ua),
    uaHash: hashValue(ua),
    language: cleanText(payload.language || header(req, "accept-language"), 48),
    tz: cleanText(payload.tz, 64),
    screen: cleanText(payload.screen, 32),
    dpr: Number.isFinite(Number(payload.dpr)) ? Math.max(0, Math.min(8, Number(payload.dpr))) : null,
    ageMs: Number.isFinite(Number(payload.ageMs)) ? Math.max(0, Math.min(86400000, Math.round(Number(payload.ageMs)))) : null,
  };
}

async function rateLimited(config, record) {
  const key = `visits:rl:${record.ipHash || record.visitorHash}:${Math.floor(Date.now() / 60000)}`;
  const count = Number(await redisCommand(config, ["INCR", key]));
  if (count === 1) await redisCommand(config, ["EXPIRE", key, 120]);
  return count > RATE_LIMIT_PER_MINUTE;
}

async function handler(req, res) {
  if (req.method !== "POST") return json(res, 405, { ok: false, error: "method_not_allowed" });

  let payload;
  try {
    payload = await readJson(req);
  } catch {
    return json(res, 400, { ok: false, error: "invalid_json" });
  }

  const config = storageConfig();
  if (!config) return json(res, 503, { ok: false, error: "visitor_storage_not_configured" });

  const record = normalizeRecord(req, payload);
  const reasons = classify(req, payload);
  const day = record.ts.slice(0, 10);

  try {
    if (await rateLimited(config, record)) {
      await redisPipeline(config, [["HINCRBY", `visits:rejected:${day}`, "rate_limited", 1]]);
      return empty(res);
    }

    if (reasons.length) {
      await redisPipeline(config, [
        ["HINCRBY", `visits:rejected:${day}`, reasons[0], 1],
        ["HINCRBY", `visits:rejected:${day}`, "total", 1],
      ]);
      return empty(res);
    }

    await redisPipeline(config, [
      ["LPUSH", "visits:events", JSON.stringify(record)],
      ["LTRIM", "visits:events", 0, MAX_EVENTS - 1],
      ["HINCRBY", `visits:daily:${day}`, "total", 1],
      ["HINCRBY", `visits:daily:${day}`, record.event, 1],
      ["HINCRBY", `visits:paths:${day}`, record.path, 1],
      ["HINCRBY", `visits:referrers:${day}`, record.referrerHost || "direct", 1],
      ["PFADD", `visits:unique:${day}`, record.visitorHash || record.ipHash],
      ["EXPIRE", `visits:events`, 60 * 60 * 24 * 90],
      ["EXPIRE", `visits:daily:${day}`, 60 * 60 * 24 * 180],
      ["EXPIRE", `visits:paths:${day}`, 60 * 60 * 24 * 180],
      ["EXPIRE", `visits:referrers:${day}`, 60 * 60 * 24 * 180],
      ["EXPIRE", `visits:unique:${day}`, 60 * 60 * 24 * 180],
      ["EXPIRE", `visits:rejected:${day}`, 60 * 60 * 24 * 180],
    ]);

    return empty(res);
  } catch (err) {
    return json(res, 500, { ok: false, error: "visitor_storage_write_failed" });
  }
}

module.exports = handler;
module.exports.config = { api: { bodyParser: { sizeLimit: "8kb" } } };
