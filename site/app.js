// LVR Hook Live Instrument — reads the deployed OracleAnchoredLVRHook USDC/WETH
// pool on Base Sepolia and logs every EVM interaction (raw calldata/returndata/
// topics) to the bus inspector, so the mechanism can be verified from primary data.

import {
  createPublicClient, http, encodeFunctionData, decodeFunctionResult,
  decodeEventLog, parseAbi, keccak256, encodePacked, toFunctionSelector, parseUnits,
} from "https://esm.sh/viem@2.21.54";

// ── deployment (Base Sepolia, docs/deployment.md) ───────────────────────────
const RPC = "https://sepolia.base.org";
const CHAIN_ID = 84532;
const EXPLORER = "https://sepolia.basescan.org";
const A = {
  hook: "0x22081E668dC0f43B6166561Ac4A6Df359AA88880",
  poolManager: "0x05E73354cFDd6745C338b50BcFDfA3Aa6fA03408",
  token0: "0x036CbD53842c5426634e7929541eC2318f3dCF7e", // USDC
  token1: "0x4200000000000000000000000000000000000006", // WETH
  token0Symbol: "USDC",
  token1Symbol: "WETH",
  token0Decimals: 6,
  token1Decimals: 18,
  baseFeed: "0xd30e2101a97dcbAeBCBC04F14C3f624E67A35165", // USDC/USD
  quoteFeed: "0x4aDC67696bA383F43DD60A9e78F2C97Fbbfc7cb1", // ETH/USD
  baseFeedLabel: "USDC/USD",
  quoteFeedLabel: "ETH/USD",
  feedDecimals: 8,
  oracle: "0xA68812AA66A2417BDFAFF9a45BD9A7578C5A3202",
  swapRouter: "0x8054C37cF5C23d0186EFc0F61D7F021b5DF854e4",
  liquidityRouter: "0xB9729C4Ff9ffbe34F65a2DbBFDB412A344Cc5154",
  poolId: "0x6a269352e17a2c717d4fbc96b74f5c19a26b28688c90ee545fc97ad7fd287ff7",
};
const KEY = [A.token0, A.token1, 8388608, 60, A.hook]; // dynamic-fee flag, spacing 60

// ── ABIs ────────────────────────────────────────────────────────────────────
const hookAbi = parseAbi([
  "struct PoolKey { address currency0; address currency1; uint24 fee; int24 tickSpacing; address hooks; }",
  "function previewSwapFee(PoolKey key, bool zeroForOne) view returns (bool toxic, uint24 feeUnits, uint160 referenceSqrtPriceX96, uint160 poolSqrtPriceX96)",
  "function auctionStatus(PoolKey key) view returns (bool eligible, uint64 startTs, uint256 concessionWad, uint256 gapPremiumWad)",
  "function pokeAuction(PoolKey key) returns (bool open, uint256 concessionWad)",
  "function config(bytes32 id) view returns (address oracle, uint24 baseFee, uint24 maxFee, uint24 alphaBps, uint32 maxOracleAge, uint32 latencySecs, uint32 centerTolTicks, uint256 lvrBudgetWad, uint256 bootstrapSigma2PerSecondWad, uint24 triggerGapBps, uint256 startConcessionWad, uint256 concessionGrowthWadPerSec, uint256 maxConcessionWad)",
  "event AuctionOpened(bytes32 indexed poolId, uint64 startTs, uint256 gapPremiumWad)",
  "event AuctionClosed(bytes32 indexed poolId, uint256 gapPremiumWad)",
  "event RiskUpdated(bytes32 indexed poolId, uint256 sigma2PerSecondWad, uint256 lastOraclePriceWad, uint256 lastOracleTs)",
]);
const oracleAbi = parseAbi([
  "function latestPriceWad() view returns (uint256 priceWad, uint256 updatedAt, uint256 latestFeedTs)",
]);
const feedAbi = parseAbi([
  "function latestRoundData() view returns (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)",
]);
const pmAbi = parseAbi([
  "function extsload(bytes32 slot) view returns (bytes32)",
  "event Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)",
  "event ModifyLiquidity(bytes32 indexed id, address indexed sender, int24 tickLower, int24 tickUpper, int256 liquidityDelta, bytes32 salt)",
  "event Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1, uint24 fee, int24 tickSpacing, address hooks, uint160 sqrtPriceX96, int24 tick)",
  "event Donate(bytes32 indexed id, address indexed sender, uint256 amount0, uint256 amount1)",
]);
const erc20Abi = parseAbi([
  "function balanceOf(address who) view returns (uint256)",
  "function allowance(address owner, address spender) view returns (uint256)",
  "function approve(address spender, uint256 amount) returns (bool)",
  "function deposit() payable",
  "event Transfer(address indexed from, address indexed to, uint256 amount)",
  "event Approval(address indexed owner, address indexed spender, uint256 amount)",
]);
const routerAbi = parseAbi([
  "struct PoolKey { address currency0; address currency1; uint24 fee; int24 tickSpacing; address hooks; }",
  "struct ModifyLiquidityParams { int24 tickLower; int24 tickUpper; int256 liquidityDelta; bytes32 salt; }",
  "struct SwapParams { bool zeroForOne; int256 amountSpecified; uint160 sqrtPriceLimitX96; }",
  "struct TestSettings { bool takeClaims; bool settleUsingBurn; }",
  "function modifyLiquidity(PoolKey key, ModifyLiquidityParams params, bytes hookData) payable returns (int256 delta)",
  "function swap(PoolKey key, SwapParams params, TestSettings testSettings, bytes hookData) payable returns (int256 delta)",
]);
const allEventAbis = [...hookAbi, ...pmAbi, ...erc20Abi].filter((f) => f.type === "event");
const EVENT_TOPICS = {
  swap: "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
  initialize: "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
  auctionOpened: "0xaa9d399007abfa8fb3c641e33ef4a6db65e5a7b06b4dbe2f43ef2189f14df8c1",
  riskUpdated: "0xb85847ced2828900ff254c1567f7d59c4dcc98a2becf910e2a4bc76d237c1cb2",
};

const ERROR_NAMES = {};
for (const sig of ["OracleStale()", "InvalidOraclePrice()", "InvalidConfig()", "InvalidPool()",
  "NotOwner()", "DeviationTooLarge(uint24,uint24)", "SequencerDown()",
  "SequencerGracePeriodNotOver(uint256)", "WidthTooNarrow(uint256,uint256)"]) {
  ERROR_NAMES[toFunctionSelector(sig)] = sig;
}

const client = createPublicClient({ transport: http(RPC) });
const Q96 = 2n ** 96n;
const PRICE_DECIMAL_FACTOR = 10 ** (A.token0Decimals - A.token1Decimals);
const ZERO_BYTES = "0x";
const ZERO_SALT = "0x" + "0".repeat(64);
const MAX_UINT = (1n << 256n) - 1n;
const TICK_SPACING = 60;
const LIQUIDITY_HALF_WIDTH_TICKS = 12000;
const MIN_USABLE_TICK = -887220;
const MAX_USABLE_TICK = 887220;
const WAD = 10n ** 18n;
const HALF_BPS_WAD = 50_000_000_000_000n;
const FEE_SCALE = 1_000_000_000_000n;
const FEE_DENOMINATOR = 1_000_000n;
const BPS_DENOMINATOR = 10_000n;
const STATIC_BASELINE_FEE = 3000;

// ── visitor telemetry ──────────────────────────────────────────────────────
const VISITOR_ID_KEY = "lvrhook.visitor";

function visitorId() {
  try {
    let id = localStorage.getItem(VISITOR_ID_KEY);
    if (!id) {
      id = globalThis.crypto?.randomUUID?.() || `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
      localStorage.setItem(VISITOR_ID_KEY, id);
    }
    return id;
  } catch {
    return "";
  }
}

function visitorScreen() {
  try {
    return `${screen.width}x${screen.height}@${screen.colorDepth || 0}`;
  } catch {
    return "";
  }
}

function visitorTimezone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "";
  } catch {
    return "";
  }
}

function trackVisit(event, extra = {}) {
  if (navigator.doNotTrack === "1" || window.doNotTrack === "1") return;
  const payload = {
    event,
    path: `${location.pathname}${location.search}${location.hash}`,
    referrer: document.referrer,
    clientId: visitorId(),
    language: navigator.language || "",
    tz: visitorTimezone(),
    screen: visitorScreen(),
    dpr: window.devicePixelRatio || 1,
    webdriver: navigator.webdriver === true,
    ...extra,
  };
  const body = JSON.stringify(payload);

  try {
    if (navigator.sendBeacon) {
      const blob = new Blob([body], { type: "application/json" });
      if (navigator.sendBeacon("/api/visit", blob)) return;
    }
  } catch {}

  try {
    fetch("/api/visit", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
      keepalive: true,
    }).catch(() => {});
  } catch {}
}

// ── bus inspector ───────────────────────────────────────────────────────────
const busEl = document.getElementById("buslog");
let paused = false, quietReads = false;
document.getElementById("pausebus").addEventListener("change", (e) => (paused = e.target.checked));
document.getElementById("quietreads").addEventListener("change", (e) => (quietReads = e.target.checked));
document.getElementById("clearbus").addEventListener("click", () => (busEl.innerHTML = ""));

function fmtHex(h, limit = 4096) {
  if (!h) return "∅ (empty)";
  const s = h.length > limit ? h.slice(0, limit) + `… (+${(h.length - limit) / 2} bytes)` : h;
  if (s.length > 10) return `<span class="sel">${s.slice(0, 10)}</span>${s.slice(10)}`;
  return s;
}
function busLog({ kind, title, meta = "", raw = {}, decoded = "", quiet = false }) {
  if (paused || (quiet && quietReads)) return;
  const ent = document.createElement("div");
  ent.className = "bent";
  const ts = new Date().toISOString().slice(11, 23);
  let detail = "";
  if (raw.to) detail += `<div class="lbl">to</div><div class="hex"><a href="${EXPLORER}/address/${raw.to}" target="_blank">${raw.to}</a></div>`;
  if (raw.data) detail += `<div class="lbl">calldata</div><div class="hex">${fmtHex(raw.data)}</div>`;
  if (raw.result !== undefined) detail += `<div class="lbl">returndata</div><div class="hex">${fmtHex(raw.result)}</div>`;
  if (raw.topics) detail += `<div class="lbl">topics</div><div class="hex">${raw.topics.map((t) => fmtHex(t)).join("<br>")}</div><div class="lbl">data</div><div class="hex">${fmtHex(raw.logdata)}</div>`;
  if (raw.tx) detail += `<div class="lbl">tx</div><div class="hex"><a href="${EXPLORER}/tx/${raw.tx}" target="_blank">${raw.tx}</a></div>`;
  if (decoded) detail += `<div class="lbl">decoded</div><div class="dec">${decoded}</div>`;
  ent.innerHTML = `<div class="row"><span class="ts">${ts}</span><span class="tag ${kind}">${kind}</span><span class="title">${title}</span><span class="meta">${meta}</span></div><div class="detail">${detail}</div>`;
  ent.querySelector(".row").addEventListener("click", () => ent.classList.toggle("open"));
  busEl.prepend(ent);
  while (busEl.children.length > 400) busEl.lastChild.remove();
}

function fmtArgs(args) {
  return Object.entries(args ?? {})
    .map(([k, v]) => `<span class="fld">${k}=</span>${typeof v === "bigint" ? v.toString() : v}`)
    .join("  ");
}

// logged eth_call: raw JSON-RPC so the wire bytes (incl. revert selectors)
// land in the inspector exactly as the node returned them
let rpcId = 1;
async function rawEthCall(to, data, blockTag = "latest") {
  const res = await fetch(RPC, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: rpcId++, method: "eth_call", params: [{ to, data }, blockTag] }),
  });
  return res.json();
}
async function loggedCall(abi, to, functionName, args = [], { quiet = true } = {}) {
  const data = encodeFunctionData({ abi, functionName, args });
  const resp = await rawEthCall(to, data);
  if (resp.error) {
    const rev = typeof resp.error.data === "string" ? resp.error.data : null;
    const sel = rev ? rev.slice(0, 10) : null;
    const name = sel && ERROR_NAMES[sel] ? ERROR_NAMES[sel] : sel ?? `${resp.error.message} (no data)`;
    busLog({ kind: "ERR", title: `${functionName}() reverted`, meta: name,
      raw: { to, data, result: rev ?? undefined },
      decoded: `revert <span class="fld">${name}</span>`, quiet: false });
    throw Object.assign(new Error(`${functionName} reverted: ${name}`), { revertName: name });
  }
  const result = resp.result;
  const decoded = decodeFunctionResult({ abi, functionName, data: result ?? "0x" });
  const fn = abi.find((f) => f.name === functionName && f.type === "function");
  const names = fn.outputs.map((o, i) => o.name || `out${i}`);
  const vals = fn.outputs.length === 1 ? [decoded] : decoded;
  const decStr = names.map((n, i) => `<span class="fld">${n}=</span>${vals[i]}`).join("  ");
  busLog({ kind: "CALL", title: `${functionName}()`, meta: to.slice(0, 10) + "…", raw: { to, data, result }, decoded: decStr, quiet });
  return decoded;
}

// ── formatting helpers ──────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const STATE_CLASSES = new Set(["pulse", "amber", "green", "red", "dim"]);
function baseClass(el) {
  if (!el.dataset.baseClass) {
    el.dataset.baseClass = [...el.classList].filter((c) => !STATE_CLASSES.has(c)).join(" ") || "v";
  }
  return el.dataset.baseClass;
}
function set(id, text, cls) {
  const el = $(id);
  if (!el) return;
  const changed = el.textContent !== text;
  if (changed) el.textContent = text;
  el.className = [baseClass(el), cls].filter(Boolean).join(" ");
  if (changed) { void el.offsetWidth; el.classList.add("pulse"); }
}
const fmtAge = (s) => (s < 0 ? "0s" : s < 90 ? `${s}s` : s < 5400 ? `${(s / 60).toFixed(1)}m` : `${(s / 3600).toFixed(1)}h`);
const sqrtToPrice = (sq) => { const f = Number(sq) / Number(Q96); return f * f; };
const rawToWholePrice = (raw) => raw * PRICE_DECIMAL_FACTOR;
function fmtPrice(v) {
  if (!Number.isFinite(v)) return "—";
  if (Math.abs(v) >= 1000) return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (Math.abs(v) >= 1) return v.toFixed(6);
  if (Math.abs(v) >= 0.000001) return v.toFixed(8);
  return v.toExponential(4);
}
function fmtUsd(v, places = 4) {
  if (!Number.isFinite(v)) return "—";
  const sign = v < 0 ? "-" : "";
  const x = Math.abs(v);
  if (x >= 1000) return `${sign}$${x.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  if (x >= 1) return `${sign}$${x.toFixed(places)}`;
  if (x >= 0.0001) return `${sign}$${x.toFixed(6)}`;
  return `${sign}$${x.toExponential(2)}`;
}
function fmtFeedAnswer(answer) {
  const n = Number(answer) / 10 ** A.feedDecimals;
  if (!Number.isFinite(n)) return "—";
  return `$${n >= 100 ? n.toFixed(2) : n.toFixed(6)}`;
}
function formatUnits(value, decimals, places = 4) {
  const x = BigInt(value);
  const base = 10n ** BigInt(decimals);
  const whole = x / base;
  const frac = x % base;
  const scaled = (frac * 10n ** BigInt(places)) / base;
  const suffix = scaled.toString().padStart(places, "0").replace(/0+$/, "");
  return suffix ? `${whole}.${suffix}` : whole.toString();
}
function fmtInt(value) {
  return BigInt(value).toLocaleString();
}
function fmtPct(value, places = 2) {
  if (!Number.isFinite(value)) return "—";
  return `${value.toFixed(places)}%`;
}
function fmtSignedUsd(v, places = 4) {
  if (!Number.isFinite(v)) return "—";
  return `${v >= 0 ? "+" : ""}${fmtUsd(v, places)}`;
}
function toTxHex(value) {
  const v = BigInt(value);
  return "0x" + v.toString(16);
}
function parseAmountInput(id, decimals, fallback) {
  const raw = ($(id)?.value || fallback).trim();
  if (!raw || Number(raw) <= 0) throw new Error(`${id} must be greater than zero`);
  return parseUnits(raw, decimals);
}
function slotOffset(slot, offset) {
  return "0x" + (BigInt(slot) + BigInt(offset)).toString(16).padStart(64, "0");
}
function absBigInt(value) {
  return value < 0n ? -value : value;
}
function gapPremiumWad(refSqrt, poolSqrt) {
  const ref = BigInt(refSqrt), pool = BigInt(poolSqrt);
  if (ref > pool) return { premiumWad: (ref * WAD) / pool - WAD, oracleAbove: true };
  if (pool > ref) return { premiumWad: (pool * WAD) / ref - WAD, oracleAbove: false };
  return { premiumWad: 0n, oracleAbove: false };
}
function wholeAmount(raw, decimals) {
  return Number(raw) / 10 ** decimals;
}
function rawPriceFromSqrt(sqrt) {
  return sqrtToPrice(sqrt);
}
function sqrtFromRawPrice(price) {
  if (!Number.isFinite(price) || price <= 0) return 0n;
  return BigInt(Math.floor(Math.sqrt(price) * Number(Q96)));
}
function tokenValueUsdc(raw, tokenIndex, refSqrt) {
  const amount = wholeAmount(raw, tokenIndex === 0 ? A.token0Decimals : A.token1Decimals);
  if (tokenIndex === 0) return amount;
  const wethPerUsdc = rawToWholePrice(rawPriceFromSqrt(refSqrt));
  return wethPerUsdc > 0 ? amount / wethPerUsdc : 0;
}
function feeValueUsdc(inputRaw, inputTokenIndex, feeUnits, refSqrt) {
  const feeRaw = (inputRaw * BigInt(Math.max(0, feeUnits))) / FEE_DENOMINATOR;
  return tokenValueUsdc(feeRaw, inputTokenIndex, refSqrt);
}
function unconcededFeeUnits(premiumWad) {
  const alpha = BigInt(S.cfg?.alphaBps ?? 10000);
  const surchargeWad = (BigInt(premiumWad) * alpha) / BPS_DENOMINATOR;
  return (S.cfg?.baseFee ?? 500) + Number(surchargeWad / FEE_SCALE);
}
function impliedConcessionPct(actualFeeUnits, noConcessionFeeUnits) {
  const base = S.cfg?.baseFee ?? 500;
  const denom = noConcessionFeeUnits - base;
  if (denom <= 0) return 0;
  return Math.max(0, Math.min(100, ((noConcessionFeeUnits - actualFeeUnits) / denom) * 100));
}
function cfgConcessionPctAt(startTs, blockTs) {
  if (!S.cfg || !startTs || !blockTs) return null;
  const elapsed = Math.max(0, Number(blockTs) - Number(startTs));
  let wad = S.cfg.startConcessionWad + S.cfg.concessionGrowthWadPerSec * BigInt(elapsed);
  if (wad > S.cfg.maxConcessionWad) wad = S.cfg.maxConcessionWad;
  return Number(wad) / 1e16;
}
function eventSort(a, b) {
  const ab = typeof a.blockNumber === "bigint" ? a.blockNumber : BigInt(a.blockNumber);
  const bb = typeof b.blockNumber === "bigint" ? b.blockNumber : BigInt(b.blockNumber);
  if (ab !== bb) return ab < bb ? -1 : 1;
  const ai = Number(a.logIndex ?? 0), bi = Number(b.logIndex ?? 0);
  return ai - bi;
}
function displayAmount(raw, tokenIndex, places = 6) {
  return `${formatUnits(absBigInt(raw), tokenIndex === 0 ? A.token0Decimals : A.token1Decimals, places)} ${tokenIndex === 0 ? A.token0Symbol : A.token1Symbol}`;
}
function shortHash(hash) {
  return hash ? `${hash.slice(0, 8)}…${hash.slice(-4)}` : "tx";
}
function setImpact(id, text, cls) {
  set(id, text, cls);
}

// ── state + refresh ─────────────────────────────────────────────────────────
const S = { cfg: null, status: null, preview01: null, preview10: null, oracle: null,
  baseRound: null, quoteRound: null, poolSqrt: null, chainTs: 0, chainTsAt: 0,
  stale: false, block: 0n, liquidity: 0n, balances: null, allowances: null };

const POOLS_SLOT = "0x" + (6n).toString(16).padStart(64, "0");
const POOL_STATE_SLOT = keccak256(encodePacked(["bytes32", "bytes32"], [A.poolId, POOLS_SLOT]));
const SLOT0 = POOL_STATE_SLOT;
const LIQUIDITY_SLOT = slotOffset(POOL_STATE_SLOT, 3n);

async function refresh() {
  try {
    const block = await client.getBlock();
    S.block = block.number; S.chainTs = Number(block.timestamp); S.chainTsAt = Date.now();
    $("netchip").textContent = `base sepolia · block ${S.block}`;
    $("netchip").classList.remove("err");

    if (!S.cfg) {
      const c = await loggedCall(hookAbi, A.hook, "config", [A.poolId], { quiet: false });
      S.cfg = { oracle: c[0], baseFee: Number(c[1]), maxFee: Number(c[2]), alphaBps: Number(c[3]),
        maxOracleAge: Number(c[4]), triggerGapBps: Number(c[9]), startConcessionWad: c[10],
        concessionGrowthWadPerSec: c[11], maxConcessionWad: c[12] };
      set("cfg-trigger", `${S.cfg.triggerGapBps} bps`);
      set("cfg-base", `${S.cfg.baseFee} ppm (${S.cfg.baseFee / 100} bps)`);
      set("cfg-max", `${S.cfg.maxFee} ppm`);
      set("cfg-growth", `${Number(S.cfg.concessionGrowthWadPerSec) / 1e14} bps/s of surcharge`);
      set("cfg-maxage", fmtAge(S.cfg.maxOracleAge));
    }

    const slot0 = (await loggedCall(pmAbi, A.poolManager, "extsload", [SLOT0]));
    S.poolSqrt = BigInt(slot0) & ((1n << 160n) - 1n);
    const liquidityWord = await loggedCall(pmAbi, A.poolManager, "extsload", [LIQUIDITY_SLOT]);
    S.liquidity = BigInt(liquidityWord) & ((1n << 128n) - 1n);

    try { S.baseRound = await loggedCall(feedAbi, A.baseFeed, "latestRoundData"); }
    catch { S.baseRound = null; }
    try { S.quoteRound = await loggedCall(feedAbi, A.quoteFeed, "latestRoundData"); }
    catch { S.quoteRound = null; }

    try {
      S.oracle = await loggedCall(oracleAbi, S.cfg.oracle || A.oracle, "latestPriceWad");
      S.stale = S.chainTs > Number(S.oracle[1]) + S.cfg.maxOracleAge;
    } catch { S.stale = true; }

    try {
      S.preview01 = await loggedCall(hookAbi, A.hook, "previewSwapFee", [KEY, true]);
      S.preview10 = await loggedCall(hookAbi, A.hook, "previewSwapFee", [KEY, false]);
      S.status = await loggedCall(hookAbi, A.hook, "auctionStatus", [KEY]);
      S.stale = false;
    } catch (err) {
      S.preview01 = S.preview10 = S.status = null;
      S.stale = true;
    }
    render();
  } catch (err) {
    $("netchip").textContent = "rpc unreachable";
    $("netchip").classList.add("err");
  }
}

function currentConcessionWad() {
  if (!S.status || !S.cfg || !S.status[0]) return 0n;
  const startTs = Number(S.status[1]);
  if (startTs === 0) return S.cfg.startConcessionWad;
  const now = S.chainTs + Math.floor((Date.now() - S.chainTsAt) / 1000);
  const c = S.cfg.startConcessionWad + S.cfg.concessionGrowthWadPerSec * BigInt(Math.max(0, now - startTs));
  return c > S.cfg.maxConcessionWad ? S.cfg.maxConcessionWad : c;
}

function render() {
  $("failclosed").classList.toggle("show", S.stale);
  renderFeedRows();
  renderOperatorPlan();
  set("pool-liq", fmtInt(S.liquidity), S.liquidity > 0n ? "green" : "red");
  if (S.stale) {
    set("gapbig", "—"); $("gapunits").textContent = "";
    ["fee-toxic", "fee-benign", "auc-elig", "auc-clock", "auc-conc", "auc-fee"].forEach((id) => set(id, "—", "dim"));
    set("pool-price", S.poolSqrt ? fmtPrice(rawToWholePrice(sqrtToPrice(S.poolSqrt))) : "—", "dim");
    if (S.oracle) {
      set("ora-price", fmtPrice(rawToWholePrice(Number(S.oracle[0]) / 1e18)));
      set("ora-age", fmtAge(S.chainTs - Number(S.oracle[1])), "red");
    } else set("ora-age", "stale / unreadable", "red");
    drawGauge(null);
    return;
  }
  const [, , refSqrt, poolSqrt] = S.preview01;
  const refP = sqrtToPrice(refSqrt), poolP = sqrtToPrice(poolSqrt);
  const premium = Number(S.status[3]) / 1e18;
  const gapBps = premium / 0.00005; // premium = e^{|z|/2}-1 ≈ half the gap; HALF_BPS_WAD
  const oracleAbove = refSqrt > poolSqrt;

  set("gapbig", gapBps.toFixed(2));
  $("gapunits").textContent = "bps stale gap";
  $("dirn").innerHTML = premium < 1e-12
    ? `pool sits exactly on the oracle price — <span class="ben">both directions benign</span>, base fee applies`
    : `oracle ${oracleAbove ? "above" : "below"} pool → <span class="tox">toxic: ${oracleAbove ? "buy token0 (1→0)" : "sell token0 (0→1)"}</span> pays the surcharge · <span class="ben">opposite direction pays base fee</span>`;

  set("ora-price", fmtPrice(rawToWholePrice(refP)));
  set("pool-price", fmtPrice(rawToWholePrice(poolP)));
  const oracleAge = S.chainTs - Number(S.oracle[1]);
  set("ora-age", fmtAge(oracleAge), oracleAge > S.cfg.maxOracleAge ? "red" : oracleAge > 3600 ? "amber" : "green");

  const toxicPrev = S.preview01[0] ? S.preview01 : S.preview10;
  const benignPrev = S.preview01[0] ? S.preview10 : S.preview01;
  set("fee-toxic", premium < 1e-12 ? `${S.cfg.baseFee} ppm` : `${Number(toxicPrev[1])} ppm`, premium < 1e-12 ? "green" : "red");
  set("fee-benign", `${Number(benignPrev[1])} ppm`, "green");

  const eligible = S.status[0];
  const startTs = Number(S.status[1]);
  set("auc-elig", eligible ? "ELIGIBLE" : "closed", eligible ? "amber" : "dim");
  set("auc-clock", startTs === 0 ? (eligible ? "not started — poke it" : "—") : `t+${fmtAge(S.chainTs - startTs)}`, startTs ? "amber" : "dim");
  drawGauge({ refP, poolP, gapBps, oracleAbove });
  tickConcession();
}

function tickFromSqrtPrice(sqrtX96) {
  const ratio = Number(sqrtX96) / Number(Q96);
  const price = ratio * ratio;
  if (!Number.isFinite(price) || price <= 0) return 0;
  return Math.floor(Math.log(price) / Math.log(1.0001));
}

function plannedRange() {
  const sqrt = S.preview01?.[2] || S.poolSqrt || Q96;
  const mid = Math.round(tickFromSqrtPrice(sqrt) / TICK_SPACING) * TICK_SPACING;
  const tickLower = Math.max(MIN_USABLE_TICK, mid - LIQUIDITY_HALF_WIDTH_TICKS);
  const tickUpper = Math.min(MAX_USABLE_TICK, mid + LIQUIDITY_HALF_WIDTH_TICKS);
  return { tickLower, tickUpper };
}

function toxicPlan() {
  if (!S.preview01 || !S.preview10) return null;
  if (!S.preview01[0] && !S.preview10[0]) return null;
  const zeroForOne = Boolean(S.preview01[0]);
  const preview = zeroForOne ? S.preview01 : S.preview10;
  return {
    zeroForOne,
    preview,
    inputSymbol: zeroForOne ? A.token0Symbol : A.token1Symbol,
    outputSymbol: zeroForOne ? A.token1Symbol : A.token0Symbol,
    inputDecimals: zeroForOne ? A.token0Decimals : A.token1Decimals,
  };
}

function renderOperatorPlan() {
  const range = plannedRange();
  set("liq-range", `${range.tickLower} / ${range.tickUpper}`, "dim");
  const plan = toxicPlan();
  const fillInput = $("fillAmount");
  if (plan) {
    set("fill-dir", `${plan.inputSymbol} -> ${plan.outputSymbol}`, S.status?.[1] ? "green" : "amber");
    set("fill-token", plan.inputSymbol, "dim");
    if (fillInput && fillInput.dataset.dirty !== "1") fillInput.value = plan.zeroForOne ? "10" : "0.005";
  } else {
    set("fill-dir", "no toxic direction", "dim");
    set("fill-token", "input", "dim");
  }
  if (S.status) {
    const eligible = S.status[0];
    const startTs = Number(S.status[1]);
    set("step-auction", eligible ? (startTs ? "clock running" : "ready to poke") : "below trigger", eligible ? "amber" : "dim");
  }
}

function renderFeedRows() {
  if (S.baseRound) {
    set("feed-base", `${A.baseFeedLabel} ${fmtFeedAnswer(S.baseRound[1])}`);
    set("feed-base-age", fmtAge(S.chainTs - Number(S.baseRound[3])), "dim");
  } else {
    set("feed-base", `${A.baseFeedLabel} unreadable`, "red");
    set("feed-base-age", "—", "red");
  }
  if (S.quoteRound) {
    set("feed-quote", `${A.quoteFeedLabel} ${fmtFeedAnswer(S.quoteRound[1])}`);
    set("feed-quote-age", fmtAge(S.chainTs - Number(S.quoteRound[3])), "dim");
  } else {
    set("feed-quote", `${A.quoteFeedLabel} unreadable`, "red");
    set("feed-quote-age", "—", "red");
  }
}

function tickConcession() {
  if (!S.status || S.stale) return;
  const c = currentConcessionWad();
  const pct = Number(c) / 1e16;
  set("auc-conc", S.status[0] ? `${pct.toFixed(4)} %` : "—", S.status[0] ? "green" : "dim");
  if (S.status[0] && S.cfg && S.preview01) {
    const toxicPrev = S.preview01[0] ? S.preview01 : S.preview10;
    const surcharge = Math.max(0, Number(toxicPrev[1]) - S.cfg.baseFee);
    const scheduled = S.cfg.baseFee + Math.round((surcharge * (1e18 - Number(currentConcessionWad()))) / (1e18 - Number(S.status[2] ?? 0n)));
    set("auc-fee", `${Number(toxicPrev[1])} ppm now`, "amber");
  }
}
setInterval(tickConcession, 250);

// ── gauge (SVG) ─────────────────────────────────────────────────────────────
function drawGauge(d) {
  const svg = $("gauge");
  const W = svg.clientWidth || 900, H = 130, cx = W / 2, axisY = 78;
  let inner = "";
  const line = (x1, y1, x2, y2, stroke, w = 1, dash = "") =>
    `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${stroke}" stroke-width="${w}" ${dash ? `stroke-dasharray="${dash}"` : ""}/>`;
  const txt = (x, y, t, fill, size = 10, anchor = "middle") =>
    `<text x="${x}" y="${y}" fill="${fill}" font-size="${size}" text-anchor="${anchor}" font-family="IBM Plex Mono,monospace">${t}</text>`;

  // axis in bps-of-gap space, pool price fixed at center; ±60bps window
  const span = 60, scale = (W - 80) / (2 * span);
  const X = (bps) => cx + bps * scale;
  inner += line(40, axisY, W - 40, axisY, "#2c2517", 1);
  for (let b = -span; b <= span; b += 10) {
    inner += line(X(b), axisY - (b % 30 === 0 ? 8 : 4), X(b), axisY + (b % 30 === 0 ? 8 : 4), "#4a3c22", 1);
    if (b % 30 === 0) inner += txt(X(b), axisY + 24, `${b > 0 ? "+" : ""}${b}`, "#625843");
  }
  // trigger band
  const trig = S.cfg ? S.cfg.triggerGapBps : 10;
  inner += line(X(trig), axisY - 26, X(trig), axisY + 12, "#a97b3f", 1, "3 3");
  inner += line(X(-trig), axisY - 26, X(-trig), axisY + 12, "#a97b3f", 1, "3 3");
  inner += txt(X(trig), axisY - 32, `trigger +${trig}`, "#a97b3f", 9);
  inner += txt(X(-trig), axisY - 32, `−${trig}`, "#a97b3f", 9);
  // pool marker (fixed center)
  inner += line(cx, axisY - 22, cx, axisY + 22, "#e8ddc8", 2);
  inner += txt(cx, axisY + 40, "pool price", "#9c8f74", 10);

  if (d) {
    const g = Math.max(-span, Math.min(span, d.oracleAbove ? d.gapBps : -d.gapBps));
    const xo = X(g);
    // shaded gap = the stale-loss region the auction reprices
    const x0 = Math.min(cx, xo), x1 = Math.max(cx, xo);
    if (Math.abs(g) > 0.05) {
      const col = Math.abs(d.gapBps) >= trig ? "rgba(255,180,84,0.18)" : "rgba(232,221,200,0.08)";
      inner += `<rect x="${x0}" y="${axisY - 18}" width="${Math.max(1, x1 - x0)}" height="36" fill="${col}"/>`;
    }
    // oracle marker
    inner += line(xo, axisY - 26, xo, axisY + 26, "#ffb454", 2.5);
    inner += `<circle cx="${xo}" cy="${axisY - 30}" r="3.5" fill="#ffb454"><animate attributeName="opacity" values="1;0.3;1" dur="1.6s" repeatCount="indefinite"/></circle>`;
    inner += txt(xo, axisY - 40, "oracle", "#ffb454", 10);
  } else {
    inner += txt(cx, 30, "oracle unreadable — pool fails closed", "#ff6b5e", 11);
  }
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.innerHTML = inner;
}

// ── historical event tail ───────────────────────────────────────────────────
async function rawGetLogs(filter) {
  const res = await fetch(RPC, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: rpcId++, method: "eth_getLogs", params: [filter] }),
  });
  const j = await res.json();
  if (j.error) throw new Error(j.error.message);
  return j.result;
}
const hexBlock = (n) => "0x" + n.toString(16);

function normalizeLog(lg) {
  return {
    ...lg,
    blockNumber: typeof lg.blockNumber === "bigint" ? lg.blockNumber : BigInt(lg.blockNumber),
    logIndex: typeof lg.logIndex === "number" ? lg.logIndex : Number(lg.logIndex ?? 0),
    topics: lg.topics,
    data: lg.data,
  };
}

async function fetchEventTail() {
  // The load-balanced public RPC caps eth_getLogs ranges inconsistently (some
  // backends allow 10k blocks, some 2k). Walk backwards with an adaptive chunk:
  // halve on range errors instead of giving up.
  let chunk = 1800n;
  const MAX_REQUESTS = 120;
  let latest;
  try { latest = await client.getBlockNumber(); } catch { return { collected: [], scanned: 0n, requests: 0 }; }
  let to = latest, collected = [], requests = 0, scanned = 0n, foundAtScan = -1n;
  while (requests < MAX_REQUESTS && to > 0n) {
    const from = to > chunk ? to - chunk : 0n;
    try {
      const [hookLogs, swapLogs] = await Promise.all([
        rawGetLogs({ address: A.hook, fromBlock: hexBlock(from), toBlock: hexBlock(to) }),
        rawGetLogs({ address: A.poolManager, fromBlock: hexBlock(from), toBlock: hexBlock(to), topics: [null, A.poolId] }),
      ]);
      requests += 2;
      collected.push(...hookLogs, ...swapLogs);
      scanned += to - from + 1n;
      if (collected.length && foundAtScan < 0n) foundAtScan = scanned;
      to = from - 1n;
      const topics = collected.map((l) => l.topics?.[0]);
      const swapCount = topics.filter((t) => t === EVENT_TOPICS.swap).length;
      const hasPriceAnchor =
        topics.includes(EVENT_TOPICS.initialize) ||
        topics.includes(EVENT_TOPICS.auctionOpened) ||
        swapCount >= 3;
      const hasSwapAndRisk = swapCount > 0 && topics.includes(EVENT_TOPICS.riskUpdated);
      // Keep one chunk of context beyond the first hit. If the log tail lacks
      // a prior swap/init price, the impact analyzer falls back to a historical
      // slot0 read at the block immediately before the swap.
      if (foundAtScan >= 0n && (hasSwapAndRisk || hasPriceAnchor) && scanned > foundAtScan + chunk) break;
      if (collected.length > 220) break;
    } catch (e) {
      requests += 2;
      const msg = String(e?.message ?? e);
      if (/block range|too many|limit|exceed/i.test(msg) && chunk > 200n) {
        chunk /= 2n; // backend with a tighter cap — shrink and retry same window
        continue;
      }
      busLog({ kind: "ERR", title: `eth_getLogs chunk failed`, meta: msg.slice(0, 70) });
      break;
    }
  }
  collected = collected.map(normalizeLog).sort(eventSort);
  return { collected, scanned, requests };
}

function decodeEvent(lg) {
  try {
    return decodeEventLog({ abi: allEventAbis, data: lg.data, topics: lg.topics });
  } catch {
    return null;
  }
}

const blockTsCache = new Map();
async function blockTimestamp(blockNumber) {
  const key = blockNumber.toString();
  if (!blockTsCache.has(key)) {
    const b = await client.getBlock({ blockNumber });
    blockTsCache.set(key, Number(b.timestamp));
  }
  return blockTsCache.get(key);
}

const historicalSqrtCache = new Map();
async function historicalPoolSqrt(blockNumber) {
  const key = blockNumber.toString();
  if (historicalSqrtCache.has(key)) return historicalSqrtCache.get(key);
  const data = encodeFunctionData({ abi: pmAbi, functionName: "extsload", args: [SLOT0] });
  try {
    const resp = await rawEthCall(A.poolManager, data, hexBlock(blockNumber));
    if (resp.error || !resp.result) throw new Error(resp.error?.message || "missing result");
    const word = decodeFunctionResult({ abi: pmAbi, functionName: "extsload", data: resp.result });
    const sqrt = BigInt(word) & ((1n << 160n) - 1n);
    historicalSqrtCache.set(key, sqrt);
    busLog({ kind: "CALL", title: "extsload() historical slot0", meta: `block ${blockNumber}`,
      raw: { to: A.poolManager, data, result: resp.result },
      decoded: `<span class="fld">sqrtPriceX96=</span>${sqrt}`, quiet: true });
    return sqrt;
  } catch {
    historicalSqrtCache.set(key, null);
    return null;
  }
}

function referenceSqrtFromRiskPrice(priceWad) {
  return sqrtFromRawPrice(Number(priceWad) / 1e18);
}

function gapBpsFromPremium(premiumWad) {
  return Number(premiumWad) / Number(HALF_BPS_WAD);
}

function directionFromAmounts(amount0, amount1) {
  if (amount0 < 0n) return { inputToken: 0, inputRaw: -amount0, outputToken: 1, outputRaw: absBigInt(amount1) };
  if (amount1 < 0n) return { inputToken: 1, inputRaw: -amount1, outputToken: 0, outputRaw: absBigInt(amount0) };
  return null;
}

async function buildImpactRows(logs) {
  const rows = [];
  let poolSqrt = null;
  let referenceSqrt = null;
  let lastOracleTs = null;
  let auctionStartTs = 0;
  let auctionPremiumWad = null;

  for (const lg of logs) {
    const ev = decodeEvent(lg);
    if (!ev) continue;

    if (ev.eventName === "Initialize") {
      poolSqrt = BigInt(ev.args.sqrtPriceX96);
      continue;
    }

    if (ev.eventName === "RiskUpdated") {
      referenceSqrt = referenceSqrtFromRiskPrice(ev.args.lastOraclePriceWad);
      lastOracleTs = Number(ev.args.lastOracleTs);
      continue;
    }

    if (ev.eventName === "AuctionOpened") {
      auctionStartTs = Number(ev.args.startTs);
      auctionPremiumWad = BigInt(ev.args.gapPremiumWad);
      continue;
    }

    if (ev.eventName === "AuctionClosed") {
      auctionStartTs = 0;
      auctionPremiumWad = null;
      continue;
    }

    if (ev.eventName !== "Swap") continue;
    const postSqrt = BigInt(ev.args.sqrtPriceX96);
    const amount0 = BigInt(ev.args.amount0);
    const amount1 = BigInt(ev.args.amount1);
    const dir = directionFromAmounts(amount0, amount1);
    const feeUnits = Number(ev.args.fee);
    const blockTs = await blockTimestamp(lg.blockNumber);
    const preSqrt = poolSqrt ?? (lg.blockNumber > 0n ? await historicalPoolSqrt(lg.blockNumber - 1n) : null);

    let premiumWad = null;
    if (referenceSqrt && preSqrt) {
      premiumWad = gapPremiumWad(referenceSqrt, preSqrt).premiumWad;
    } else if (auctionPremiumWad != null) {
      premiumWad = auctionPremiumWad;
    }

    poolSqrt = postSqrt;
    if (!dir || premiumWad == null) continue;

    const gapBps = gapBpsFromPremium(premiumWad);
    const trigger = S.cfg?.triggerGapBps ?? 10;
    if (gapBps < Math.max(1, trigger * 0.75)) continue;

    const noConcessionFeeUnits = unconcededFeeUnits(premiumWad);
    const baseFee = S.cfg?.baseFee ?? 500;
    const lpSurchargeUnits = Math.max(0, feeUnits - baseFee);
    const solverConcessionUnits = Math.max(0, noConcessionFeeUnits - feeUnits);
    const feeRefSqrt = referenceSqrt || postSqrt;
    const hookFeeUsdc = feeValueUsdc(dir.inputRaw, dir.inputToken, feeUnits, feeRefSqrt);
    const staticFeeUsdc = feeValueUsdc(dir.inputRaw, dir.inputToken, STATIC_BASELINE_FEE, feeRefSqrt);
    const lpSurchargeUsdc = feeValueUsdc(dir.inputRaw, dir.inputToken, lpSurchargeUnits, feeRefSqrt);
    const solverConcessionUsdc = feeValueUsdc(dir.inputRaw, dir.inputToken, solverConcessionUnits, feeRefSqrt);
    const concessionPct = cfgConcessionPctAt(auctionStartTs, blockTs)
      ?? impliedConcessionPct(feeUnits, noConcessionFeeUnits);
    const feedAgeSec = lastOracleTs ? Math.max(0, blockTs - lastOracleTs) : null;
    const prePoolPrice = preSqrt ? rawToWholePrice(rawPriceFromSqrt(preSqrt)) : null;
    const postPoolPrice = rawToWholePrice(rawPriceFromSqrt(postSqrt));

    rows.push({
      blockNumber: lg.blockNumber,
      blockTs,
      transactionHash: lg.transactionHash,
      sender: ev.args.sender,
      input: displayAmount(dir.inputRaw, dir.inputToken),
      output: displayAmount(dir.outputRaw, dir.outputToken),
      feeUnits,
      noConcessionFeeUnits,
      hookFeeUsdc,
      staticFeeUsdc,
      lpSurchargeUsdc,
      solverConcessionUsdc,
      concessionPct,
      feedAgeSec,
      gapBps,
      trigger,
      beforeAfterGap: `${gapBps.toFixed(2)} → 0.00 bps`,
      priceMove: `${prePoolPrice ? fmtPrice(prePoolPrice) : "?"} → ${fmtPrice(postPoolPrice)}`,
      useful: gapBps >= trigger,
      beforeFullConcession: concessionPct < 99.95,
      agedFeed: feedAgeSec != null && feedAgeSec > 3600,
    });
  }

  return rows.sort((a, b) => (a.blockNumber === b.blockNumber ? 0 : a.blockNumber > b.blockNumber ? -1 : 1));
}

function sumRows(rows, field) {
  return rows.reduce((acc, row) => acc + (Number.isFinite(row[field]) ? row[field] : 0), 0);
}

function impactRatio(numerator, denominator) {
  return denominator > 0 ? (numerator / denominator) * 100 : 0;
}

function renderImpactRows(rows) {
  const fillCount = rows.length;
  const hookFees = sumRows(rows, "hookFeeUsdc");
  const staticFees = sumRows(rows, "staticFeeUsdc");
  const lpSurcharge = sumRows(rows, "lpSurchargeUsdc");
  const solverConcession = sumRows(rows, "solverConcessionUsdc");
  const splitTotal = lpSurcharge + solverConcession;
  const beforeCap = rows.filter((r) => r.beforeFullConcession).length;
  const useful = rows.filter((r) => r.useful).length;
  const aged = rows.filter((r) => r.agedFeed).length;
  const last = rows[0];

  setImpact("impact-fills", fillCount ? `${fillCount}` : "0", fillCount ? "green" : "dim");
  setImpact("impact-hook-fees", fmtUsd(hookFees), hookFees > 0 ? "green" : "dim");
  setImpact("impact-static-fees", fmtUsd(staticFees), "dim");
  setImpact("impact-vs-static", fmtSignedUsd(hookFees - staticFees), hookFees >= staticFees ? "green" : "red");
  setImpact("impact-lp-surcharge", fmtUsd(lpSurcharge), lpSurcharge > 0 ? "green" : "dim");
  setImpact("impact-solver-conc", fmtUsd(solverConcession), solverConcession > 0 ? "amber" : "dim");
  setImpact("impact-solver-share", fmtPct(impactRatio(solverConcession, splitTotal)), solverConcession > 0 ? "amber" : "green");
  setImpact("impact-before-cap", fillCount ? `${beforeCap}/${fillCount}` : "0/0", beforeCap ? "green" : "amber");
  setImpact("impact-useful", fillCount ? `${useful}/${fillCount}` : "0/0", useful ? "green" : "dim");
  setImpact("impact-aged", fillCount ? `${aged}/${fillCount}` : "0/0", aged ? "amber" : "dim");
  setImpact("impact-last-gap", last ? `${last.gapBps.toFixed(2)} bps` : "—", last?.useful ? "green" : "dim");
  setImpact("impact-last-fee", last ? `${last.feeUnits} ppm` : "—", last?.feeUnits > (S.cfg?.baseFee ?? 500) ? "green" : "amber");

  const target = $("impactRows");
  if (!target) return;
  if (!rows.length) {
    target.innerHTML = `<div class="impactrow muted">No repricing fills found in the scanned event tail.</div>`;
    return;
  }

  const body = rows.slice(0, 10).map((r) => {
    const split = `${fmtUsd(r.lpSurchargeUsdc)} LP / ${fmtUsd(r.solverConcessionUsdc)} solver`;
    const timing = `${r.beforeFullConcession ? "before" : "at"} 100% · ${fmtPct(r.concessionPct, 1)} · feed ${r.feedAgeSec == null ? "?" : fmtAge(r.feedAgeSec)}`;
    return `<div class="impactrow">
      <span><a href="${EXPLORER}/tx/${r.transactionHash}" target="_blank">${shortHash(r.transactionHash)}</a></span>
      <span>${r.beforeAfterGap}</span>
      <span>${r.input} → ${r.output}</span>
      <span>${fmtUsd(r.hookFeeUsdc)} <span class="dim">vs ${fmtUsd(r.staticFeeUsdc)}</span></span>
      <span>${split}</span>
      <span class="${r.beforeFullConcession ? "pos" : "dim"}">${timing}</span>
    </div>`;
  }).join("");
  target.innerHTML = `<div class="impactrow head">
    <span>tx</span><span>gap before/after</span><span>fill</span><span>LP fee vs static</span><span>surcharge split</span><span>timing</span>
  </div>${body}`;
}

async function refreshImpact(logs = null) {
  const target = $("impactRows");
  if (target) target.innerHTML = `<div class="impactrow muted">scanning Base Sepolia PoolManager logs…</div>`;
  const eventLogs = logs ?? (await fetchEventTail()).collected;
  const rows = await buildImpactRows(eventLogs);
  renderImpactRows(rows);
  return rows;
}

async function loadEvents() {
  const { collected, scanned, requests } = await fetchEventTail();
  await refreshImpact(collected);
  for (const lg of collected) {
    decodeAndLogEvent(
      lg,
      `block ${lg.blockNumber}`
    );
  }
  busLog({ kind: "LOG", title: `event tail: ${collected.length} events over the last ${scanned.toLocaleString()} blocks`, meta: `eth_getLogs ×${requests}` });
}
function decodeAndLogEvent(lg, meta) {
  let title = "unknown event", dec = "";
  try {
    const ev = decodeEventLog({ abi: allEventAbis, data: lg.data, topics: lg.topics });
    title = `${ev.eventName} @ ${lg.address.slice(0, 10)}…`;
    dec = fmtArgs(ev.args);
  } catch { title = `raw log @ ${lg.address.slice(0, 10)}…`; }
  busLog({ kind: "LOG", title, meta, raw: { topics: lg.topics, logdata: lg.data, tx: lg.transactionHash }, decoded: dec });
}

// ── wallet + actions ────────────────────────────────────────────────────────
let account = null;
const eth = () => window.ethereum;

async function ensureChain() {
  const idHex = "0x" + CHAIN_ID.toString(16);
  try {
    await eth().request({ method: "wallet_switchEthereumChain", params: [{ chainId: idHex }] });
  } catch (e) {
    if (e.code === 4902) {
      await eth().request({ method: "wallet_addEthereumChain", params: [{
        chainId: idHex, chainName: "Base Sepolia", rpcUrls: [RPC],
        nativeCurrency: { name: "ETH", symbol: "ETH", decimals: 18 },
        blockExplorerUrls: [EXPLORER] }] });
    } else throw e;
  }
}

async function connect() {
  if (!eth()) { alert("No wallet detected. Install MetaMask/Rabby, fund it on Base Sepolia, and reload."); return; }
  await ensureChain();
  const [acc] = await eth().request({ method: "eth_requestAccounts" });
  account = acc;
  $("addr").textContent = acc.slice(0, 6) + "…" + acc.slice(-4);
  document.querySelectorAll("button[data-needs-wallet]").forEach((b) => (b.disabled = false));
  $("connect").textContent = "connected";
  trackVisit("wallet_connect", { chainId: CHAIN_ID });
  await refreshBalances();
}

async function refreshBalances() {
  if (!account) return;
  const b0 = await loggedCall(erc20Abi, A.token0, "balanceOf", [account]);
  const b1 = await loggedCall(erc20Abi, A.token1, "balanceOf", [account]);
  const liq0 = await loggedCall(erc20Abi, A.token0, "allowance", [account, A.liquidityRouter]);
  const liq1 = await loggedCall(erc20Abi, A.token1, "allowance", [account, A.liquidityRouter]);
  const swap0 = await loggedCall(erc20Abi, A.token0, "allowance", [account, A.swapRouter]);
  const swap1 = await loggedCall(erc20Abi, A.token1, "allowance", [account, A.swapRouter]);
  S.balances = { b0, b1 };
  S.allowances = { liq0, liq1, swap0, swap1 };
  $("bal").textContent = `${A.token0Symbol} ${formatUnits(b0, A.token0Decimals)} · ${A.token1Symbol} ${formatUnits(b1, A.token1Decimals)}`;
  set("allow-liq", liq0 > 0n && liq1 > 0n ? "ready" : "needs approval", liq0 > 0n && liq1 > 0n ? "green" : "amber");
  set("allow-swap", swap0 > 0n && swap1 > 0n ? "ready" : "needs approval", swap0 > 0n && swap1 > 0n ? "green" : "amber");
}

async function sendTx(abi, to, functionName, args, label, value = 0n) {
  const data = encodeFunctionData({ abi, functionName, args });
  const tx = { from: account, to, data };
  if (value > 0n) tx.value = toTxHex(value);
  busLog({ kind: "TX", title: `${label} - submitting`, meta: functionName, raw: { to, data } });
  const hash = await eth().request({ method: "eth_sendTransaction", params: [tx] });
  busLog({ kind: "TX", title: `${label} - pending`, meta: hash.slice(0, 12) + "…", raw: { tx: hash } });
  const rcpt = await client.waitForTransactionReceipt({ hash });
  busLog({ kind: rcpt.status === "success" ? "TX" : "ERR",
    title: `${label} - ${rcpt.status} (gas ${rcpt.gasUsed})`, meta: `block ${rcpt.blockNumber}`, raw: { tx: hash } });
  for (const lg of rcpt.logs) decodeAndLogEvent(lg, `tx log #${lg.logIndex}`);
  await refresh();
  await refreshBalances();
  return rcpt;
}

async function approveRouters() {
  if (!account) throw new Error("connect wallet first");
  await refreshBalances();
  const approvals = [
    { token: A.token0, symbol: A.token0Symbol, spender: A.liquidityRouter, field: "liq0", label: "liquidity router" },
    { token: A.token1, symbol: A.token1Symbol, spender: A.liquidityRouter, field: "liq1", label: "liquidity router" },
    { token: A.token0, symbol: A.token0Symbol, spender: A.swapRouter, field: "swap0", label: "swap router" },
    { token: A.token1, symbol: A.token1Symbol, spender: A.swapRouter, field: "swap1", label: "swap router" },
  ].filter((a) => (S.allowances?.[a.field] || 0n) === 0n);
  if (!approvals.length) {
    busLog({ kind: "TX", title: "router approvals already ready", meta: "USDC/WETH" });
    return;
  }
  for (const a of approvals) {
    await sendTx(erc20Abi, a.token, "approve", [a.spender, MAX_UINT], `approve ${a.symbol} to ${a.label}`);
  }
  await refreshBalances();
}

async function wrapWeth() {
  if (!account) throw new Error("connect wallet first");
  const amount = parseAmountInput("wrapAmount", 18, "0.01");
  await sendTx(erc20Abi, A.token1, "deposit", [], `wrap ${formatUnits(amount, 18, 6)} ETH to WETH`, amount);
}

function liquidityDeltaInput() {
  const raw = ($("liqUnits")?.value || "").trim().replaceAll("_", "");
  if (!/^[0-9]+$/.test(raw)) throw new Error("liquidity units must be a whole number");
  const delta = BigInt(raw);
  if (delta <= 0n) throw new Error("liquidity units must be greater than zero");
  return delta;
}

async function addLiquidity() {
  if (!account) throw new Error("connect wallet first");
  const { tickLower, tickUpper } = plannedRange();
  const liquidityDelta = liquidityDeltaInput();
  await sendTx(
    routerAbi,
    A.liquidityRouter,
    "modifyLiquidity",
    [KEY, [tickLower, tickUpper, liquidityDelta, ZERO_SALT], ZERO_BYTES],
    `add active liquidity ${tickLower}/${tickUpper}`,
  );
}

async function fillTowardOracle() {
  if (!account) throw new Error("connect wallet first");
  const plan = toxicPlan();
  if (!plan) throw new Error("no trigger-eligible toxic direction to fill");
  if (!S.status?.[0] || Number(S.status[1]) === 0) throw new Error("auction clock is not running; poke first");
  if (S.liquidity === 0n) throw new Error("pool has no active liquidity");
  const amount = parseAmountInput("fillAmount", plan.inputDecimals, plan.zeroForOne ? "10" : "0.005");
  await sendTx(
    routerAbi,
    A.swapRouter,
    "swap",
    [KEY, [plan.zeroForOne, -amount, plan.preview[2]], [false, false], ZERO_BYTES],
    `fill ${plan.inputSymbol}->${plan.outputSymbol} toward oracle`,
  );
}

const actions = {
  connect,
  refreshNow: refresh,
  approveRouters,
  wrapWeth,
  addLiquidity,
  poke: async () => sendTx(hookAbi, A.hook, "pokeAuction", [KEY], "pokeAuction (open the clock)"),
  fill: fillTowardOracle,
};
for (const [id, fn] of Object.entries(actions)) {
  const el = $(id);
  if (el) el.addEventListener("click", () => fn().catch((e) => {
    busLog({ kind: "ERR", title: `${id} failed`, meta: (e.revertName ?? e?.shortMessage ?? e?.message ?? "error").slice(0, 90) });
  }));
}
$("fillAmount")?.addEventListener("input", (e) => { e.target.dataset.dirty = "1"; });
$("refreshImpact")?.addEventListener("click", () => refreshImpact().catch((e) => {
  busLog({ kind: "ERR", title: "impact refresh failed", meta: (e?.message ?? "error").slice(0, 90) });
}));

// ── boot ────────────────────────────────────────────────────────────────────
trackVisit("pageview");
let leaveTracked = false;
document.addEventListener("visibilitychange", () => {
  if (!leaveTracked && document.visibilityState === "hidden") {
    leaveTracked = true;
    trackVisit("leave", { ageMs: Math.round(performance.now()) });
  }
});
busLog({ kind: "CALL", title: "instrument boot — every EVM read/write on this page is logged here", meta: RPC });
refresh().finally(() => loadEvents());
setInterval(refresh, 5000);
window.addEventListener("resize", () => S.preview01 && render());
