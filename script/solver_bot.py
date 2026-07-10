#!/usr/bin/env python3
"""Solver/keeper bot for the OracleAnchoredLVRHook Dutch auction.

Watches a hooked pool's auction state and closes the loop the backtests model:
when a stale gap opens it pokes the auction clock, and once the scheduled
concession clears the configured threshold it executes the repricing swap
through a PoolSwapTest router with the reference sqrt price as the price limit
(so the pool lands exactly on the oracle price and the auction closes).

Stdlib-only: all chain access goes through the Foundry `cast` binary, matching
the repo's tooling. Compatible with Python 3.9+.

Subcommands:
  status          print auction/fee state and the current trigger-gap size
  poke            open (or close) the auction clock from the current gap
  fill            execute the repricing swap now
  make-gap        move the demo base feed to create a stale gap (demo pools)
  refresh-oracle  re-stamp the demo feeds so they do not go stale (demo pools)
  run             poll loop: poke on gap open, fill once profitable

Example against the Base Sepolia demo pool (addresses from DeployDemoPool):
  python3 script/solver_bot.py run --keep-fresh --min-concession-wad 5e15

Configuration comes from flags or the environment: HOOK, TOKEN0, TOKEN1,
SWAP_ROUTER, BASE_FEED, QUOTE_FEED, TICK_SPACING, RPC_URL (falls back to
BASE_SEPOLIA_RPC_URL), PRIVATE_KEY (falls back to DEPLOYER_KEY).
"""

import argparse
import os
import subprocess
import sys
import time
from typing import List, Optional, Tuple

WAD = 10**18
HALF_BPS_WAD = 5 * 10**13  # hook's premium unit: 1 trigger-bps of gap
DYNAMIC_FEE_FLAG = 0x800000
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_SALT = "0x" + "0" * 64
Q96 = 2**96
POOLS_MAPPING_SLOT = 6  # PoolManager: mapping(PoolId => Pool.State) at slot 6

POOL_KEY_ABI = "(address,address,uint24,int24,address)"
SWAP_ABI = (
    "swap((address,address,uint24,int24,address),(bool,int256,uint160),(bool,bool),bytes)"
)


# ---------------------------------------------------------------------------
# Pure decision/math helpers (unit-tested in test_solver_bot.py)
# ---------------------------------------------------------------------------

def gap_premium_wad(ref_sqrtp: int, pool_sqrtp: int) -> Tuple[int, bool]:
    """Unsigned sqrt-price premium of the pool-oracle gap in WAD, mirroring the
    hook's _gapPremiumWad, plus whether the oracle sits above the pool."""
    if ref_sqrtp > pool_sqrtp:
        return ref_sqrtp * WAD // pool_sqrtp - WAD, True
    if ref_sqrtp < pool_sqrtp:
        return pool_sqrtp * WAD // ref_sqrtp - WAD, False
    return 0, False


def gap_trigger_bps(premium_wad: int) -> float:
    """Gap size in the hook's trigger units (a 10 bps stale gap ~= 10.0)."""
    return premium_wad / HALF_BPS_WAD


def toxic_zero_for_one(oracle_above_pool: bool) -> bool:
    """Direction of the repricing (toxic) swap: buying the undervalued side.
    Oracle above pool means token0 is cheap, so the repricer sells token1
    (zeroForOne = False); mirrored otherwise."""
    return not oracle_above_pool


def should_poke(eligible: bool, start_ts: int) -> bool:
    """Poke when a trigger-eligible gap exists but the clock has not started,
    so the concession accrues from gap birth rather than from the first swap."""
    return eligible and start_ts == 0


def should_fill(
    eligible: bool,
    start_ts: int,
    concession_wad: int,
    min_concession_wad: int,
    fee_deterred: bool,
) -> bool:
    """Fill once the auction is open and the scheduled concession has grown past
    the solver's threshold. A fee still above maxFee (fail-closed preview) means
    the swap cannot clear yet regardless of willingness."""
    if fee_deterred:
        return False
    return eligible and start_ts != 0 and concession_wad >= min_concession_wad


def feed_answer_for_gap(current_answer: int, gap_bps: float) -> int:
    """Base-feed answer that moves the reference `gap_bps` (price bps) away from
    where the feed points now; the hook's premium reads this as ~gap_bps trigger
    units. Negative gap_bps moves the reference below the pool."""
    return int(current_answer * (10_000 + gap_bps) // 10_000)


def decode_balance_delta(packed: int) -> Tuple[int, int]:
    """Split a v4 BalanceDelta (int256 packing two int128s) into signed
    (amount0, amount1)."""
    unsigned = packed & ((1 << 256) - 1)
    lower = unsigned & ((1 << 128) - 1)
    upper = unsigned >> 128
    amount1 = lower - (1 << 128) if lower >= (1 << 127) else lower
    amount0 = upper - (1 << 128) if upper >= (1 << 127) else upper
    return amount0, amount1


def position_value_token1(amount0: int, amount1: int, ref_sqrtp: int) -> int:
    """Value of a token pair in token1 raw units at the reference price implied
    by `ref_sqrtp` (amount0 * P_ref + amount1, integer math)."""
    return amount0 * ref_sqrtp * ref_sqrtp // (Q96 * Q96) + amount1


def parse_cast_values(output: str) -> List[str]:
    """Values from `cast call` multi-return output: one value per line, with
    display suffixes like `1000000 [1e6]` stripped."""
    values = []
    for line in output.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        values.append(line.split(" ")[0])
    return values


# ---------------------------------------------------------------------------
# Chain access via cast
# ---------------------------------------------------------------------------

class Chain:
    def __init__(self, rpc_url: str, private_key: Optional[str]):
        self.rpc_url = rpc_url
        self.private_key = private_key

    def call(self, target: str, sig: str, *args: str) -> List[str]:
        cmd = ["cast", "call", target, sig, *args, "--rpc-url", self.rpc_url]
        out = subprocess.run(cmd, capture_output=True, text=True)
        if out.returncode != 0:
            raise CallReverted(out.stderr.strip())
        return parse_cast_values(out.stdout)

    # Public RPCs are often load-balanced across nodes with inconsistent mempool
    # views, so back-to-back sends can transiently collide on nonce estimation.
    TRANSIENT_SEND_ERRORS = ("replacement transaction underpriced", "nonce too low")

    def send(self, target: str, sig: str, *args: str) -> str:
        if not self.private_key:
            raise RuntimeError("no PRIVATE_KEY/DEPLOYER_KEY configured for sending")
        cmd = [
            "cast", "send", target, sig, *args,
            "--rpc-url", self.rpc_url, "--private-key", self.private_key, "--json",
        ]
        attempts = 3
        for attempt in range(attempts):
            out = subprocess.run(cmd, capture_output=True, text=True)
            if out.returncode == 0:
                break
            error = out.stderr.strip()
            transient = any(marker in error for marker in self.TRANSIENT_SEND_ERRORS)
            if not transient or attempt == attempts - 1:
                raise RuntimeError("cast send failed: %s" % error)
            time.sleep(4)
        import json
        receipt = json.loads(out.stdout)
        if int(receipt.get("status", "0x0"), 16) != 1:
            raise RuntimeError("transaction reverted: %s" % receipt.get("transactionHash"))
        return receipt["transactionHash"]


class CallReverted(Exception):
    pass


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class SolverBot:
    def __init__(self, chain: Chain, cfg: argparse.Namespace):
        self.chain = chain
        self.hook = cfg.hook
        self.swap_router = cfg.swap_router
        self.base_feed = cfg.base_feed
        self.quote_feed = cfg.quote_feed
        self.liquidity_router = cfg.liquidity_router
        self.baseline_fee = cfg.baseline_fee
        self.tick_lower = cfg.tick_lower
        self.tick_upper = cfg.tick_upper
        self.seed_liquidity = cfg.seed_liquidity
        self.caller = os.environ.get("DEPLOYER_ADDRESS")
        self._pool_manager = None  # type: Optional[str]
        self.key_tuple = "(%s,%s,%d,%d,%s)" % (
            cfg.token0, cfg.token1, DYNAMIC_FEE_FLAG, cfg.tick_spacing, cfg.hook
        )
        self.baseline_key_tuple = "(%s,%s,%d,%d,%s)" % (
            cfg.token0, cfg.token1, cfg.baseline_fee, cfg.tick_spacing, ZERO_ADDRESS
        )

    # -- reads --------------------------------------------------------------

    def auction_status(self):
        vals = self.chain.call(
            self.hook,
            "auctionStatus(%s)(bool,uint64,uint256,uint256)" % POOL_KEY_ABI,
            self.key_tuple,
        )
        return vals[0] == "true", int(vals[1]), int(vals[2]), int(vals[3])

    def preview(self, zero_for_one: bool):
        vals = self.chain.call(
            self.hook,
            "previewSwapFee(%s,bool)(bool,uint24,uint160,uint160)" % POOL_KEY_ABI,
            self.key_tuple,
            "true" if zero_for_one else "false",
        )
        return vals[0] == "true", int(vals[1]), int(vals[2]), int(vals[3])

    def read_gap(self):
        """(premium_wad, oracle_above, toxic_fee_or_None, ref, pool).

        The toxic-direction preview fails closed above maxFee; the benign one
        never does, so at least one call returns the sqrt prices."""
        toxic_fee = None
        try:
            toxic0, fee0, ref, pool = self.preview(True)
            if toxic0:
                toxic_fee = fee0
            else:
                try:
                    toxic1, fee1, _, _ = self.preview(False)
                    if toxic1:
                        toxic_fee = fee1
                except CallReverted:
                    toxic_fee = None  # toxic direction deterred above maxFee
        except CallReverted:
            _, _, ref, pool = self.preview(False)
            toxic_fee = None  # zeroForOne was the toxic, deterred direction
        premium, above = gap_premium_wad(ref, pool)
        return premium, above, toxic_fee, ref, pool

    def pool_manager(self) -> str:
        if self._pool_manager is None:
            self._pool_manager = self.chain.call(
                self.hook, "poolManager()(address)"
            )[0]
        return self._pool_manager

    def pool_sqrtp(self, key_tuple: str) -> int:
        """Any pool's current sqrt price straight from PoolManager storage via
        extsload (slot0 is the first word of Pool.State), so no lens contract or
        hook is needed - works for the hookless baseline pool."""
        encoded = subprocess.run(
            ["cast", "abi-encode", "f(%s)" % POOL_KEY_ABI, key_tuple],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        pool_id = subprocess.run(
            ["cast", "keccak", encoded], capture_output=True, text=True, check=True
        ).stdout.strip()
        state_slot = subprocess.run(
            ["cast", "index", "bytes32", pool_id, str(POOLS_MAPPING_SLOT)],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        word = self.chain.call(
            self.pool_manager(), "extsload(bytes32)(bytes32)", state_slot
        )[0]
        return int(word, 16) & ((1 << 160) - 1)

    def withdraw_simulation(self, key_tuple: str) -> Tuple[int, int]:
        """Simulated full withdrawal of the seeded position (eth_call, nothing
        is broadcast): returns the exact token amounts the LP would receive now,
        fees included."""
        params = "(%d,%d,-%d,%s)" % (
            self.tick_lower, self.tick_upper, self.seed_liquidity, ZERO_SALT
        )
        cmd = [
            "cast", "call", self.liquidity_router,
            "modifyLiquidity(%s,(int24,int24,int256,bytes32),bytes)(int256)"
            % POOL_KEY_ABI,
            key_tuple, params, "0x", "--rpc-url", self.chain.rpc_url,
        ]
        if self.caller:
            cmd += ["--from", self.caller]
        out = subprocess.run(cmd, capture_output=True, text=True)
        if out.returncode != 0:
            raise CallReverted(out.stderr.strip())
        packed = int(parse_cast_values(out.stdout)[0])
        return decode_balance_delta(packed)

    def feed_round(self, feed: str) -> Tuple[int, int]:
        vals = self.chain.call(
            feed, "latestRoundData()(uint80,int256,uint256,uint256,uint80)"
        )
        return int(vals[1]), int(vals[3])

    def feed_answer(self, feed: str) -> int:
        return self.feed_round(feed)[0]

    # -- actions ------------------------------------------------------------

    def poke(self) -> str:
        return self.chain.send(
            self.hook, "pokeAuction(%s)" % POOL_KEY_ABI, self.key_tuple
        )

    def fill(self, amount_in: int, baseline: bool = False) -> str:
        premium, above, _, ref, _ = self.read_gap()
        key_tuple = self.key_tuple
        if baseline:
            key_tuple = self.baseline_key_tuple
            premium, above = gap_premium_wad(ref, self.pool_sqrtp(key_tuple))
        if premium == 0:
            raise RuntimeError("no gap to reprice")
        zero_for_one = toxic_zero_for_one(above)
        swap_params = "(%s,-%d,%d)" % (str(zero_for_one).lower(), amount_in, ref)
        return self.chain.send(
            self.swap_router, SWAP_ABI, key_tuple, swap_params, "(false,false)", "0x"
        )

    def compare(self) -> None:
        """Side-by-side LP value of the hooked and baseline pools: simulated
        full withdrawal of the identical seeded position, valued in token1 raw
        units at the current oracle reference price."""
        _, _, _, ref, hooked_sqrtp = self.read_gap()
        pools = (
            ("hooked", self.key_tuple, hooked_sqrtp),
            ("baseline", self.baseline_key_tuple, self.pool_sqrtp(self.baseline_key_tuple)),
        )
        values = {}
        for name, key_tuple, sqrtp in pools:
            premium, _ = gap_premium_wad(ref, sqrtp)
            amount0, amount1 = self.withdraw_simulation(key_tuple)
            value = position_value_token1(amount0, amount1, ref)
            values[name] = value
            log(
                "%-8s | gap %6.2f trigger-bps | withdrawable %s token0 + %s token1"
                " | value %s token1-units"
                % (name, gap_trigger_bps(premium), amount0, amount1, value)
            )
        delta = values["hooked"] - values["baseline"]
        log(
            "hooked LP - baseline LP = %+d token1-units (%+.6f whole tokens)"
            % (delta, delta / 10**18)
        )

    def refresh_oracle(self, max_age_secs: int = 0) -> None:
        """Re-stamp the demo feeds. With `max_age_secs` set, feeds younger than
        that are left alone, so a polling loop does not race its own pending
        re-stamp transactions."""
        now = int(time.time())
        for feed in (self.base_feed, self.quote_feed):
            if not feed:
                continue
            answer, updated_at = self.feed_round(feed)
            if max_age_secs and now - updated_at < max_age_secs:
                continue
            self.chain.send(
                feed, "setRoundData(int256,uint256)", str(answer), str(now)
            )

    def make_gap(self, gap_bps: float) -> str:
        if not self.base_feed:
            raise RuntimeError("make-gap needs --base-feed (demo pools only)")
        answer = self.feed_answer(self.base_feed)
        target = feed_answer_for_gap(answer, gap_bps)
        now = int(time.time())
        tx = self.chain.send(
            self.base_feed, "setRoundData(int256,uint256)", str(target), str(now)
        )
        if self.quote_feed:
            quote = self.feed_answer(self.quote_feed)
            self.chain.send(
                self.quote_feed, "setRoundData(int256,uint256)", str(quote), str(now)
            )
        return tx

    # -- loop ---------------------------------------------------------------

    def tick(self, min_concession_wad: int, amount_in: int, keep_fresh: bool) -> str:
        if keep_fresh:
            # Half the demo config's default 24h max oracle age, with margin.
            self.refresh_oracle(max_age_secs=6 * 3600)

        eligible, start_ts, concession, _ = self.auction_status()
        premium, _, toxic_fee, _, _ = self.read_gap()
        fee_deterred = toxic_fee is None and premium > 0

        log(
            "gap %.2f trigger-bps | eligible=%s clock=%s concession=%.4f%% | toxic fee %s"
            % (
                gap_trigger_bps(premium),
                eligible,
                start_ts or "-",
                concession / WAD * 100,
                ("%d ppm" % toxic_fee) if toxic_fee is not None else "deterred/none",
            )
        )

        if should_poke(eligible, start_ts):
            tx = self.poke()
            log("poked auction clock: %s" % tx)
            return "poked"
        if should_fill(eligible, start_ts, concession, min_concession_wad, fee_deterred):
            tx = self.fill(amount_in)
            log("filled repricing swap: %s" % tx)
            return "filled"
        return "waited"


def log(msg: str) -> None:
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rpc-url", default=os.environ.get("RPC_URL")
                   or os.environ.get("BASE_SEPOLIA_RPC_URL"))
    p.add_argument("--private-key", default=os.environ.get("PRIVATE_KEY")
                   or os.environ.get("DEPLOYER_KEY"))
    p.add_argument("--hook", default=os.environ.get("HOOK"))
    p.add_argument("--token0", default=os.environ.get("TOKEN0"))
    p.add_argument("--token1", default=os.environ.get("TOKEN1"))
    p.add_argument("--swap-router", default=os.environ.get("SWAP_ROUTER"))
    p.add_argument("--base-feed", default=os.environ.get("BASE_FEED"))
    p.add_argument("--quote-feed", default=os.environ.get("QUOTE_FEED"))
    p.add_argument("--tick-spacing", type=int,
                   default=int(os.environ.get("TICK_SPACING", "60")))
    p.add_argument("--liquidity-router", default=os.environ.get("LIQUIDITY_ROUTER"))
    p.add_argument("--baseline-fee", type=int,
                   default=int(os.environ.get("BASELINE_FEE", "3000")),
                   help="static fee (ppm) of the hookless control pool")
    p.add_argument("--tick-lower", type=int, default=-12_000)
    p.add_argument("--tick-upper", type=int, default=12_000)
    p.add_argument("--seed-liquidity", type=int, default=10**21,
                   help="liquidity of the seeded position in both pools")

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    sub.add_parser("poke")
    sub.add_parser("refresh-oracle")
    sub.add_parser("compare")

    fill = sub.add_parser("fill")
    fill.add_argument("--amount-in", type=float, default=100e18,
                      help="max exact-in size; the ref price limit truncates it")
    fill.add_argument("--pool", choices=("hooked", "baseline"), default="hooked")

    gap = sub.add_parser("make-gap")
    gap.add_argument("--bps", type=float, required=True,
                     help="price gap in bps; negative moves the reference down")

    run = sub.add_parser("run")
    run.add_argument("--interval", type=float, default=10.0)
    run.add_argument("--max-iterations", type=int, default=0,
                     help="stop after N ticks (0 = forever)")
    run.add_argument("--min-concession-wad", type=float, default=1e15)
    run.add_argument("--amount-in", type=float, default=100e18)
    run.add_argument("--keep-fresh", action="store_true",
                     help="re-stamp demo feeds every tick so they never go stale")
    return p


def main() -> int:
    args = build_parser().parse_args()
    for required in ("rpc_url", "hook", "token0", "token1"):
        if not getattr(args, required):
            print("missing --%s (or its environment variable)" % required.replace("_", "-"))
            return 2

    bot = SolverBot(Chain(args.rpc_url, args.private_key), args)

    if args.command == "status":
        eligible, start_ts, concession, premium_status = bot.auction_status()
        premium, above, toxic_fee, ref, pool = bot.read_gap()
        log("gap: %.2f trigger-bps (oracle %s pool)" % (
            gap_trigger_bps(premium), "above" if above else "at/below"))
        log("auction: eligible=%s startTs=%s concession=%.4f%% premium=%d" % (
            eligible, start_ts, concession / WAD * 100, premium_status))
        log("toxic fee: %s | ref sqrtP %d | pool sqrtP %d" % (
            ("%d ppm" % toxic_fee) if toxic_fee is not None else "deterred/none", ref, pool))
    elif args.command == "poke":
        log("poke tx: %s" % bot.poke())
    elif args.command == "refresh-oracle":
        bot.refresh_oracle()
        log("feeds re-stamped")
    elif args.command == "fill":
        log("fill tx: %s" % bot.fill(int(args.amount_in), args.pool == "baseline"))
    elif args.command == "compare":
        bot.compare()
    elif args.command == "make-gap":
        log("make-gap tx: %s" % bot.make_gap(args.bps))
    elif args.command == "run":
        i = 0
        while True:
            try:
                bot.tick(int(args.min_concession_wad), int(args.amount_in), args.keep_fresh)
            except CallReverted as exc:
                log("read reverted (stale oracle or missing config?): %s" % exc)
            except RuntimeError as exc:
                log("action failed: %s" % exc)
            i += 1
            if args.max_iterations and i >= args.max_iterations:
                break
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
