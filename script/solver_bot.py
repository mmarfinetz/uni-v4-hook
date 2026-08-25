#!/usr/bin/env python3
"""Solver/keeper bot for the OracleAnchoredLVRHook Dutch auction.

Watches a hooked pool's auction state and closes the loop the backtests model:
when a stale gap opens it pokes the auction clock, and once the scheduled
concession clears the configured threshold it executes the repricing swap
through a PoolSwapTest router with the reference sqrt price as the price limit
(so the swap cannot cross the oracle price; it closes the gap when the configured
directional input cap is large enough).

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
  python3 script/solver_bot.py --native-token-price-token1 1 \
    run --keep-fresh --min-concession-wad 5e15

Configuration comes from flags or the environment: HOOK, TOKEN0, TOKEN1,
SWAP_ROUTER, BASE_FEED, QUOTE_FEED, TICK_SPACING, RPC_URL (falls back to
BASE_SEPOLIA_RPC_URL), and KEYSTORE_ACCOUNT. Raw private keys are limited to an
explicit unsafe testnet opt-in in continuous run mode.
"""

import argparse
import fcntl
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
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


@dataclass(frozen=True)
class FillEconomics:
    gross_surplus_token1: int
    gas_cost_token1: int
    required_edge_token1: int
    minimum_profit_token1: int
    estimated_lp_fee_token1: int
    available_gap_value_token1: int
    required_compensation_token1: int
    minimum_compensation_wad: Optional[int]
    current_solver_share_wad: Optional[int]
    free_energy_gap_potential_wad: int
    net_profit_token1: int
    profitable: bool
    reason: str


def evaluate_fill_economics(
    *,
    gross_surplus_token1: int,
    gas_cost_token1: int,
    required_edge_token1: int,
    minimum_profit_token1: int,
    estimated_lp_fee_token1: int = 0,
    free_energy_gap_potential_wad: int = 0,
) -> FillEconomics:
    """Apply solver gates and expose the minimum share of gap value required."""
    if min(
        gross_surplus_token1,
        gas_cost_token1,
        required_edge_token1,
        minimum_profit_token1,
        estimated_lp_fee_token1,
        free_energy_gap_potential_wad,
    ) < 0:
        raise ValueError("fill economics inputs must be non-negative")
    required = gas_cost_token1 + required_edge_token1 + minimum_profit_token1
    available_gap_value = gross_surplus_token1 + estimated_lp_fee_token1
    minimum_compensation = minimum_compensation_fraction_wad(
        required_compensation_token1=required,
        available_gap_value_token1=available_gap_value,
    )
    current_solver_share = minimum_compensation_fraction_wad(
        required_compensation_token1=gross_surplus_token1,
        available_gap_value_token1=available_gap_value,
    )
    net_profit = gross_surplus_token1 - gas_cost_token1
    profitable = gross_surplus_token1 >= required
    return FillEconomics(
        gross_surplus_token1=gross_surplus_token1,
        gas_cost_token1=gas_cost_token1,
        required_edge_token1=required_edge_token1,
        minimum_profit_token1=minimum_profit_token1,
        estimated_lp_fee_token1=estimated_lp_fee_token1,
        available_gap_value_token1=available_gap_value,
        required_compensation_token1=required,
        minimum_compensation_wad=minimum_compensation,
        current_solver_share_wad=current_solver_share,
        free_energy_gap_potential_wad=free_energy_gap_potential_wad,
        net_profit_token1=net_profit,
        profitable=profitable,
        reason="profitable" if profitable else "below_profit_reserve",
    )


def minimum_compensation_fraction_wad(
    *,
    required_compensation_token1: int,
    available_gap_value_token1: int,
) -> Optional[int]:
    """Ceiling of required execution value divided by available stale-gap value."""
    if min(required_compensation_token1, available_gap_value_token1) < 0:
        raise ValueError("compensation inputs must be non-negative")
    if required_compensation_token1 == 0:
        return 0
    if available_gap_value_token1 == 0:
        return None
    return (
        required_compensation_token1 * WAD + available_gap_value_token1 - 1
    ) // available_gap_value_token1


def free_energy_gap_potential_wad(premium_wad: int) -> int:
    """Return (exp(|z|/2)-1)^2 in WAD from the hook's premium directly."""
    if premium_wad < 0:
        raise ValueError("premium_wad must be non-negative")
    return premium_wad * premium_wad // WAD


def estimated_input_fee_token1(input_notional_token1: int, fee_ppm: int) -> int:
    """Conservatively estimate the LP fee from gross input notional and fee ppm."""
    if input_notional_token1 < 0:
        raise ValueError("input_notional_token1 must be non-negative")
    if not 0 <= fee_ppm <= 1_000_000:
        raise ValueError("fee_ppm must be between 0 and 1000000")
    if input_notional_token1 == 0 or fee_ppm == 0:
        return 0
    return (input_notional_token1 * fee_ppm + 1_000_000 - 1) // 1_000_000


def input_notional_token1_from_delta(
    *,
    amount0: int,
    amount1: int,
    zero_for_one: bool,
    reference_sqrt_price_x96: int,
) -> int:
    """Value the exact input actually consumed by the simulated swap in token1."""
    raw_input = -amount0 if zero_for_one else -amount1
    if raw_input <= 0:
        raise ValueError("simulated BalanceDelta does not contain the expected input")
    if zero_for_one:
        return position_value_token1(raw_input, 0, reference_sqrt_price_x96)
    return raw_input


def gas_cost_in_token1_raw(
    gas_units: int,
    gas_price_wei: int,
    native_token_price_token1_wad: int,
    token1_decimals: int,
) -> int:
    """Convert native gas cost into token1 raw units with integer arithmetic."""
    if min(gas_units, gas_price_wei, native_token_price_token1_wad) < 0:
        raise ValueError("gas inputs must be non-negative")
    if not 0 <= token1_decimals <= 36:
        raise ValueError("token1_decimals must be between 0 and 36")
    return (
        gas_units
        * gas_price_wei
        * native_token_price_token1_wad
        * 10**token1_decimals
        // 10**36
    )


def parse_rate_wad(value: str) -> int:
    """Parse a positive whole-token exchange rate into WAD."""
    try:
        rate = Decimal(str(value).replace("_", ""))
    except InvalidOperation as exc:
        raise ValueError("invalid native/token1 rate: %s" % value) from exc
    if rate <= 0:
        raise ValueError("native/token1 rate must be positive")
    scaled = rate * WAD
    if scaled != scaled.to_integral_value():
        raise ValueError("native/token1 rate has more than 18 decimal places")
    return int(scaled)


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
    """Poke when on-chain auction state is stale relative to the current gap.

    Above trigger, this starts the clock so concession accrues from gap birth.
    Below trigger, this clears any old clock so the next eligible gap does not
    inherit aged concession from a previous auction.
    """
    return (eligible and start_ts == 0) or (not eligible and start_ts != 0)


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


def _parse_int(value) -> int:
    if isinstance(value, int):
        return value
    text = str(value).strip().strip('"')
    return int(text, 16) if text.lower().startswith("0x") else int(text)


def parse_units(value: str, decimals: int) -> int:
    """Parse a human token amount into raw units without binary-float rounding."""
    try:
        amount = Decimal(str(value).replace("_", ""))
    except InvalidOperation as exc:
        raise ValueError("invalid token amount: %s" % value) from exc
    if amount <= 0:
        raise ValueError("token amount must be positive: %s" % value)
    scale = Decimal(10) ** decimals
    raw = amount * scale
    if raw != raw.to_integral_value():
        raise ValueError("%s has more than %d decimal places" % (value, decimals))
    return int(raw)


def parse_nonnegative_units(value: str, decimals: int) -> int:
    """Parse a non-negative human token amount into raw units."""
    try:
        amount = Decimal(str(value).replace("_", ""))
    except InvalidOperation as exc:
        raise ValueError("invalid token amount: %s" % value) from exc
    if amount < 0:
        raise ValueError("token amount must be non-negative: %s" % value)
    scale = Decimal(10) ** decimals
    raw = amount * scale
    if raw != raw.to_integral_value():
        raise ValueError("%s has more than %d decimal places" % (value, decimals))
    return int(raw)


def parse_raw_units(value: Optional[str]) -> Optional[int]:
    """Parse a legacy raw-unit amount, accepting decimal/scientific notation."""
    if value is None:
        return None
    try:
        raw = Decimal(str(value).replace("_", ""))
    except InvalidOperation as exc:
        raise ValueError("invalid raw amount: %s" % value) from exc
    if raw <= 0 or raw != raw.to_integral_value():
        raise ValueError("raw amount must be a positive integer: %s" % value)
    return int(raw)


def amount_for_direction(
    zero_for_one: bool,
    *,
    amount0_in: int,
    amount1_in: int,
    legacy_amount_in_raw: Optional[int],
) -> int:
    """Select the exact-input cap for the swap's input token."""
    if legacy_amount_in_raw is not None:
        return legacy_amount_in_raw
    return amount0_in if zero_for_one else amount1_in


class RuntimeStore:
    """Durable keeper state plus JSONL and Prometheus-compatible metrics."""

    def __init__(self, state_path: Path, metrics_path: Path, health_path: Path):
        self.state_path = state_path
        self.metrics_path = metrics_path
        self.health_path = health_path
        self.state = self._load_state()

    def _load_state(self) -> dict:
        if not self.state_path.exists():
            return {
                "schema_version": 1,
                "counters": {},
                "consecutive_errors": 0,
                "pending_transaction": None,
            }
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RuntimeError("invalid solver state file %s: %s" % (self.state_path, exc))
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            raise RuntimeError("unsupported solver state schema in %s" % self.state_path)
        payload.setdefault("counters", {})
        payload.setdefault("consecutive_errors", 0)
        payload.setdefault("pending_transaction", None)
        return payload

    def record(self, event: str, **fields) -> None:
        now = int(time.time())
        counters = self.state.setdefault("counters", {})
        counters[event] = int(counters.get(event, 0)) + 1
        self.state["last_event"] = event
        self.state["last_event_at"] = now
        if event == "tick_success":
            self.state["last_successful_tick_at"] = now
            self.state["consecutive_errors"] = 0
        elif event == "tick_error":
            self.state["last_error_at"] = now
            self.state["last_error"] = str(fields.get("error") or "unknown")[:500]
            self.state["consecutive_errors"] = int(
                self.state.get("consecutive_errors", 0)
            ) + 1
        elif event == "tx_submitted":
            self.state["pending_transaction"] = {
                "action": fields.get("action"),
                "nonce": fields.get("nonce"),
                "hashes": list(fields.get("hashes") or []),
                "submitted_at": now,
            }
        elif event == "tx_confirmed":
            self.state["pending_transaction"] = None
            self.state["last_confirmed_tx"] = fields.get("tx_hash")
            self.state["last_confirmed_tx_at"] = now
        elif event == "fill_evaluated":
            self.state["last_fill_economics"] = dict(fields)

        event_payload = {"timestamp": now, "event": event, **fields}
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with self.metrics_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event_payload, sort_keys=True) + "\n")
        self._write_state()
        self._write_health(now)
        self._write_prometheus(now)

    def health(self, *, max_age_seconds: int, max_consecutive_errors: int) -> dict:
        now = int(time.time())
        last_success = int(self.state.get("last_successful_tick_at") or 0)
        age = None if last_success == 0 else now - last_success
        errors = int(self.state.get("consecutive_errors") or 0)
        reasons = []
        if age is None:
            reasons.append("no_successful_tick")
        elif age > max_age_seconds:
            reasons.append("last_successful_tick_stale")
        if errors >= max_consecutive_errors:
            reasons.append("too_many_consecutive_errors")
        pending = self.state.get("pending_transaction")
        if isinstance(pending, dict) and now - int(pending.get("submitted_at") or now) > max_age_seconds:
            reasons.append("pending_transaction_stale")
        return {
            "healthy": not reasons,
            "checked_at": now,
            "last_successful_tick_age_seconds": age,
            "consecutive_errors": errors,
            "pending_transaction": pending,
            "reasons": reasons,
        }

    def _write_state(self) -> None:
        _atomic_write_json(self.state_path, self.state)

    def _write_health(self, now: int) -> None:
        payload = {
            "updated_at": now,
            "last_event": self.state.get("last_event"),
            "last_successful_tick_at": self.state.get("last_successful_tick_at"),
            "consecutive_errors": self.state.get("consecutive_errors", 0),
            "pending_transaction": self.state.get("pending_transaction"),
        }
        _atomic_write_json(self.health_path, payload)

    def _write_prometheus(self, now: int) -> None:
        path = self.metrics_path.with_suffix(".prom")
        lines = [
            "# TYPE lvr_solver_events_total counter",
            *[
                'lvr_solver_events_total{event="%s"} %d' % (name, value)
                for name, value in sorted(self.state.get("counters", {}).items())
            ],
            "# TYPE lvr_solver_consecutive_errors gauge",
            "lvr_solver_consecutive_errors %d"
            % int(self.state.get("consecutive_errors") or 0),
            "# TYPE lvr_solver_last_successful_tick_timestamp_seconds gauge",
            "lvr_solver_last_successful_tick_timestamp_seconds %d"
            % int(self.state.get("last_successful_tick_at") or 0),
            "# TYPE lvr_solver_metrics_written_timestamp_seconds gauge",
            "lvr_solver_metrics_written_timestamp_seconds %d" % now,
        ]
        last_fill = self.state.get("last_fill_economics")
        if isinstance(last_fill, dict):
            gauge_fields = (
                "gross_surplus_token1",
                "gas_cost_token1",
                "required_edge_token1",
                "minimum_profit_token1",
                "estimated_lp_fee_token1",
                "available_gap_value_token1",
                "required_compensation_token1",
                "minimum_compensation_wad",
                "current_solver_share_wad",
                "free_energy_gap_potential_wad",
                "net_profit_token1",
            )
            for field_name in gauge_fields:
                value = last_fill.get(field_name)
                if value is None:
                    continue
                metric_name = "lvr_solver_last_fill_%s" % field_name
                lines.extend(
                    [
                        "# TYPE %s gauge" % metric_name,
                        "%s %d" % (metric_name, int(value)),
                    ]
                )
            lines.extend(
                [
                    "# TYPE lvr_solver_last_fill_profitable gauge",
                    "lvr_solver_last_fill_profitable %d"
                    % (1 if last_fill.get("profitable") else 0),
                ]
            )
        _atomic_write_text(path, "\n".join(lines) + "\n")


class InstanceLock:
    """Advisory single-process lock preventing duplicate nonce managers."""

    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another solver process holds %s" % self.path) from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(str(os.getpid()))
        self.handle.flush()
        return self

    def __exit__(self, exc_type, exc, traceback):
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()


def _atomic_write_json(path: Path, payload: dict) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _atomic_write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(value)
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


# ---------------------------------------------------------------------------
# Chain access via cast
# ---------------------------------------------------------------------------

class Chain:
    """Failover-aware chain access and receipt-tracked transaction submission."""

    def __init__(
        self,
        rpc_url: str,
        private_key: Optional[str],
        keystore_account: Optional[str] = None,
        fallback_rpc_urls: Optional[List[str]] = None,
        sender_address: Optional[str] = None,
        runtime_store: Optional[RuntimeStore] = None,
        confirmations: int = 1,
        transaction_timeout_seconds: int = 60,
        replacement_attempts: int = 2,
        replacement_bump_bps: int = 1_250,
    ):
        self.rpc_urls = []
        for candidate in [rpc_url, *(fallback_rpc_urls or [])]:
            if candidate and candidate not in self.rpc_urls:
                self.rpc_urls.append(candidate)
        if not self.rpc_urls:
            raise ValueError("at least one RPC URL is required")
        self._rpc_index = 0
        self.private_key = private_key
        self.keystore_account = keystore_account
        self._sender_address = sender_address
        self.runtime_store = runtime_store
        self.confirmations = max(1, confirmations)
        self.transaction_timeout_seconds = max(5, transaction_timeout_seconds)
        self.replacement_attempts = max(0, replacement_attempts)
        self.replacement_bump_bps = max(100, replacement_bump_bps)
        self._warned_argv_key = False

    @property
    def rpc_url(self) -> str:
        return self.rpc_urls[self._rpc_index]

    @property
    def can_send(self) -> bool:
        return bool(self.keystore_account or self.private_key)

    def _signer_args(self) -> List[str]:
        """Return the signing argv for `cast`, preferring the keystore.

        With a keystore account nothing secret touches argv: cast decrypts the
        keystore itself, taking the password from the file named by ETH_PASSWORD.
        (`--interactive` is not usable here — cast prompts on /dev/tty, not
        stdin, so it cannot be fed headlessly.) A raw --private-key remains as an
        explicit opt-in for throwaway testnet keys and warns once, since argv is
        world-readable via `ps`.
        """
        if self.keystore_account:
            return ["--account", self.keystore_account]
        if not self._warned_argv_key:
            self._warned_argv_key = True
            print(
                "[warn] signing with --private-key: the key is visible in the "
                "process table (ps) to any local user. Use a keystore for any "
                "key holding real value:\n"
                "       cast wallet import keeper --interactive\n"
                "       printf '<pw>' > ~/.keeper.pass && chmod 600 ~/.keeper.pass\n"
                "       export KEYSTORE_ACCOUNT=keeper ETH_PASSWORD=~/.keeper.pass",
                file=sys.stderr,
            )
        return ["--private-key", self.private_key]

    def call(self, target: str, sig: str, *args: str) -> List[str]:
        return self._call(None, target, sig, *args)

    def call_from(self, sender: str, target: str, sig: str, *args: str) -> List[str]:
        return self._call(sender, target, sig, *args)

    def _call(self, sender: Optional[str], target: str, sig: str, *args: str) -> List[str]:
        errors = []
        for index, rpc_url in self._rpc_candidates():
            cmd = ["cast", "call", target, sig, *args]
            if sender:
                cmd += ["--from", sender]
            cmd += ["--rpc-url", rpc_url]
            out = subprocess.run(cmd, capture_output=True, text=True)
            if out.returncode == 0:
                self._rpc_index = index
                return parse_cast_values(out.stdout)
            errors.append(out.stderr.strip())
        raise CallReverted(errors[-1] if errors else "cast call failed")

    def estimate_gas(self, sender: str, target: str, sig: str, *args: str) -> int:
        errors = []
        for index, rpc_url in self._rpc_candidates():
            cmd = [
                "cast", "estimate", target, sig, *args,
                "--from", sender, "--rpc-url", rpc_url,
            ]
            out = subprocess.run(cmd, capture_output=True, text=True)
            if out.returncode == 0:
                self._rpc_index = index
                return _parse_int(out.stdout.strip().splitlines()[0])
            errors.append(out.stderr.strip())
        raise CallReverted(errors[-1] if errors else "gas estimation failed")

    def gas_price(self) -> int:
        return self._read_cast_int(["cast", "gas-price"])

    def sender_address(self) -> str:
        if self._sender_address:
            return self._sender_address
        if self.keystore_account:
            wallet_args = ["--account", self.keystore_account]
        elif self.private_key:
            # This follows the same explicit unsafe-testnet path as cast send:
            # the key reaches argv, and _signer_args emits the one-time warning.
            wallet_args = self._signer_args()
        else:
            raise RuntimeError("cannot derive sender without a configured signer")
        out = subprocess.run(
            ["cast", "wallet", "address", *wallet_args],
            capture_output=True,
            text=True,
        )
        if out.returncode != 0:
            raise RuntimeError("cannot derive signer address: %s" % out.stderr.strip())
        self._sender_address = out.stdout.strip().splitlines()[-1]
        return self._sender_address

    # Public RPCs are often load-balanced across nodes with inconsistent mempool
    # views, so back-to-back sends can transiently collide on nonce estimation.
    TRANSIENT_SEND_ERRORS = (
        "replacement transaction underpriced",
        "nonce too low",
        "already known",
    )

    def send(self, target: str, sig: str, *args: str, action: str = "transaction") -> str:
        if not self.can_send:
            raise RuntimeError(
                "no signer configured: set KEYSTORE_ACCOUNT (preferred, see "
                "`cast wallet import`) or PRIVATE_KEY/DEPLOYER_KEY"
            )
        sender = self.sender_address()
        nonce = self._read_cast_int(["cast", "nonce", sender, "--block", "pending"])
        gas_estimate = self.estimate_gas(sender, target, sig, *args)
        gas_limit = max(gas_estimate + 10_000, gas_estimate * 120 // 100)
        max_fee = max(1, self.gas_price() * 120 // 100)
        priority_fee = max(1, self._priority_fee())
        hashes: List[str] = []
        errors: List[str] = []

        for replacement in range(self.replacement_attempts + 1):
            if replacement:
                max_fee = max_fee * (10_000 + self.replacement_bump_bps) // 10_000
                priority_fee = (
                    priority_fee * (10_000 + self.replacement_bump_bps) // 10_000
                )
            tx_hash = self._submit_async(
                target,
                sig,
                args,
                nonce=nonce,
                gas_limit=gas_limit,
                max_fee=max_fee,
                priority_fee=priority_fee,
                errors=errors,
            )
            if tx_hash and tx_hash not in hashes:
                hashes.append(tx_hash)
                if self.runtime_store:
                    self.runtime_store.record(
                        "tx_submitted",
                        action=action,
                        nonce=nonce,
                        hashes=hashes,
                        replacement=replacement,
                    )
            if not hashes:
                continue
            receipt = self._wait_for_any_receipt(hashes)
            if receipt is not None:
                confirmed_hash, status = receipt
                if status != 1:
                    raise RuntimeError("transaction reverted: %s" % confirmed_hash)
                if self.runtime_store:
                    self.runtime_store.record(
                        "tx_confirmed",
                        action=action,
                        nonce=nonce,
                        tx_hash=confirmed_hash,
                    )
                return confirmed_hash

        raise RuntimeError(
            "transaction remains pending after replacements; nonce=%d hashes=%s errors=%s"
            % (nonce, ",".join(hashes), " | ".join(errors[-3:]))
        )

    def reconcile_pending(self) -> None:
        if not self.runtime_store:
            return
        pending = self.runtime_store.state.get("pending_transaction")
        if not isinstance(pending, dict):
            return
        hashes = [str(value) for value in pending.get("hashes") or []]
        receipt = self._find_receipt(hashes)
        if receipt is None:
            raise RuntimeError(
                "unresolved pending transaction in state file; inspect hashes before restart: %s"
                % ",".join(hashes)
            )
        tx_hash, status = receipt
        if status != 1:
            raise RuntimeError("persisted transaction reverted: %s" % tx_hash)
        self.runtime_store.record(
            "tx_confirmed",
            action=pending.get("action"),
            nonce=pending.get("nonce"),
            tx_hash=tx_hash,
            recovered=True,
        )

    def _submit_async(
        self,
        target: str,
        sig: str,
        args: Tuple[str, ...],
        *,
        nonce: int,
        gas_limit: int,
        max_fee: int,
        priority_fee: int,
        errors: List[str],
    ) -> Optional[str]:
        for index, rpc_url in self._rpc_candidates():
            cmd = [
                "cast", "send", target, sig, *args,
                "--rpc-url", rpc_url,
                *self._signer_args(),
                "--async",
                "--nonce", str(nonce),
                "--gas-limit", str(gas_limit),
                "--gas-price", str(max_fee),
                "--priority-gas-price", str(priority_fee),
            ]
            out = subprocess.run(cmd, capture_output=True, text=True)
            if out.returncode == 0:
                self._rpc_index = index
                return out.stdout.strip().splitlines()[-1]
            error = out.stderr.strip()
            errors.append(error)
        return None

    def _wait_for_any_receipt(self, hashes: List[str]) -> Optional[Tuple[str, int]]:
        deadline = time.monotonic() + self.transaction_timeout_seconds
        while time.monotonic() < deadline:
            receipt = self._find_receipt(hashes)
            if receipt is not None:
                return receipt
            time.sleep(2)
        return None

    def _find_receipt(self, hashes: List[str]) -> Optional[Tuple[str, int]]:
        for tx_hash in reversed(hashes):
            for index, rpc_url in self._rpc_candidates():
                out = subprocess.run(
                    [
                        "cast", "receipt", tx_hash,
                        "--rpc-url", rpc_url,
                        "--confirmations", str(self.confirmations),
                        "--async", "--json",
                    ],
                    capture_output=True,
                    text=True,
                )
                if out.returncode != 0:
                    continue
                try:
                    receipt = json.loads(out.stdout)
                    status = _parse_int(receipt.get("status", 0))
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                self._rpc_index = index
                return tx_hash, status
        return None

    def _priority_fee(self) -> int:
        try:
            return self._read_cast_int(["cast", "rpc", "eth_maxPriorityFeePerGas"])
        except RuntimeError:
            return max(1, self.gas_price() // 100)

    def _read_cast_int(self, base_cmd: List[str]) -> int:
        errors = []
        for index, rpc_url in self._rpc_candidates():
            out = subprocess.run(
                [*base_cmd, "--rpc-url", rpc_url],
                capture_output=True,
                text=True,
            )
            if out.returncode == 0:
                self._rpc_index = index
                return _parse_int(out.stdout.strip().splitlines()[-1])
            errors.append(out.stderr.strip())
        raise RuntimeError(errors[-1] if errors else "cast read failed")

    def _rpc_candidates(self):
        for offset in range(len(self.rpc_urls)):
            index = (self._rpc_index + offset) % len(self.rpc_urls)
            yield index, self.rpc_urls[index]


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
        self.caller = cfg.solver_address or os.environ.get("DEPLOYER_ADDRESS")
        self.amount0_in = parse_units(cfg.amount0_in, cfg.token0_decimals)
        self.amount1_in = parse_units(cfg.amount1_in, cfg.token1_decimals)
        self.legacy_amount_in_raw = parse_raw_units(cfg.amount_in_raw)
        self.token1_decimals = cfg.token1_decimals
        self.solver_edge_bps = cfg.solver_edge_bps
        self.minimum_profit_token1 = parse_nonnegative_units(
            cfg.min_profit_token1, cfg.token1_decimals
        )
        self.native_token_price_token1_wad = parse_rate_wad(
            cfg.native_token_price_token1 or "1"
        )
        self.gas_limit_buffer_bps = cfg.gas_limit_buffer_bps
        self.max_gas_price_wei = int(Decimal(str(cfg.max_gas_price_gwei)) * 10**9)
        self.max_concession_wad = int(Decimal(str(cfg.max_concession_wad)))
        self.dry_run = bool(cfg.dry_run)
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
            self.hook,
            "pokeAuction(%s)" % POOL_KEY_ABI,
            self.key_tuple,
            action="poke",
        )

    def fill(self, amount_in: Optional[int] = None, baseline: bool = False) -> str:
        premium, above, _, ref, _ = self.read_gap()
        key_tuple = self.key_tuple
        if baseline:
            key_tuple = self.baseline_key_tuple
            premium, above = gap_premium_wad(ref, self.pool_sqrtp(key_tuple))
        if premium == 0:
            raise RuntimeError("no gap to reprice")
        zero_for_one = toxic_zero_for_one(above)
        if amount_in is None:
            amount_in = amount_for_direction(
                zero_for_one,
                amount0_in=self.amount0_in,
                amount1_in=self.amount1_in,
                legacy_amount_in_raw=self.legacy_amount_in_raw,
            )
        swap_params = "(%s,-%d,%d)" % (str(zero_for_one).lower(), amount_in, ref)
        return self.chain.send(
            self.swap_router,
            SWAP_ABI,
            key_tuple,
            swap_params,
            "(false,false)",
            "0x",
            action="baseline_fill" if baseline else "fill",
        )

    def estimate_fill_economics(self, amount_in: Optional[int] = None) -> FillEconomics:
        """Simulate the exact router call and apply gas/edge profitability gates."""
        premium, above, toxic_fee, ref, _ = self.read_gap()
        if premium == 0:
            raise RuntimeError("no gap to reprice")
        if toxic_fee is None:
            raise RuntimeError("toxic fill remains fee-deterred")
        zero_for_one = toxic_zero_for_one(above)
        if amount_in is None:
            amount_in = amount_for_direction(
                zero_for_one,
                amount0_in=self.amount0_in,
                amount1_in=self.amount1_in,
                legacy_amount_in_raw=self.legacy_amount_in_raw,
            )
        swap_params = "(%s,-%d,%d)" % (str(zero_for_one).lower(), amount_in, ref)
        sender = self.chain.sender_address()
        call_args = (self.key_tuple, swap_params, "(false,false)", "0x")
        values = self.chain.call_from(sender, self.swap_router, SWAP_ABI, *call_args)
        if not values:
            raise RuntimeError("fill preflight returned no BalanceDelta")
        amount0, amount1 = decode_balance_delta(int(values[0]))
        gross_surplus = position_value_token1(amount0, amount1, ref)
        if gross_surplus < 0:
            raise RuntimeError("simulated fill has negative gross surplus")
        input_notional = input_notional_token1_from_delta(
            amount0=amount0,
            amount1=amount1,
            zero_for_one=zero_for_one,
            reference_sqrt_price_x96=ref,
        )
        estimated_lp_fee = estimated_input_fee_token1(input_notional, toxic_fee)
        gas_units = self.chain.estimate_gas(
            sender, self.swap_router, SWAP_ABI, *call_args
        )
        gas_units = gas_units * (10_000 + self.gas_limit_buffer_bps) // 10_000
        gas_price_wei = self.chain.gas_price()
        gas_cost_token1 = gas_cost_in_token1_raw(
            gas_units,
            gas_price_wei,
            self.native_token_price_token1_wad,
            self.token1_decimals,
        )
        edge_token1 = input_notional * self.solver_edge_bps // 10_000
        economics = evaluate_fill_economics(
            gross_surplus_token1=gross_surplus,
            gas_cost_token1=gas_cost_token1,
            required_edge_token1=edge_token1,
            minimum_profit_token1=self.minimum_profit_token1,
            estimated_lp_fee_token1=estimated_lp_fee,
            free_energy_gap_potential_wad=free_energy_gap_potential_wad(premium),
        )
        if gas_price_wei <= self.max_gas_price_wei:
            return economics
        return replace(
            economics,
            profitable=False,
            reason="gas_price_above_cap",
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
                feed,
                "setRoundData(int256,uint256)",
                str(answer),
                str(now),
                action="refresh_oracle",
            )

    def make_gap(self, gap_bps: float) -> str:
        if not self.base_feed:
            raise RuntimeError("make-gap needs --base-feed (demo pools only)")
        answer = self.feed_answer(self.base_feed)
        target = feed_answer_for_gap(answer, gap_bps)
        now = int(time.time())
        tx = self.chain.send(
            self.base_feed,
            "setRoundData(int256,uint256)",
            str(target),
            str(now),
            action="make_gap",
        )
        if self.quote_feed:
            quote = self.feed_answer(self.quote_feed)
            self.chain.send(
                self.quote_feed,
                "setRoundData(int256,uint256)",
                str(quote),
                str(now),
                action="make_gap_quote_refresh",
            )
        return tx

    # -- loop ---------------------------------------------------------------

    def tick(self, min_concession_wad: int, amount_in: Optional[int], keep_fresh: bool) -> str:
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
            if self.dry_run:
                log("dry-run: would poke auction clock")
                return "would_poke"
            tx = self.poke()
            log("poked auction clock: %s" % tx)
            return "poked"
        if should_fill(eligible, start_ts, concession, min_concession_wad, fee_deterred):
            if concession > self.max_concession_wad:
                log(
                    "keeper reserve blocks fill: concession %.4f%% exceeds cap %.4f%%"
                    % (concession / WAD * 100, self.max_concession_wad / WAD * 100)
                )
                return "concession_capped"
            economics = self.estimate_fill_economics(amount_in)
            log(
                "fill economics (token1 raw): gap_value=%d lp_fee=%d solver=%d "
                "required=%d min_share=%s current_share=%s gas=%d edge=%d min=%d net=%d [%s]"
                % (
                    economics.available_gap_value_token1,
                    economics.estimated_lp_fee_token1,
                    economics.gross_surplus_token1,
                    economics.required_compensation_token1,
                    _format_optional_wad_pct(economics.minimum_compensation_wad),
                    _format_optional_wad_pct(economics.current_solver_share_wad),
                    economics.gas_cost_token1,
                    economics.required_edge_token1,
                    economics.minimum_profit_token1,
                    economics.net_profit_token1,
                    economics.reason,
                )
            )
            if self.chain.runtime_store:
                self.chain.runtime_store.record("fill_evaluated", **asdict(economics))
            if not economics.profitable:
                return "unprofitable"
            if self.dry_run:
                log("dry-run: profitable fill would be submitted")
                return "would_fill"
            tx = self.fill(amount_in)
            log("filled repricing swap: %s" % tx)
            return "filled"
        return "waited"


def log(msg: str) -> None:
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def _format_optional_wad_pct(value: Optional[int]) -> str:
    if value is None:
        return "unmeasurable"
    return "%.6f%%" % (value / WAD * 100)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rpc-url", default=os.environ.get("RPC_URL")
                   or os.environ.get("BASE_SEPOLIA_RPC_URL"))
    p.add_argument(
        "--rpc-fallback-url",
        action="append",
        default=[
            value.strip()
            for value in os.environ.get("RPC_FALLBACK_URLS", "").split(",")
            if value.strip()
        ],
        help="fallback RPC endpoint; repeat for more than one",
    )
    p.add_argument("--private-key", default=os.environ.get("PRIVATE_KEY")
                   or os.environ.get("DEPLOYER_KEY"),
                   help="unsafe raw testnet key; visible in process argv. "
                        "Production run mode rejects it unless explicitly opted in.")
    p.add_argument("--keystore-account",
                   default=os.environ.get("KEYSTORE_ACCOUNT")
                   or os.environ.get("ETH_KEYSTORE_ACCOUNT"),
                   help="encrypted keystore account in ~/.foundry/keystores "
                        "(create with `cast wallet import <name> --interactive`); "
                        "set ETH_PASSWORD to the path of its password file")
    p.add_argument("--solver-address", default=os.environ.get("SOLVER_ADDRESS"))
    p.add_argument("--allow-unsafe-raw-key", action="store_true")
    p.add_argument("--hook", default=os.environ.get("HOOK"))
    p.add_argument("--token0", default=os.environ.get("TOKEN0"))
    p.add_argument("--token1", default=os.environ.get("TOKEN1"))
    p.add_argument("--swap-router", default=os.environ.get("SWAP_ROUTER"))
    p.add_argument("--base-feed", default=os.environ.get("BASE_FEED"))
    p.add_argument("--quote-feed", default=os.environ.get("QUOTE_FEED"))
    p.add_argument("--token0-decimals", type=int,
                   default=int(os.environ.get("TOKEN0_DECIMALS", "18")))
    p.add_argument("--token1-decimals", type=int,
                   default=int(os.environ.get("TOKEN1_DECIMALS", "18")))
    p.add_argument("--amount0-in", default=os.environ.get("AMOUNT0_IN", "100"),
                   help="token0 exact-input cap in human units")
    p.add_argument("--amount1-in", default=os.environ.get("AMOUNT1_IN", "100"),
                   help="token1 exact-input cap in human units")
    p.add_argument("--amount-in-raw", default=os.environ.get("AMOUNT_IN_RAW"),
                   help="legacy raw exact-input cap for both directions")
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
    p.add_argument(
        "--solver-edge-bps",
        type=int,
        default=int(os.environ.get("SOLVER_EDGE_BPS", "5")),
        help="minimum solver edge as bps of simulated input notional",
    )
    p.add_argument(
        "--min-profit-token1",
        default=os.environ.get("MIN_PROFIT_TOKEN1", "0"),
        help="additional minimum net profit in human token1 units",
    )
    p.add_argument(
        "--native-token-price-token1",
        default=os.environ.get("NATIVE_TOKEN_PRICE_TOKEN1"),
        help="whole token1 units per native gas token; required in run mode",
    )
    p.add_argument(
        "--max-gas-price-gwei",
        default=os.environ.get("MAX_GAS_PRICE_GWEI", "5"),
    )
    p.add_argument(
        "--gas-limit-buffer-bps",
        type=int,
        default=int(os.environ.get("GAS_LIMIT_BUFFER_BPS", "2000")),
    )
    p.add_argument(
        "--max-concession-wad",
        default=os.environ.get("KEEPER_MAX_CONCESSION_WAD", str(WAD)),
        help="first-party LP-reserve cap; refuse fills above this concession",
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--state-path",
        default=os.environ.get("SOLVER_STATE_PATH", ".tmp/solver/state.json"),
    )
    p.add_argument(
        "--metrics-path",
        default=os.environ.get("SOLVER_METRICS_PATH", ".tmp/solver/metrics.jsonl"),
    )
    p.add_argument(
        "--health-path",
        default=os.environ.get("SOLVER_HEALTH_PATH", ".tmp/solver/health.json"),
    )
    p.add_argument(
        "--confirmations",
        type=int,
        default=int(os.environ.get("SOLVER_CONFIRMATIONS", "1")),
    )
    p.add_argument(
        "--transaction-timeout",
        type=int,
        default=int(os.environ.get("SOLVER_TX_TIMEOUT", "60")),
    )
    p.add_argument(
        "--replacement-attempts",
        type=int,
        default=int(os.environ.get("SOLVER_REPLACEMENT_ATTEMPTS", "2")),
    )
    p.add_argument(
        "--replacement-bump-bps",
        type=int,
        default=int(os.environ.get("SOLVER_REPLACEMENT_BUMP_BPS", "1250")),
    )

    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    sub.add_parser("poke")
    sub.add_parser("refresh-oracle")
    sub.add_parser("compare")
    health = sub.add_parser("health")
    health.add_argument("--max-age-seconds", type=int, default=120)
    health.add_argument("--max-consecutive-errors", type=int, default=5)

    fill = sub.add_parser("fill")
    fill.add_argument("--amount-in", default=None,
                      help="legacy raw exact-in size; prefer --amount0-in/--amount1-in")
    fill.add_argument("--pool", choices=("hooked", "baseline"), default="hooked")

    gap = sub.add_parser("make-gap")
    gap.add_argument("--bps", type=float, required=True,
                     help="price gap in bps; negative moves the reference down")

    run = sub.add_parser("run")
    run.add_argument("--interval", type=float, default=10.0)
    run.add_argument("--max-iterations", type=int, default=0,
                     help="stop after N ticks (0 = forever)")
    run.add_argument("--min-concession-wad", default=str(10**15))
    run.add_argument("--amount-in", default=None,
                     help="legacy raw exact-in size; prefer --amount0-in/--amount1-in")
    run.add_argument("--keep-fresh", action="store_true",
                     help="re-stamp demo feeds every tick so they never go stale")
    run.add_argument("--max-consecutive-errors", type=int, default=5)
    run.add_argument("--max-error-backoff", type=float, default=120.0)
    return p


def main() -> int:
    args = build_parser().parse_args()
    store = RuntimeStore(
        Path(args.state_path), Path(args.metrics_path), Path(args.health_path)
    )
    if args.command == "health":
        health = store.health(
            max_age_seconds=args.max_age_seconds,
            max_consecutive_errors=args.max_consecutive_errors,
        )
        print(json.dumps(health, indent=2, sort_keys=True))
        return 0 if health["healthy"] else 1

    if getattr(args, "amount_in", None) is not None:
        args.amount_in_raw = args.amount_in
    for required in ("rpc_url", "hook", "token0", "token1"):
        if not getattr(args, required):
            print("missing --%s (or its environment variable)" % required.replace("_", "-"))
            return 2

    if args.command in {"fill", "run"} and not args.swap_router:
        print("missing --swap-router (or SWAP_ROUTER)")
        return 2
    if args.command == "compare" and not args.liquidity_router:
        print("missing --liquidity-router (or LIQUIDITY_ROUTER)")
        return 2
    if args.command in {"poke", "fill", "make-gap", "refresh-oracle"} and not (
        args.keystore_account or args.private_key
    ):
        print("command needs --keystore-account (preferred) or a testnet signer")
        return 2
    if args.command == "make-gap" and not args.base_feed:
        print("make-gap needs --base-feed (demo pools only)")
        return 2
    if args.command == "refresh-oracle" and not (args.base_feed or args.quote_feed):
        print("refresh-oracle needs --base-feed or --quote-feed (demo pools only)")
        return 2
    if args.command == "run":
        if not args.native_token_price_token1:
            print(
                "missing --native-token-price-token1: profitability cannot convert gas "
                "into token1 units"
            )
            return 2
        if args.private_key and not args.keystore_account and not args.allow_unsafe_raw_key:
            print(
                "run mode refuses a raw private key because cast exposes it in process argv; "
                "use --keystore-account or explicitly pass --allow-unsafe-raw-key for testnet"
            )
            return 2
        if not args.dry_run and not (args.keystore_account or args.private_key):
            print("run mode needs --keystore-account (preferred) or a testnet signer")
            return 2
        if args.dry_run and not (args.solver_address or args.keystore_account):
            print("dry-run mode needs --solver-address or --keystore-account for eth_call")
            return 2
    try:
        _validate_runtime_config(args)
    except ValueError as exc:
        print("invalid configuration: %s" % exc)
        return 2

    bot = SolverBot(
        Chain(
            args.rpc_url,
            args.private_key,
            args.keystore_account,
            fallback_rpc_urls=args.rpc_fallback_url,
            sender_address=args.solver_address,
            runtime_store=store if args.command == "run" else None,
            confirmations=args.confirmations,
            transaction_timeout_seconds=args.transaction_timeout,
            replacement_attempts=args.replacement_attempts,
            replacement_bump_bps=args.replacement_bump_bps,
        ),
        args,
    )

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
        log("fill tx: %s" % bot.fill(None, args.pool == "baseline"))
    elif args.command == "compare":
        bot.compare()
    elif args.command == "make-gap":
        log("make-gap tx: %s" % bot.make_gap(args.bps))
    elif args.command == "run":
        lock_path = Path(args.state_path).with_suffix(".lock")
        try:
            with InstanceLock(lock_path):
                bot.chain.reconcile_pending()
                return _run_loop(bot, args, store)
        except RuntimeError as exc:
            store.record("tick_error", error=str(exc), phase="startup")
            log("startup failed: %s" % exc)
            return 1
    return 0


def _run_loop(bot: SolverBot, args: argparse.Namespace, store: RuntimeStore) -> int:
    iteration = 0
    while True:
        sleep_seconds = args.interval
        try:
            action = bot.tick(int(args.min_concession_wad), None, args.keep_fresh)
            store.record("tick_success", action=action, iteration=iteration)
        except KeyboardInterrupt:
            store.record("shutdown", reason="keyboard_interrupt")
            return 0
        except (
            CallReverted,
            RuntimeError,
            OSError,
            ValueError,
            subprocess.SubprocessError,
        ) as exc:
            store.record("tick_error", error=str(exc), iteration=iteration)
            errors = int(store.state.get("consecutive_errors") or 0)
            log("tick failed (%d/%d): %s" % (errors, args.max_consecutive_errors, exc))
            if errors >= args.max_consecutive_errors:
                log("stopping after consecutive-error budget was exhausted")
                return 1
            sleep_seconds = min(
                args.max_error_backoff,
                max(2.0, args.interval) * (2 ** max(0, errors - 1)),
            )
        iteration += 1
        if args.max_iterations and iteration >= args.max_iterations:
            return 0
        try:
            time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            store.record("shutdown", reason="keyboard_interrupt")
            return 0


def _validate_runtime_config(args: argparse.Namespace) -> None:
    if not 0 <= args.token0_decimals <= 36 or not 0 <= args.token1_decimals <= 36:
        raise ValueError("token decimals must be between 0 and 36")
    if not 0 <= args.solver_edge_bps <= 10_000:
        raise ValueError("solver-edge-bps must be between 0 and 10000")
    if not 0 <= args.gas_limit_buffer_bps <= 50_000:
        raise ValueError("gas-limit-buffer-bps must be between 0 and 50000")
    max_concession = _configuration_decimal(
        args.max_concession_wad, "max-concession-wad"
    )
    if (
        max_concession < 0
        or max_concession > WAD
        or max_concession != max_concession.to_integral_value()
    ):
        raise ValueError("max-concession-wad must be an integer between 0 and 1e18")
    max_gas_price = _configuration_decimal(
        args.max_gas_price_gwei, "max-gas-price-gwei"
    )
    if max_gas_price <= 0:
        raise ValueError("max-gas-price-gwei must be positive")
    max_gas_price_wei = max_gas_price * 10**9
    if max_gas_price_wei != max_gas_price_wei.to_integral_value():
        raise ValueError("max-gas-price-gwei has more than 9 decimal places")
    if args.confirmations < 1:
        raise ValueError("confirmations must be at least 1")
    if args.transaction_timeout < 5:
        raise ValueError("transaction-timeout must be at least 5 seconds")
    if args.replacement_attempts < 0:
        raise ValueError("replacement-attempts must be non-negative")
    if not 100 <= args.replacement_bump_bps <= 100_000:
        raise ValueError("replacement-bump-bps must be between 100 and 100000")
    parse_units(args.amount0_in, args.token0_decimals)
    parse_units(args.amount1_in, args.token1_decimals)
    parse_raw_units(args.amount_in_raw)
    parse_nonnegative_units(args.min_profit_token1, args.token1_decimals)
    if args.native_token_price_token1:
        parse_rate_wad(args.native_token_price_token1)
    if args.command == "run":
        min_concession = _configuration_decimal(
            args.min_concession_wad, "min-concession-wad"
        )
        if (
            min_concession < 0
            or min_concession > WAD
            or min_concession != min_concession.to_integral_value()
        ):
            raise ValueError(
                "min-concession-wad must be an integer between 0 and 1e18"
            )
        if min_concession > max_concession:
            raise ValueError("min-concession-wad cannot exceed max-concession-wad")
        args.min_concession_wad = int(min_concession)
        if args.interval <= 0:
            raise ValueError("interval must be positive")
        if args.max_iterations < 0:
            raise ValueError("max-iterations must be non-negative")
        if args.max_consecutive_errors < 1:
            raise ValueError("max-consecutive-errors must be at least 1")
        if args.max_error_backoff <= 0:
            raise ValueError("max-error-backoff must be positive")


def _configuration_decimal(value, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value).replace("_", ""))
    except InvalidOperation as exc:
        raise ValueError(f"{label} is not a valid number") from exc
    if not parsed.is_finite():
        raise ValueError(f"{label} must be finite")
    return parsed


if __name__ == "__main__":
    sys.exit(main())
