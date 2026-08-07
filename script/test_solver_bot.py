import unittest
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from script.solver_bot import (
    WAD,
    Chain,
    RuntimeStore,
    _validate_runtime_config,
    amount_for_direction,
    decode_balance_delta,
    evaluate_fill_economics,
    feed_answer_for_gap,
    gas_cost_in_token1_raw,
    gap_premium_wad,
    gap_trigger_bps,
    parse_cast_values,
    parse_nonnegative_units,
    parse_raw_units,
    parse_rate_wad,
    parse_units,
    position_value_token1,
    should_fill,
    should_poke,
    toxic_zero_for_one,
)

Q96 = 2**96


class GapMathTest(unittest.TestCase):
    def test_gap_premium_matches_hook_convention(self):
        # Sqrt ratio 1.001 (~20 bps price gap) with exactly divisible fixtures.
        pool = 10**18
        ref = 10**18 + 10**15
        premium, above = gap_premium_wad(ref, pool)
        self.assertTrue(above)
        self.assertEqual(premium, 10**15)
        self.assertAlmostEqual(gap_trigger_bps(premium), 20.0, places=6)

        premium_down, above_down = gap_premium_wad(pool, ref)
        self.assertFalse(above_down)
        # Reciprocal direction floors slightly differently but stays within a wei.
        self.assertAlmostEqual(premium_down / WAD, premium / WAD, places=15)

    def test_zero_gap_has_no_premium(self):
        premium, above = gap_premium_wad(Q96, Q96)
        self.assertEqual(premium, 0)
        self.assertFalse(above)

    def test_toxic_direction_mirrors_hook(self):
        # Oracle above pool: token0 undervalued, repricer sells token1.
        self.assertFalse(toxic_zero_for_one(True))
        self.assertTrue(toxic_zero_for_one(False))


class DecisionTest(unittest.TestCase):
    def test_pokes_when_auction_state_needs_sync(self):
        self.assertTrue(should_poke(True, 0))
        self.assertFalse(should_poke(True, 123))
        self.assertFalse(should_poke(False, 0))
        self.assertTrue(should_poke(False, 123))

    def test_fill_requires_open_clock_and_threshold(self):
        self.assertTrue(should_fill(True, 100, 2 * 10**15, 10**15, False))
        self.assertFalse(should_fill(True, 100, 5 * 10**14, 10**15, False))
        self.assertFalse(should_fill(True, 0, 2 * 10**15, 10**15, False))
        self.assertFalse(should_fill(False, 100, 2 * 10**15, 10**15, False))

    def test_fill_waits_while_fee_is_deterred_above_max_fee(self):
        self.assertFalse(should_fill(True, 100, 2 * 10**15, 10**15, True))


class FeedMathTest(unittest.TestCase):
    def test_feed_answer_moves_by_price_bps(self):
        self.assertEqual(feed_answer_for_gap(10**18, 30), 10**18 * 10_030 // 10_000)
        self.assertEqual(feed_answer_for_gap(10**18, -30), 10**18 * 9_970 // 10_000)
        self.assertEqual(feed_answer_for_gap(10**18, 0), 10**18)


class BalanceDeltaTest(unittest.TestCase):
    def test_decodes_positive_amounts(self):
        packed = (5 << 128) | 7
        self.assertEqual(decode_balance_delta(packed), (5, 7))

    def test_decodes_negative_amounts(self):
        a0, a1 = -3, -11
        packed = ((a0 & ((1 << 128) - 1)) << 128) | (a1 & ((1 << 128) - 1))
        # Interpret the 256-bit two's complement as a signed int like cast does.
        signed = packed - (1 << 256) if packed >= (1 << 255) else packed
        self.assertEqual(decode_balance_delta(signed), (-3, -11))

    def test_decodes_mixed_signs(self):
        a0, a1 = 42, -1
        packed = ((a0 & ((1 << 128) - 1)) << 128) | (a1 & ((1 << 128) - 1))
        signed = packed - (1 << 256) if packed >= (1 << 255) else packed
        self.assertEqual(decode_balance_delta(signed), (42, -1))


class PositionValueTest(unittest.TestCase):
    def test_values_token0_at_reference_price(self):
        # P_ref = 4 (sqrtP = 2 * Q96): 10 token0 = 40 token1, plus 5 token1.
        self.assertEqual(position_value_token1(10, 5, 2 * Q96), 45)

    def test_one_to_one_price_sums_amounts(self):
        self.assertEqual(position_value_token1(7, 8, Q96), 15)


class CastParsingTest(unittest.TestCase):
    def test_strips_display_suffixes_and_blank_lines(self):
        out = "true\n1000000 [1e6]\n\n79228162514264337593543950336 [7.922e28]\n"
        self.assertEqual(
            parse_cast_values(out),
            ["true", "1000000", "79228162514264337593543950336"],
        )


class AmountParsingTest(unittest.TestCase):
    def test_parse_units_handles_token_decimals(self):
        self.assertEqual(parse_units("10", 6), 10_000_000)
        self.assertEqual(parse_units("0.003", 18), 3_000_000_000_000_000)
        self.assertEqual(parse_units("1_000.25", 6), 1_000_250_000)

    def test_parse_units_rejects_excess_precision(self):
        with self.assertRaises(ValueError):
            parse_units("0.0000001", 6)

    def test_parse_raw_units_accepts_scientific_integer_notation(self):
        self.assertEqual(parse_raw_units("100e18"), 100 * 10**18)
        self.assertEqual(parse_raw_units(None), None)

    def test_nonnegative_units_allow_zero(self):
        self.assertEqual(parse_nonnegative_units("0", 6), 0)
        with self.assertRaises(ValueError):
            parse_nonnegative_units("-0.1", 6)

    def test_direction_uses_the_input_token_or_legacy_override(self):
        values = {"amount0_in": 10, "amount1_in": 20, "legacy_amount_in_raw": None}
        self.assertEqual(amount_for_direction(True, **values), 10)
        self.assertEqual(amount_for_direction(False, **values), 20)
        values["legacy_amount_in_raw"] = 99
        self.assertEqual(amount_for_direction(False, **values), 99)


class FillEconomicsTest(unittest.TestCase):
    def test_profit_gate_includes_gas_edge_and_minimum_profit(self):
        economics = evaluate_fill_economics(
            gross_surplus_token1=130,
            gas_cost_token1=20,
            required_edge_token1=5,
            minimum_profit_token1=105,
        )
        self.assertTrue(economics.profitable)
        self.assertEqual(economics.net_profit_token1, 110)

        below = evaluate_fill_economics(
            gross_surplus_token1=129,
            gas_cost_token1=20,
            required_edge_token1=5,
            minimum_profit_token1=105,
        )
        self.assertFalse(below.profitable)
        self.assertEqual(below.reason, "below_profit_reserve")

    def test_native_gas_cost_converts_to_token1_raw_units(self):
        # 200k gas at 1 gwei = 0.0002 native token, at 1 token1/native.
        self.assertEqual(
            gas_cost_in_token1_raw(200_000, 10**9, WAD, 18),
            2 * 10**14,
        )
        self.assertEqual(parse_rate_wad("2500.5"), 2_500_500_000_000_000_000_000)


class RuntimeStoreTest(unittest.TestCase):
    def test_state_metrics_and_health_survive_restart(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state.json"
            metrics = root / "metrics.jsonl"
            health = root / "health.json"
            store = RuntimeStore(state, metrics, health)
            store.record("tick_success", action="waited")

            restored = RuntimeStore(state, metrics, health)
            self.assertEqual(restored.state["counters"]["tick_success"], 1)
            self.assertTrue(
                restored.health(max_age_seconds=60, max_consecutive_errors=5)[
                    "healthy"
                ]
            )
            self.assertTrue(metrics.exists())
            self.assertTrue(metrics.with_suffix(".prom").exists())
            self.assertTrue(health.exists())

    def test_confirmed_send_persists_then_clears_pending_transaction(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            store = RuntimeStore(
                root / "state.json", root / "metrics.jsonl", root / "health.json"
            )

            class ConfirmingChain(Chain):
                def sender_address(self):
                    return "0x0000000000000000000000000000000000000001"

                def _read_cast_int(self, _):
                    return 7

                def estimate_gas(self, *_):
                    return 100_000

                def gas_price(self):
                    return 10**9

                def _priority_fee(self):
                    return 10**8

                def _submit_async(self, *_, **__):
                    return "0xabc"

                def _wait_for_any_receipt(self, _):
                    self.assert_pending()
                    return "0xabc", 1

                def assert_pending(self):
                    if store.state["pending_transaction"]["hashes"] != ["0xabc"]:
                        raise AssertionError("transaction was not persisted before polling")

            chain = ConfirmingChain(
                "http://rpc", None, "keeper", runtime_store=store
            )
            self.assertEqual(chain.send("0x2", "f()", action="fill"), "0xabc")
            self.assertIsNone(store.state["pending_transaction"])
            self.assertEqual(store.state["counters"]["tx_submitted"], 1)
            self.assertEqual(store.state["counters"]["tx_confirmed"], 1)


class SignerSecrecyTest(unittest.TestCase):
    """A signing key in argv is readable by any local user via `ps`."""

    KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

    def test_keystore_account_keeps_every_secret_off_argv(self):
        # The keystore path is the one safe for keys holding real value: cast
        # decrypts it itself, reading the password from ETH_PASSWORD.
        argv = Chain("http://rpc", self.KEY, "keeper")._signer_args()
        self.assertEqual(argv, ["--account", "keeper"])
        self.assertNotIn(self.KEY, argv)

    def test_keystore_wins_when_both_are_configured(self):
        chain = Chain("http://rpc", self.KEY, "keeper")
        self.assertNotIn(self.KEY, chain._signer_args())

    def test_send_requires_some_signer(self):
        self.assertFalse(Chain("http://rpc", None).can_send)
        self.assertTrue(Chain("http://rpc", None, "keeper").can_send)
        self.assertTrue(Chain("http://rpc", self.KEY).can_send)

    @patch("script.solver_bot.subprocess.run")
    def test_explicit_unsafe_raw_key_can_derive_sender(self, run):
        run.return_value.returncode = 0
        run.return_value.stdout = "0x0000000000000000000000000000000000000001\n"
        run.return_value.stderr = ""
        chain = Chain("http://rpc", self.KEY)

        with patch("script.solver_bot.sys.stderr"):
            self.assertEqual(
                chain.sender_address(),
                "0x0000000000000000000000000000000000000001",
            )
        self.assertIn(self.KEY, run.call_args.args[0])


class RuntimeConfigTest(unittest.TestCase):
    @staticmethod
    def args(**overrides):
        values = {
            "token0_decimals": 18,
            "token1_decimals": 6,
            "solver_edge_bps": 5,
            "gas_limit_buffer_bps": 2_000,
            "max_concession_wad": str(WAD),
            "max_gas_price_gwei": "5",
            "confirmations": 1,
            "transaction_timeout": 60,
            "replacement_attempts": 2,
            "replacement_bump_bps": 1_250,
            "amount0_in": "1",
            "amount1_in": "1",
            "amount_in_raw": None,
            "min_profit_token1": "0",
            "native_token_price_token1": "2500",
            "command": "run",
            "min_concession_wad": "1e15",
            "interval": 10.0,
            "max_iterations": 0,
            "max_consecutive_errors": 5,
            "max_error_backoff": 120.0,
        }
        values.update(overrides)
        return Namespace(**values)

    def test_normalizes_valid_minimum_concession(self):
        args = self.args()
        _validate_runtime_config(args)
        self.assertEqual(args.min_concession_wad, 10**15)

    def test_rejects_unreachable_concession_and_invalid_loop_limits(self):
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            _validate_runtime_config(
                self.args(min_concession_wad="2e15", max_concession_wad="1e15")
            )
        with self.assertRaisesRegex(ValueError, "interval"):
            _validate_runtime_config(self.args(interval=0))
        with self.assertRaisesRegex(ValueError, "token decimals"):
            _validate_runtime_config(self.args(token1_decimals=37))


if __name__ == "__main__":
    unittest.main()
