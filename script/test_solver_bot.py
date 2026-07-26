import unittest

from script.solver_bot import (
    HALF_BPS_WAD,
    WAD,
    Chain,
    decode_balance_delta,
    feed_answer_for_gap,
    gap_premium_wad,
    gap_trigger_bps,
    parse_cast_values,
    parse_raw_units,
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


if __name__ == "__main__":
    unittest.main()
