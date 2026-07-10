import unittest

from script.solver_bot import (
    HALF_BPS_WAD,
    WAD,
    feed_answer_for_gap,
    gap_premium_wad,
    gap_trigger_bps,
    parse_cast_values,
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
    def test_pokes_only_unstarted_eligible_auctions(self):
        self.assertTrue(should_poke(True, 0))
        self.assertFalse(should_poke(True, 123))
        self.assertFalse(should_poke(False, 0))

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


class CastParsingTest(unittest.TestCase):
    def test_strips_display_suffixes_and_blank_lines(self):
        out = "true\n1000000 [1e6]\n\n79228162514264337593543950336 [7.922e28]\n"
        self.assertEqual(
            parse_cast_values(out),
            ["true", "1000000", "79228162514264337593543950336"],
        )


if __name__ == "__main__":
    unittest.main()
