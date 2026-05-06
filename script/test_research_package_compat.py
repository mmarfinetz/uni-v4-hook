import unittest
import json
from pathlib import Path

import research.lvr.backtest.run_dutch_auction_backtest as new_backtest
import research.lvr.core.flow_classification as new_flow_classification
import research.lvr.core.lvr_validation as new_validation
import research.lvr.reporting.build_oracle_gap_charts as new_charts
from research.lvr.paths import CONFIG_ROOT
import script.build_oracle_gap_charts as old_charts
import script.flow_classification as old_flow_classification
import script.lvr_validation as old_validation
import script.run_dutch_auction_backtest as old_backtest


class ResearchPackageCompatibilityTest(unittest.TestCase):
    def test_old_wrappers_expose_new_implementation_objects(self) -> None:
        self.assertIs(old_validation.correction_trade, new_validation.correction_trade)
        self.assertIs(old_backtest._time_to_fill, new_backtest._time_to_fill)
        self.assertIs(old_charts.main, new_charts.main)

    def test_old_and_new_label_config_defaults_match(self) -> None:
        self.assertEqual(
            old_flow_classification.load_label_config("script/label_config.json"),
            new_flow_classification.load_label_config(),
        )

    def test_compat_config_copies_match_canonical_config(self) -> None:
        for name in ("label_config.json", "backtest_manifest.json", "replay_exclusions.json"):
            with self.subTest(name=name):
                old_payload = json.loads(Path("script", name).read_text(encoding="utf-8"))
                new_payload = json.loads((CONFIG_ROOT / name).read_text(encoding="utf-8"))
                self.assertEqual(old_payload, new_payload)


if __name__ == "__main__":
    unittest.main()
