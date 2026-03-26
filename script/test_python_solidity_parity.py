import json
import math
import os
import re
import shutil
import subprocess
import unittest
from decimal import Decimal, ROUND_FLOOR, getcontext
from pathlib import Path

from script.lvr_historical_replay import StrategyConfig, is_toxic, quoted_fee_fraction
from script.lvr_validation import required_min_width_ticks


getcontext().prec = 80
Q96 = Decimal(1 << 96)
REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = REPO_ROOT / "test" / "helpers" / "PythonParityHarness.sol"
DEFAULT_ANVIL_PRIVATE_KEY = (
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
)


class PythonSolidityParityTest(unittest.TestCase):
    harness_address: str | None = None

    @classmethod
    def setUpClass(cls) -> None:
        anvil_url = os.environ.get("ANVIL_URL")
        if not anvil_url:
            raise unittest.SkipTest("ANVIL_URL is not set.")
        if shutil.which("forge") is None or shutil.which("cast") is None:
            raise unittest.SkipTest("forge/cast are required for Solidity parity.")
        if not HARNESS_PATH.exists():
            raise unittest.SkipTest(f"Missing helper harness at {HARNESS_PATH}.")
        cls.harness_address = cls._deploy_harness(anvil_url)

    @classmethod
    def _deploy_harness(cls, anvil_url: str) -> str:
        completed = subprocess.run(
            [
                "forge",
                "create",
                str(HARNESS_PATH) + ":PythonParityHarness",
                "--rpc-url",
                anvil_url,
                "--private-key",
                DEFAULT_ANVIL_PRIVATE_KEY,
            ],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            raise unittest.SkipTest(
                f"Unable to deploy parity harness via forge create: {completed.stderr or completed.stdout}"
            )
        match = re.search(r"Deployed to:\s*(0x[a-fA-F0-9]{40})", completed.stdout)
        if match is None:
            raise unittest.SkipTest(f"Could not parse harness address from forge output: {completed.stdout}")
        return match.group(1)

    def test_fee_parity_matrix(self) -> None:
        strategy = StrategyConfig(
            name="hook_fee",
            curve="hook",
            base_fee_fraction=5.0 / 10_000.0,
            max_fee_fraction=500.0 / 10_000.0,
            alpha_fraction=10_000.0 / 10_000.0,
            max_oracle_age_seconds=3600,
        )
        for oracle_price, pool_price, direction in fee_matrix():
            toxic_python, fee_fraction = quoted_fee_fraction(
                strategy,
                direction,
                oracle_price,
                pool_price,
            )
            fee_units_python = int(fee_fraction * 1_000_000)
            toxic_solidity, fee_units_solidity = self.call_preview_swap_fee(
                oracle_price=oracle_price,
                pool_price=pool_price,
                direction=direction,
            )
            self.assertEqual(toxic_python, toxic_solidity)
            self.assertLessEqual(abs(fee_units_python - fee_units_solidity), 1)

    def test_width_parity_matrix(self) -> None:
        tick_spacing = 60
        for sigma2_per_second_wad, latency_secs, lvr_budget_wad in width_matrix():
            sigma = math.sqrt(sigma2_per_second_wad / 1e18)
            min_width_python = required_min_width_ticks(sigma, latency_secs, lvr_budget_wad / 1e18)
            if min_width_python is None:
                self.fail("Width matrix must not include impossible budgets.")
            min_width_python = int(math.ceil(min_width_python / tick_spacing) * tick_spacing)
            min_width_solidity = self.call_min_width(
                sigma2_per_second_wad=sigma2_per_second_wad,
                latency_secs=latency_secs,
                lvr_budget_wad=lvr_budget_wad,
            )
            self.assertLessEqual(abs(min_width_python - min_width_solidity), tick_spacing)

    def test_classification_parity_matrix(self) -> None:
        for oracle_price, pool_price, direction in fee_matrix():
            toxic_python = is_toxic(direction, oracle_price, pool_price)
            toxic_solidity, _ = self.call_preview_swap_fee(
                oracle_price=oracle_price,
                pool_price=pool_price,
                direction=direction,
            )
            self.assertEqual(toxic_python, toxic_solidity)

    def call_preview_swap_fee(self, *, oracle_price: float, pool_price: float, direction: str) -> tuple[bool, int]:
        pool_sqrt_price_x96 = price_to_sqrt_price_x96(pool_price)
        zero_for_one = direction == "zero_for_one"
        raw = self.cast_call(
            "previewSwapFeeForPrices(uint256,uint160,bool,uint24,uint24,uint24)(bool,uint24)",
            [
                str(price_to_wad(oracle_price)),
                str(pool_sqrt_price_x96),
                str(zero_for_one).lower(),
                "500",
                "50000",
                "10000",
            ],
        )
        decoded = json.loads(raw)
        return bool(decoded[0]), int(decoded[1], 16) if isinstance(decoded[1], str) else int(decoded[1])

    def call_min_width(
        self,
        *,
        sigma2_per_second_wad: int,
        latency_secs: int,
        lvr_budget_wad: int,
    ) -> int:
        raw = self.cast_call(
            "minWidthForRisk(uint256,uint32,uint256,uint256)(uint256)",
            [
                str(sigma2_per_second_wad),
                str(latency_secs),
                str(lvr_budget_wad),
                str(800_000_000_000_000),
            ],
        )
        decoded = json.loads(raw)
        return int(decoded, 16) if isinstance(decoded, str) else int(decoded)

    def cast_call(self, signature: str, args: list[str]) -> str:
        if self.harness_address is None:
            raise unittest.SkipTest("Harness was not deployed.")
        completed = subprocess.run(
            [
                "cast",
                "call",
                self.harness_address,
                signature,
                *args,
                "--rpc-url",
                os.environ["ANVIL_URL"],
                "--json",
            ],
            cwd=REPO_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            self.fail(completed.stderr or completed.stdout)
        return completed.stdout.strip()


def fee_matrix() -> list[tuple[float, float, str]]:
    matrix: list[tuple[float, float, str]] = []
    for gap in [0.5, 1.0, 2.0, 5.0, 10.0]:
        matrix.append((math.exp(gap / 10_000.0), 1.0, "one_for_zero"))
    for gap in [20.0, 30.0, 50.0, 100.0, 200.0]:
        matrix.append((math.exp(-gap / 10_000.0), 1.0, "zero_for_one"))
    return matrix


def width_matrix() -> list[tuple[int, int, int]]:
    return [
        (100_000_000_000, 30, 10_000_000_000_000_000),
        (1_000_000_000_000, 60, 10_000_000_000_000_000),
        (10_000_000_000_000, 120, 10_000_000_000_000_000),
        (400_000_000_000_000, 60, 20_000_000_000_000_000),
        (800_000_000_000_000, 300, 50_000_000_000_000_000),
    ]


def price_to_wad(price: float) -> int:
    return int((Decimal(str(price)) * Decimal(10**18)).to_integral_value(rounding=ROUND_FLOOR))


def price_to_sqrt_price_x96(price: float) -> int:
    sqrt_price = Decimal(str(price)).sqrt()
    return int((sqrt_price * Q96).to_integral_value(rounding=ROUND_FLOOR))


if __name__ == "__main__":
    unittest.main()
