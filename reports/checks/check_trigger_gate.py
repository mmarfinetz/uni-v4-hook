from decimal import Decimal
from _common import fail
from script.oracle_gap_policy import (
    AuctionEligibilityState,
    execution_constrained_policy_is_eligible,
    is_auction_eligible,
    lp_net_with_delay_budget_policy_is_eligible,
    lp_only_policy_is_eligible,
)


def main() -> None:
    states = [
        AuctionEligibilityState(Decimal("4.999"), 1),
        AuctionEligibilityState(Decimal("5"), -1),
        AuctionEligibilityState(Decimal("25"), 1),
    ]
    for state in states:
        expected = is_auction_eligible(state, Decimal("5"))
        checks = [
            lp_only_policy_is_eligible(state, Decimal("5")),
            execution_constrained_policy_is_eligible(state, Decimal("5")),
            lp_net_with_delay_budget_policy_is_eligible(state, Decimal("5")),
        ]
        if checks != [expected, expected, expected]:
            fail("policy eligibility diverged from shared helper")


if __name__ == "__main__":
    main()
