from decimal import Decimal
from _common import fail
from script.oracle_gap_policy import build_eligibility_state, mean_solver_payout_bps, recapture_pct, stale_loss_bps


def main() -> None:
    state = build_eligibility_state(Decimal("101"), Decimal("100"))
    if state.stale_gap_bps_before <= 0 or state.stale_gap_sign != 1:
        fail("stale gap fields are wrong")
    if stale_loss_bps(Decimal("2"), Decimal("100")) != Decimal("200.00"):
        fail("stale_loss_bps mismatch")
    if mean_solver_payout_bps(Decimal("1"), Decimal("4")) != Decimal("2500.00"):
        fail("mean_solver_payout_bps mismatch")
    if recapture_pct(Decimal("3"), Decimal("4")) != Decimal("75.00"):
        fail("recapture_pct mismatch")


if __name__ == "__main__":
    main()
