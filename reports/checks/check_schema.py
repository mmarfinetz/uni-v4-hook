import sys
from _common import require_fields


FIELDS = ["pool", "trigger_gap_bps", "base_fee_bps", "start_concession_bps", "concession_growth_bps_per_sec", "max_fee_bps", "recapture_pct", "auction_clear_rate", "mean_solver_payout_bps", "n_trigger_events", "lp_net_quote_token"]


if __name__ == "__main__":
    require_fields(sys.argv[1], FIELDS)
