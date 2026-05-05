from _common import fail, text


def main() -> None:
    audit = text("reports/charts/audit_log.md")
    if "stress_lp_loss_baselines_usd" in audit and "read-only reference" not in audit:
        fail("legacy log dollar chart was not discarded")
    active = text("script/build_oracle_gap_charts.py").lower()
    if "log" in active and "audit_log" not in active:
        fail("active chart script appears to use log scale")


if __name__ == "__main__":
    main()
