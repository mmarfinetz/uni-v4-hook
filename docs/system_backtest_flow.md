# System And Backtest Flow

This diagram matches the current May 2026 research bundle. It separates the on-chain hook paths from the two empirical pipelines used in the paper:

- the headline October 2025 stale-repricing sensitivity grid, and
- the observed-flow replay check used as supporting evidence.

The Dutch-auction path is a backtested/proposed repricing mechanism. The live Solidity hook currently exposes the synchronous fee override and liquidity-admission guards.

```mermaid
flowchart TD
    subgraph A[Production Hook Paths]
        A1[Uniswap v4 PoolManager swap]
        A2[OracleAnchoredLVRHook.beforeSwap]
        A3[Load pool config]
        A4[Load fresh oracle price]
        A5[Refresh EWMA risk state]
        A6[Read current pool slot0]
        A7[Quote informed-flow fee with square-root LVR premium]
        A8{Oracle stale or fee above maxFee?}
        A9[Revert / fail closed]
        A10[Return override LP fee]

        A11[PoolManager add liquidity]
        A12[OracleAnchoredLVRHook.beforeAddLiquidity]
        A13[Convert oracle price to reference tick]
        A14[Check LP range center]
        A15[Compute minimum width from sigma2, latency, and LVR budget]
        A16{Centered and wide enough?}
        A17[Accept liquidity]
        A18[Reject range]

        A1 --> A2 --> A3 --> A4 --> A5 --> A6 --> A7 --> A8
        A8 -->|yes| A9
        A8 -->|no| A10
        A11 --> A12 --> A13 --> A14 --> A15 --> A16
        A16 -->|yes| A17
        A16 -->|no| A18
    end

    subgraph B[Headline October 2025 Grid Inputs]
        B1[Resolved month windows<br/>.tmp/month_2025_10/checkpointed_export_combined/windows]
        B2[Per-window input bundle<br/>pool_snapshot.json<br/>initialized_ticks.csv<br/>liquidity_events.csv<br/>swap_samples.csv<br/>oracle_updates.csv<br/>market_reference_updates.csv]
        B3[Four pools<br/>WETH/USDC, WBTC/USDC, LINK/WETH, UNI/WETH]

        B1 --> B2 --> B3
    end

    subgraph C[Headline Sensitivity Grid]
        C1[script/run_oracle_gap_sensitivity_grid.py]
        C2[Build observed block timeline]
        C3[Compute fixed-fee V3 baseline]
        C4[Loop over 324 parameter sets per pool]
        C5[Compute exact stale loss with correction_trade]
        C6[Apply shared stale-gap trigger<br/>stale_gap_bps_before >= trigger_gap_bps]
        C7[Simulate pending auction concession path]
        C8[Apply max-fee and clear conditions]
        C9[Aggregate per-pool and per-window metrics]

        C1 --> C2 --> C3 --> C4 --> C5 --> C6 --> C7 --> C8 --> C9
    end

    B3 --> C1

    subgraph D[Observed-Flow Replay Check]
        D1[script/run_dutch_auction_ablation_study.py]
        D2[Build extended manifest from frozen inputs and replay diagnostics]
        D3[run_backtest_batch per window]
        D4[Exact V3 replay and fee-identity checks]
        D5[Replay observed swaps through fixed, hook, linear, and log fee policies]
        D6[Overlay Dutch-auction branch]
        D7[Compare broad all-stale rule with hook-based auction rule]

        D1 --> D2 --> D3 --> D4 --> D5 --> D6 --> D7
    end

    subgraph E[Report And Paper Artifacts]
        E1[reports/sensitivity_grid_combined.csv<br/>1,296 pool-level rows]
        E2[reports/sensitivity_grid_windows.csv<br/>40,176 window-level rows]
        E3[reports/parameter_set_outcomes.md<br/>one row per tested parameter set]
        E4[reports/sensitivity_impact_table.md<br/>one-step parameter sensitivity]
        E5[reports/solver_economics_table.md<br/>solver payout in USD terms]
        E6[reports/charts/chart_a through chart_d]
        E7[reports/checks/*.py<br/>deterministic paper checks]
        E8[lvr_v4_hook_paper_dutch_auction_v2.pdf]

        E1 --> E3
        E1 --> E4
        E1 --> E5
        E1 --> E6
        E2 --> E6
        E3 --> E8
        E4 --> E8
        E5 --> E8
        E6 --> E8
        E7 --> E8
    end

    C9 --> E1
    C9 --> E2
    D7 --> E8
```

## Reading Notes

- The production hook path is synchronous: `beforeSwap` computes a fee override, while `beforeAddLiquidity` enforces centering and minimum-width admission.
- The headline empirical result comes from `script/run_oracle_gap_sensitivity_grid.py`, not the older one-page-proof pipeline.
- The auction trigger is the current pool-oracle stale gap in bps, not an absolute dollar threshold.
- The observed-flow replay is supporting evidence for the study machinery. It does not replace the October grid as the source of the recommended parameter set.
- The report bundle in `reports/` is the audit trail for what was tried and how the final paper figures and tables were produced.
