# Captions

## chart_a_recapture_per_pool.png
Shows the selected parameter set's inferred stale-price value split, ordered by USD-equivalent opportunity size, with the fixed-fee V3 recapture marker overlaid. It matters because the reader can see where stale value went before and where it goes under the auction.

## chart_b_sensitivity_heatmap.png
Shows the trigger-gap by base-fee check with trigger 10.0 bps, base fee 5.0 bps, start concession 10.0 bps, growth 0.5 bps/sec, max fee 2500.0 bps held fixed for the other auction parameters. Hatched entries have at least one pool below 0.9 clear rate; the red outline marks the recommended parameter set.

## chart_c_temporal_recapture.png
Shows inferred fixed-window LP net gain versus fixed-fee V3 for the recommended parameter set: trigger 10.0 bps, base fee 5.0 bps, start concession 10.0 bps, growth 0.5 bps/sec, max fee 2500.0 bps. The log-dollar scale and median labels keep ordinary windows and outliers visible together.

## chart_d_consistency.png
Appendix check showing mean recapture against cross-pool variation for every parameter set, with the recommended parameter set annotated.
