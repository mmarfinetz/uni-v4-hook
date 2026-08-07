# Slither Triage

Baseline: Slither 0.11.5, reviewed 2026-08-03. CI fails on high-severity findings.

The current lower-severity detector output is accepted for these specific reasons:

- `incorrect-equality` on `startTs == 0`: zero is the explicit unopened-clock
  sentinel; timestamps are not compared for economic equality.
- `unused-return` on v4 `getSlot0` and Chainlink round tuples: the omitted tuple
  members are outside each function's decision. Round ID, answer, update time, and
  `answeredInRound` are retained where required.
- `timestamp`: oracle age, sequencer grace, and linear auction concession are
  intentionally time-based. Miner/sequencer timestamp tolerance is small relative
  to the configured windows and cannot bypass the concession cap or oracle checks.

Any changed line reported under these detector classes requires fresh review.
This triage is not a substitute for an independent audit.
