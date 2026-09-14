# Superseded Local Four-Protocol Fault Study (2026-09-13)

This study is retained only as a historical pointer. Its short measurement
windows and earlier Shortfin/Wahoo implementations produced several startup,
leader-rotation, and liveness artifacts. Do not use its numerical results.

The replacement study is:

```text
docs/local-precloud-validation-20260914.md
```

The replacement uses 60-second runs, corrected round-robin leader slots,
digest-bound Wahoo votes, certified Wahoo PBC branch delivery, and late
slow-path voting. Its curated CSV files use the `local_precloud_` prefix.
