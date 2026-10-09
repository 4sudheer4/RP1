# Experiment (a): sync vs async cliff
- Load: open loop, 20 events/batch, 40s per rate, client timeout 2000ms, 3 retries at 1s/2s/4s.
- Downstream (both modes): capacity 8 concurrent calls x 5ms per event
  => theoretical max ~ 8 / (5ms x 20) batches/s.
- Sync: ingest 'sync' profile does all work before responding. Async: ingest acks after acks=all produce;
  one consumer instance (3 threads) does the same work.
- ENS suspension model: off.
- Rates: 40 60 70 80 90 100 120. Machine: Apple M1, 16 GB RAM; Docker VM 7.7 GB.

**Validity note:** the laptop lid closed at 16:08 (pmset "Clamshell Sleep") and the machine slept repeatedly until
16:58. async-100 (started 16:05) and async-120 ran across that window and are invalid; moved to
`../../a-cliff-calibration/20261008T153227-invalid-mac-slept-1608/`. All other runs here finished before 16:08.
