# Experiment (a): sync vs async cliff
- Load: open loop, 20 events/batch, 30s per rate, client timeout 2000ms, 3 retries at 1s/2s/4s.
- Downstream (both modes): capacity 8 concurrent calls x 5ms per event
  => theoretical max ~ 8 / (5ms x 20) batches/s.
- Sync: ingest 'sync' profile does all work before responding. Async: ingest acks after acks=all produce;
  one consumer instance (3 threads) does the same work.
- Rates: 40 80 100. Machine: Apple M1, 16 GB RAM; Docker VM 7.7 GB.
