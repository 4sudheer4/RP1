| run | rate b/s | batch capture 1st try | batch capture eventual | event capture | lost | dup rows | status mismatches | 1st-try ack p50 ms | p99 ms | client attempts/batch | server batches/batch (sync) | share done after client timeout (sync) | batches dropped by ENS suspension |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| async-40 | 40 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 14 | 393 | 1.0 | - | - | 0 |
| async-60 | 60 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 16 | 419 | 1.0 | - | - | 0 |
| async-70 | 70 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 23 | 556 | 1.0 | - | - | 0 |
| async-80 | 80 | 0.852188 | 1.0 | 1.0 | 0 | 0 | 0 | 19 | 1052 | 1.342813 | - | - | 0 |
| async-90 | 90 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 24 | 364 | 1.0 | - | - | 0 |
| sync-100 | 100 | 0.01275 | 0.01275 | 0.9515 | 3880 | 0 | 0 | 2000 | 2036 | 3.96175 | 2.86 | 0.481 | 0 |
| sync-120 | 120 | 0.008125 | 0.008125 | 0.801042 | 19100 | 0 | 0 | 2000 | 2853 | 3.975625 | 2.15 | 0.432 | 0 |
| sync-40 | 40 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 184 | 1265 | 1.0 | 1.0 | 0.0 | 0 |
| sync-60 | 60 | 0.999583 | 1.0 | 1.0 | 0 | 0 | 0 | 238 | 1385 | 1.000417 | 1.0 | 0.0 | 0 |
| sync-70 | 70 | 0.16 | 0.188929 | 1.0 | 0 | 0 | 0 | 1999 | 2010 | 3.462857 | 3.46 | 0.278 | 0 |
| sync-80 | 80 | 0.015313 | 0.015938 | 1.0 | 0 | 0 | 0 | 2000 | 2278 | 3.952813 | 3.95 | 0.334 | 0 |
| sync-90 | 90 | 0.032778 | 0.034722 | 0.982778 | 1240 | 0 | 0 | 2000 | 2173 | 3.897778 | 3.37 | 0.435 | 0 |

Definitions: batch capture = share of batches that got 2xx (first attempt / within ENS's 3 retries). Event capture = distinct generated valid events present in Postgres after drain / generated. Server batches/batch = batches the sync server fully processed / batches generated (>1 = work for requests the client had abandoned and retried).
