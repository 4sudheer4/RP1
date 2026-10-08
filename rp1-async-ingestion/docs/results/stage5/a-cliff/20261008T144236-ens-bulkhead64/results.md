| run | rate b/s | batch capture 1st try | batch capture eventual | event capture | lost | dup rows | status mismatches | 1st-try ack p50 ms | p99 ms | client attempts/batch | server batches/batch (sync) | share done after client timeout (sync) | batches dropped by ENS suspension |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| async-ens-100 | 100 | 0.74475 | 0.74925 | 0.74925 | 20060 | 0 | 0 | 24 | 387 | 0.75375 | - | - | 1003 |
| async-ens-40 | 40 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 20 | 540 | 1.0 | - | - | 0 |
| async-ens-70 | 70 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 17 | 373 | 1.0 | - | - | 0 |
| sync-ens-100 | 100 | 0.06475 | 0.1385 | 0.13975 | 68820 | 0 | 0 | 1999 | 2033 | 0.3295 | 0.33 | 0.42 | 3441 |
| sync-ens-40 | 40 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 199 | 997 | 1.0 | 1.0 | 0.0 | 0 |
| sync-ens-70 | 70 | 0.422143 | 0.45 | 0.45 | 30800 | 0 | 0 | 735 | 2002 | 0.477857 | 0.48 | 0.058 | 1540 |

Definitions: batch capture = share of batches that got 2xx (first attempt / within ENS's 3 retries). Event capture = distinct generated valid events present in Postgres after drain / generated. Server batches/batch = batches the sync server fully processed / batches generated (>1 = work for requests the client had abandoned and retried).
