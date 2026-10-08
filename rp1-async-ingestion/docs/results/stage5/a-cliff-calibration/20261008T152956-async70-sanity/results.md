| run | rate b/s | batch capture 1st try | batch capture eventual | event capture | lost | dup rows | status mismatches | 1st-try ack p50 ms | p99 ms | client attempts/batch | server batches/batch (sync) | share done after client timeout (sync) | batches dropped by ENS suspension |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| async-70 | 70 | 1.0 | 1.0 | 1.0 | 0 | 0 | 0 | 53 | 637 | 1.0 | - | - | 0 |

Definitions: batch capture = share of batches that got 2xx (first attempt / within ENS's 3 retries). Event capture = distinct generated valid events present in Postgres after drain / generated. Server batches/batch = batches the sync server fully processed / batches generated (>1 = work for requests the client had abandoned and retried).
