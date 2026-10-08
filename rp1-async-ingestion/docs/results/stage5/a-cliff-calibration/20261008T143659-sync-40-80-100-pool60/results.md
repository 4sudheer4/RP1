| run | rate b/s | batch capture 1st try | batch capture eventual | event capture | lost | dup rows | status mismatches | 1st-try ack p50 ms | p99 ms | client attempts/batch | server batches/batch (sync) | share done after client timeout (sync) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| sync-100 | 100 | 0.013 | 0.013 | 1.0 | 0 | 0 | 0 | 2000 | 2016 | 3.961 | - | - |
| sync-40 | 40 | 0.998333 | 1.0 | 1.0 | 0 | 0 | 0 | 365 | 1634 | 1.001667 | 1.0 | 0.0 |
| sync-80 | 80 | 0.090833 | 0.098333 | 1.0 | 0 | 0 | 0 | 2000 | 2007 | 3.7125 | - | - |

Definitions: batch capture = share of batches that got 2xx (first attempt / within ENS's 3 retries). Event capture = distinct generated valid events present in Postgres after drain / generated. Server batches/batch = batches the sync server fully processed / batches generated (>1 = work for requests the client had abandoned and retried).
