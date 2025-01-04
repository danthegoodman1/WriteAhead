# WriteAhead

A partitioned WAL crate for building append-only high throughput durability systems.

A great component for a (distributed) data store.

For distributed usage, consider FDB's model (may want to place a custom in-memory index in front for custom ID->offset mappings)

Sub-ms persistence, sometimes sneaking over if a file rotation occurrs:
```
2025-01-04T19:56:07.459536Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=1.08ms time.idle=3.58µs
2025-01-04T19:56:07.459541Z DEBUG src/logfile.rs:283: Logfile 0000000021 writer actor disconnected
2025-01-04T19:56:07.459836Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=267µs time.idle=3.50µs
2025-01-04T19:56:07.460112Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=247µs time.idle=3.42µs
2025-01-04T19:56:07.460400Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=249µs time.idle=4.83µs
2025-01-04T19:56:07.460660Z DEBUG write_batch: src/write_ahead.rs:150: Rotating log file 22
2025-01-04T19:56:07.460730Z DEBUG src/logfile.rs:305: Sealing logfile 0000000022
2025-01-04T19:56:07.461482Z DEBUG write_batch:launch: src/logfile.rs:270: close time.busy=309µs time.idle=4.79µs path="./test_logs/test_write_ahead_stream/0000000023.log"
2025-01-04T19:56:07.461530Z DEBUG src/logfile.rs:283: Logfile 0000000022 writer actor disconnected
2025-01-04T19:56:07.461528Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=1.09ms time.idle=4.04µs
2025-01-04T19:56:07.461755Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=189µs time.idle=5.08µs
2025-01-04T19:56:07.461960Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=177µs time.idle=3.50µs
2025-01-04T19:56:07.462155Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=171µs time.idle=4.08µs
2025-01-04T19:56:07.462360Z DEBUG write_batch: src/write_ahead.rs:150: Rotating log file 23
2025-01-04T19:56:07.462395Z DEBUG src/logfile.rs:305: Sealing logfile 0000000023
2025-01-04T19:56:07.463003Z DEBUG write_batch:launch: src/logfile.rs:270: close time.busy=272µs time.idle=4.50µs path="./test_logs/test_write_ahead_stream/0000000024.log"
2025-01-04T19:56:07.463042Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=859µs time.idle=4.92µs
2025-01-04T19:56:07.463047Z DEBUG src/logfile.rs:283: Logfile 0000000023 writer actor disconnected
2025-01-04T19:56:07.463325Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=253µs time.idle=3.62µs
2025-01-04T19:56:07.463514Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=161µs time.idle=4.12µs
2025-01-04T19:56:07.463705Z DEBUG write_batch: src/write_ahead.rs:124: close time.busy=157µs time.idle=5.04µs
2025-01-04T19:56:07.463897Z DEBUG write_batch: src/write_ahead.rs:150: Rotating log file 24
```

M3 Max MBP

A rotation has to do 2 additional writes, and there's a bit of overhead in message passing with the flume channels. This was also run in a VSCode dev container.
