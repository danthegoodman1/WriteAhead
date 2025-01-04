# WriteAhead

A partitioned WAL crate for building append-only high throughput durability systems.

A great component for a (distributed) data store.

For distributed usage, consider FDB's model (may want to place a custom in-memory index in front for custom ID->offset mappings)

Almost always under 200us persistence on my M3 MBP, rarely breaking the 1ms mark when a log rotation occurrs.

```
2025-01-04T20:07:40.161477Z DEBUG src/write_ahead.rs:610: Write time taken: 1.003083ms
2025-01-04T20:07:40.161649Z DEBUG src/write_ahead.rs:610: Write time taken: 154.542µs
2025-01-04T20:07:40.161817Z DEBUG src/write_ahead.rs:610: Write time taken: 148.125µs
2025-01-04T20:07:40.162006Z DEBUG src/write_ahead.rs:610: Write time taken: 177.584µs
2025-01-04T20:07:40.162882Z DEBUG src/write_ahead.rs:610: Write time taken: 864.125µs
2025-01-04T20:07:40.163141Z DEBUG src/write_ahead.rs:610: Write time taken: 242.292µs
2025-01-04T20:07:40.163355Z DEBUG src/write_ahead.rs:610: Write time taken: 195.208µs
2025-01-04T20:07:40.163544Z DEBUG src/write_ahead.rs:610: Write time taken: 175µs
2025-01-04T20:07:40.164352Z DEBUG src/write_ahead.rs:610: Write time taken: 795.958µs
2025-01-04T20:07:40.164517Z DEBUG src/write_ahead.rs:610: Write time taken: 152.125µs
2025-01-04T20:07:40.164672Z DEBUG src/write_ahead.rs:610: Write time taken: 141.5µs
2025-01-04T20:07:40.164813Z DEBUG src/write_ahead.rs:610: Write time taken: 129.125µs
2025-01-04T20:07:40.165483Z DEBUG src/write_ahead.rs:610: Write time taken: 657.542µs
2025-01-04T20:07:40.165636Z DEBUG src/write_ahead.rs:610: Write time taken: 141.333µs
2025-01-04T20:07:40.165784Z DEBUG src/write_ahead.rs:610: Write time taken: 138.042µs
2025-01-04T20:07:40.165942Z DEBUG src/write_ahead.rs:610: Write time taken: 144.083µs
2025-01-04T20:07:40.166539Z DEBUG src/write_ahead.rs:610: Write time taken: 586.75µs
2025-01-04T20:07:40.166691Z DEBUG src/write_ahead.rs:610: Write time taken: 139.666µs
2025-01-04T20:07:40.166834Z DEBUG src/write_ahead.rs:610: Write time taken: 132.917µs
2025-01-04T20:07:40.167017Z DEBUG src/write_ahead.rs:610: Write time taken: 173.042µs
```

TLDR it's super fast.
