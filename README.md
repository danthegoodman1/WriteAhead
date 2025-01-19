# WriteAhead

A partitioned WAL crate for building append-only high throughput durability systems.

A great component for a (distributed) data store.

For distributed usage, consider FDB's model (may want to place a custom in-memory index in front for custom ID->offset mappings)

Almost always under 200us persistence on my M3 MBP, rarely breaking the 1ms mark when a log rotation occurrs.

This is an aggressive log rotation test where each write is a `Hello world! {i}` record, and log rotation occurs every 4 writes due to log file size restrictions:

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

# Notes

## Separating writers and readers

By using a single writer, we can increase throughput via batching with simplicity (`io_uring` does not provide as significant benefit for a single writer with a dedicated thread, only if you can't coalesce writes really, and if you have fewer threads than disks).

With a separate reader, can we have a single writer using the `sfd::fs::File`, while readers can use `io_uring` to prevent blocking each other. This means the writer never waits for readers, and the readers never wait for each other.

### Some thoughts on io_uring

io_uring for fileio is only particularly useful if using direct IO, which generally is good for a data storage system that intends to store a large amount of data.

However, one use case where it's not a good fit is for a WAL. That's because in the normal case, we expect readers will likely be reading very recently written data, and as you can tell from tests, `SimpleFile` which uses the page cache reads back faster so much faster than the `IOUringFile` package:

`SimpleFile`:
```
2025-01-19T00:35:31.122171Z DEBUG src/write_ahead.rs:92: Creating initial log file
2025-01-19T00:35:31.206835Z DEBUG src/write_ahead.rs:445: Write time taken: 83.650875ms
2025-01-19T00:35:31.208957Z DEBUG src/write_ahead.rs:457: Read time taken: 2.086291ms
```

`IOUringFile` (which uses `SimpleFile` for writing):
```
2025-01-19T00:35:21.075255Z DEBUG src/write_ahead.rs:92: Creating initial log file
2025-01-19T00:35:21.154822Z DEBUG src/write_ahead.rs:484: Write time taken: 79.14325ms
2025-01-19T00:35:21.280025Z DEBUG src/write_ahead.rs:496: Read time taken: 125.154709ms
```

As you can see, reading recently written files is quite a bit faster with the page cache. As a result, `IOUringFile` is more likely to be removed than completed.

## Integrating with a thread-safe API framework (Axum, tonic, etc.)

Since WriteAhead is single-threaded, to preserve performance it's probably best to spawn off a thread dedicated for this, and use a channel to communicate writes and reads. Then you can even buffer them up in memory and micro-batch them (e.g. at most 500us) for increased throughput.

If you need to stream a WriteAheadStream back to a response, you can use either a stream helper, or write a little helper that will turn that stream into a channel, then use something like `flume::Receiver::into_stream` to respond. Should use a bounded channel so you don't bloat memory.

WriteAheadStream is also Send (see `test_write_ahead_stream_is_send`), so you can actually send it over the channel back to the API handler, and feed that stream directly to the response as well if you don't need as much control (e.g. you can just stream to the end)
