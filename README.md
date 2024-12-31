# WriteAhead

A partitioned WAL crate for building append-only high throughput durability systems.

A great component for a (distributed) data store.

For distributed usage, consider FDB's model (may want to place a custom in-memory index in front for custom ID->offset mappings)
