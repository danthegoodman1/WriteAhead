# WriteAhead

A partitioned WAL crate for building append-only high throughput durability systems.

A great component for a (distributed) data store.

For distributed usage, consider FDB's model (may want to place a custom in-memory index in front for custom ID->offset mappings)


## TODO

Now that we've removed mutable methods (interior mutability), we can use Arc without Mutex.

Some relationships:
- WriteAhead creates and manages new access to Logfiles
- LogFileStreams may outlive WriteAhead's ownership of a Logfile, and therefore needs it's own Arc (and WriteAhead needs an Arc too), for example if a request comes in and starts streaming, but 
- WriteAheadStream never can outlive a WriteAhead, so it can operate off a reference to it (no Arc needed)
- A WriteAheadStream must take a snapshot of existing Logfiles, but may gather new ones. This ensures no gaps are created from dropping. Therefore the BTreeMap will need either a Mutex, or maybe just some atomic/waiting for the ability to take mutability. Or we can have some sequential processing of instructions, and rolling and TTL'ing is an internally sent instruction (maybe use mpsc then, and poke a worker via another mpsc)
- For writing, we can still keep a reference to the active log file outside in a dedicated Arc so we don't need to lock to write

Updating in a github issue
