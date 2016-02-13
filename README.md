# FilteringExample

This shows an example of how to remove duplicates in a stream.  It essentially uses an LRU cache and filters out duplicate messages that are seen within a set amount of time.  Have a look at the DedupeFilterFunction.

In this example there is a stream of Tweets except (just to show the deduplication) there are *lots* of duplicate Tweet IDs.  The LRU cache is setup for keys to expire out every 10 seconds by default.

The end result is that you'll see 10 tweets every 10 seconds even though the source is generating tweets in the range 0..9 randomly in a tight loop.

This filtering is done in a fully fault-tolerant way since the state of the DedupeFilterFunction is checkpointed and will be restored in the event of failure.
