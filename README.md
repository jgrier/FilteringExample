# Filtering Examples
## DedupeFilteringJob

This shows an example of how to remove duplicates in a stream.  It essentially uses an LRU cache and filters out duplicate messages that are seen within a set amount of time.  Have a look at the DedupeFilterFunction.

In this example there is a stream of TweetImpressions except (just to show the deduplication) there are *lots* of duplicate Tweet IDs.  The LRU cache is setup for keys to expire out every 1 second by default.

The tweets are generated randomly for tweets with IDs in the range [1..10]

This filtering is done in a fully fault-tolerant way since the state of the DedupeFilterFunction is checkpointed and will be restored in the event of failure.

## TweetImpressionFilteringJob
This job consumes a stream of TweetImpressions and a stream of TweetSubscriptions from customers. There is a separate output sink for each customer and each customer receives only those TweetImpressions for tweets that they have subscribed to.  Note that both the TweetImpression stream and the TweetSubscription stream are both keyed by TweetID.  This naturally partitions the streams correctly across any number of hosts.

The TweetImpressions stream contains many duplicates so the the stream is first deduplicated using the DedupeFilterFunction.

The TweetImpressions are generated randomly for tweets with IDs in the range [1..10]

The tweet subscriptions are read from a socket.  The format of the messages is just:
> customer_name tweetId

For example:

>  Google 5
  
Indicates that customer "Google" wants to receive impressions data for the tweet with id "5"

To run this example pass the host name and port of the socket server where the subscription
messages can be read from.  For example:

> flink run -c com.dataartisans.TweetImpressionFilteringJob FilteringExample-1.0-SNAPSHOT.jar --host localhost --port 9999

A great way to test this is to use netcat to provide the socket server.  For example:

> nc -lk 9999

Then type messages to the console to enable TweetSubscriptions in the format described above

Also note the set of possible customers is fixed.  Customer must be one of:

Google, Twitter, Facebook, Apple, Amazon

