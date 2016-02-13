package com.dataartisans;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilteringJob {

  private static long DEDUPE_CACHE_EXPIRATION_TIME_MS = 10_000;

  public static void main(String[] args) throws Exception {

    // Setup the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(1000);

    DataStream<Tweet> tweetStream = env.addSource(new TweetSourceFunction());

    tweetStream
      .keyBy(Tweet.getKeySelector())
      .filter(new DedupeFilterFunction(Tweet.getKeySelector(), DEDUPE_CACHE_EXPIRATION_TIME_MS))
      .print();

    // execute program
    env.execute("Flink Java API Skeleton");
  }

}
