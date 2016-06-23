package com.dataartisans;

import com.dataartisans.domain.TweetImpression;
import com.dataartisans.filters.DedupeFilterFunction;
import com.dataartisans.sources.TweetSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DedupeFilteringJob {

  private static long DEDUPE_CACHE_EXPIRATION_TIME_MS = 1_000;

  public static void main(String[] args) throws Exception {

    // Setup the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(1000);

    DataStream<TweetImpression> tweetStream = env.addSource(new TweetSourceFunction(true), "TweetImpression Source w/ duplicates");

    tweetStream
      .keyBy(TweetImpression.getKeySelector())
      .filter(new DedupeFilterFunction(TweetImpression.getKeySelector(), DEDUPE_CACHE_EXPIRATION_TIME_MS))
      .print();

    // execute program
    env.execute();
  }

}
