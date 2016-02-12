package com.dataartisans;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilteringJob {

  public static void main(String[] args) throws Exception {
    // Setup the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(1000);
    env.getConfig().setExecutionRetryDelay(1000);

    DataStream<Tweet> tweetStream = env.addSource(new TweetSourceFunction());

    tweetStream
      .keyBy("tweetId")
      .filter(new DedupeFilterFunction())
      .print();

    // execute program
    env.execute("Flink Java API Skeleton");
  }

}
