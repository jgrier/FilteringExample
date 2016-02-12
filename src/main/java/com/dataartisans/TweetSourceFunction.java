package com.dataartisans;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class TweetSourceFunction extends RichParallelSourceFunction<Tweet> {

  private volatile boolean running = true;
  private Random random = new Random();

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void run(SourceContext ctx) throws Exception {
    while (running) {
      long tweetId = Math.abs(random.nextLong() % 10);
      ctx.collect(new Tweet(tweetId));
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
