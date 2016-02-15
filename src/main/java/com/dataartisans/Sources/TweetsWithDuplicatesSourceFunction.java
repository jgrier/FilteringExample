package com.dataartisans.sources;

import com.dataartisans.domain.TweetImpression;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class TweetsWithDuplicatesSourceFunction extends RichParallelSourceFunction<TweetImpression> {

  private static final int NUM_DUPLICATES = 10;
  private static final int NUM_TWEETS = 10;
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
    // Just generate random tweets every second in the [range 1 - NUM_TWEETS]
    while (running) {
      Thread.sleep(1000);
      for(int i=0; i<NUM_DUPLICATES; i++) {
        ctx.collect(new TweetImpression((Math.abs(random.nextLong()) % NUM_TWEETS) + 1));
      }
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
