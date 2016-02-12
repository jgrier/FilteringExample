package com.dataartisans;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DedupeFilterFunction extends RichFilterFunction<Tweet> implements CheckpointedAsynchronously<HashSet<Long>> {

  private LoadingCache<Long, Boolean> tweetCache;

  @Override
  public void open(Configuration parameters) throws Exception {
    createTweetCache();
  }


  @Override
  public boolean filter(Tweet value) throws Exception {
    boolean seen = tweetCache.get(value.getTweetId());
    if (!seen) {
      tweetCache.put(value.getTweetId(), true);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public HashSet<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
    return new HashSet<>(tweetCache.asMap().keySet());
  }

  @Override
  public void restoreState(HashSet<Long> state) throws Exception {
    createTweetCache();
    for (Long l : state) {
      tweetCache.put(l, true);
    }
  }

  private void createTweetCache() {
    tweetCache = CacheBuilder.newBuilder()
      .expireAfterWrite(10, TimeUnit.SECONDS)
      .build(new CacheLoader<Long, Boolean>() {
        @Override
        public Boolean load(Long key) throws Exception {
          return false;
        }
      });
  }
}
