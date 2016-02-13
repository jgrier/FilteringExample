package com.dataartisans;

import org.apache.flink.api.java.functions.KeySelector;

public class Tweet {
  private long tweetId;

  public Tweet(){
    this(-1L);
  }

  public Tweet(long tweetId) {
    this.tweetId = tweetId;
  }

  public void setTweetId(long tweetId){
    this.tweetId = tweetId;
  }

  public long getTweetId(){
    return tweetId;
  }

  public static KeySelector<Tweet, Long> getKeySelector() {
    return new KeySelector<Tweet, Long>() {
      @Override
      public Long getKey(Tweet tweet) throws Exception {
        return tweet.tweetId;
      }
    };
  }

  @Override
  public String toString() {
    return "Tweet(" + tweetId + ")";
  }
}
