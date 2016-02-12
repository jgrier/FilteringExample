package com.dataartisans;

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

  @Override
  public String toString() {
    return "Tweet(" + tweetId + ")";
  }
}
