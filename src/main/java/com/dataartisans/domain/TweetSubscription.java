package com.dataartisans.domain;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * This POJO represents a request from a customer to have impressions on the given
 * tweet delivered to them.
 */
public class TweetSubscription {
  private Customer customer;
  private long tweetId;

  public TweetSubscription(){
    customer = null;
    tweetId = -1;
  }

  public TweetSubscription(Customer customer, long tweetId){

    this.customer = customer;
    this.tweetId = tweetId;
  }

  public void setCustomerId(Customer customer) {
    this.customer = customer;
  }

  public Customer getCustomer() {
    return customer;
  }

  public void setTweetId(long tweetId) {
    this.tweetId = tweetId;
  }

  public long getTweetId() {
    return tweetId;
  }

  public static KeySelector<TweetSubscription, Long> getKeySelector() {
    return new KeySelector<TweetSubscription, Long>() {
      @Override
      public Long getKey(TweetSubscription filter) throws Exception {
        return filter.getTweetId();
      }
    };
  }

  @Override
  public String toString() {
    return String.format("TweetSubscription(%s, %d)", customer, tweetId);
  }
}
