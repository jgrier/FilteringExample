package com.dataartisans.domain;

/**
 * This class represents a TweetImpression that is being delivered to a particular customer.
 */
public class CustomerImpression {
  private Customer customer;
  private TweetImpression tweetImpression;

  public CustomerImpression(){
    customer = null;
    tweetImpression = null;
  }

  public CustomerImpression(Customer customer, TweetImpression tweet){
    this.customer = customer;
    this.tweetImpression = tweet;
  }

  public Customer getCustomer() {
    return customer;
  }

  public TweetImpression getTweetImpression() {
    return tweetImpression;
  }

  public void setCustomer(Customer customer) {
    this.customer = customer;
  }

  public void setTweetImpression(TweetImpression tweetImpression) {
    this.tweetImpression = tweetImpression;
  }

  @Override
  public String toString() {
    return String.format("CustomerImpression(%s, %s)", getCustomer(), getTweetImpression());
  }
}
