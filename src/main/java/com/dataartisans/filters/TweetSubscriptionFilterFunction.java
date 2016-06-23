package com.dataartisans.filters;

import com.dataartisans.domain.Customer;
import com.dataartisans.domain.TweetSubscription;
import com.dataartisans.domain.CustomerImpression;
import com.dataartisans.domain.TweetImpression;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;

/**
 * This function consumes a connected stream.  The two individual streams are a stream of TweetImpressions
 * and a stream of TweetSubscriptions.  The TweetSubscription indicates that a customer would
 * like to receive TweetImpressions for the given tweet.  For each TweetImpression consumed this function
 * emits a message for *each* customer that has subscribed to that tweet.
 */
public class TweetSubscriptionFilterFunction extends RichCoFlatMapFunction<TweetImpression, TweetSubscription, CustomerImpression> {

  private static Logger LOG = LoggerFactory.getLogger(TweetSubscriptionFilterFunction.class);

  private ListStateDescriptor<Customer> tweetSubscriptionsStateDesc =
    new ListStateDescriptor<>("tweetSubscriptions", Customer.class);

  @Override
  public void flatMap1(TweetImpression impression, Collector<CustomerImpression> out) throws Exception {
    Iterable<Customer> customers = getRuntimeContext().getListState(tweetSubscriptionsStateDesc).get();
    if (customers != null) {
      for (Customer customer : customers) {
        out.collect(new CustomerImpression(customer, impression));
      }
    }
  }

  @Override
  public void flatMap2(TweetSubscription subscription, Collector<CustomerImpression> out) throws Exception {
    ListState<Customer> subscriptionState = getRuntimeContext().getListState(tweetSubscriptionsStateDesc);

    boolean customerPresent = false;
    for(Customer c : subscriptionState.get()){
      if(c.equals(subscription.getCustomer())){
        customerPresent = true;
      }
    }

    if(!customerPresent){
      subscriptionState.add(subscription.getCustomer());
      LOG.info("Sub-task {}: Enabling delivery of impression for Tweet({}) to {}", getRuntimeContext().getIndexOfThisSubtask() + 1, subscription.getTweetId(), subscription.getCustomer());
    }
  }
}
