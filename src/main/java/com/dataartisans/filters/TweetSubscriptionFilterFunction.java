package com.dataartisans.filters;

import com.dataartisans.domain.Customer;
import com.dataartisans.domain.TweetSubscription;
import com.dataartisans.domain.CustomerImpression;
import com.dataartisans.domain.TweetImpression;
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
public class TweetSubscriptionFilterFunction extends RichCoFlatMapFunction<TweetSubscription, TweetImpression, CustomerImpression>
  implements CheckpointedAsynchronously<HashMap<Long, HashSet<Customer>>> {

  private static Logger LOG = LoggerFactory.getLogger(TweetSubscriptionFilterFunction.class);

  private HashMap<Long, HashSet<Customer>> tweetSubscriptions = new HashMap<>();

  @Override
  public void flatMap1(TweetSubscription subscription, Collector<CustomerImpression> out) throws Exception {
    HashSet<Customer> customers = getOrCreateCustomerSet(subscription.getTweetId());
    customers.add(subscription.getCustomer());
    LOG.info("Sub-task {}: Enabling delivery of impression for Tweet({}) to {}", getRuntimeContext().getIndexOfThisSubtask() + 1, subscription.getTweetId(), subscription.getCustomer());
  }

  @Override
  public void flatMap2(TweetImpression impression, Collector<CustomerImpression> out) throws Exception {
    HashSet<Customer> customers = getCustomerSet(impression.getTweetId());
    if (customers != null) {
      for (Customer customer : customers) {
        out.collect(new CustomerImpression(customer, impression));
      }
    }
  }

  @Override
  public HashMap<Long, HashSet<Customer>> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
    return deepCopy(tweetSubscriptions);
  }

  @Override
  public void restoreState(HashMap<Long, HashSet<Customer>> state) throws Exception {
    tweetSubscriptions = deepCopy(state);
  }

  private HashSet<Customer> getOrCreateCustomerSet(long tweetId) {
    HashSet<Customer> customers = getCustomerSet(tweetId);
    if (customers == null) {
      customers = new HashSet<>();
      tweetSubscriptions.put(tweetId, customers);
    }
    return customers;
  }

  private HashSet<Customer> getCustomerSet(long tweetId) {
    return tweetSubscriptions.get(tweetId);
  }


  private HashMap<Long, HashSet<Customer>> deepCopy(HashMap<Long, HashSet<Customer>> source) {
    HashMap<Long, HashSet<Customer>> copy = new HashMap();
    for(long tweetId : source.keySet()){
      copy.put(tweetId, new HashSet<Customer>());
      for(Customer customer : source.get(tweetId)){
        copy.get(tweetId).add(customer);
      }
    }
    return copy;
  }
}
