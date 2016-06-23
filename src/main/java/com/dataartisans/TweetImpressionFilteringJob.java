package com.dataartisans;

import com.dataartisans.domain.Customer;
import com.dataartisans.domain.CustomerImpression;
import com.dataartisans.domain.TweetImpression;
import com.dataartisans.filters.TweetSubscriptionFilterFunction;
import com.dataartisans.filters.DedupeFilterFunction;
import com.dataartisans.sinks.CustomerSinkFunction;
import com.dataartisans.sources.TweetSourceFunction;
import com.dataartisans.domain.TweetSubscription;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This job consumes a stream of TweetImpressions and a stream of TweetSubscriptions from customers.
 * There is an output sink for each customer and each customer receives only those TweetImpressions for
 * tweets that they have subscribed to.
 * <p>
 * The tweet subscriptions are read from a socket.  The format of the messages is just:
 * <customer_name> <tweetId>
 * <p>
 * For example:
 * Google 5
 * Indicates that customer "Google" wants to receive impressions data for the tweet with id "5"
 * <p>
 * To run this example pass the host name and port of the socket server where the subscription
 * messages can be read from.  For example:
 * <p>
 * flink run -c com.dataartisans.TweetImpressionFilteringJob <jar_file> --host localhost --port 9999
 * <p>
 * A great way to test this is to use netcat to provide the socket server.  For example:
 * <p>
 * nc -lk 9999
 * <p>
 * Then type messages to the console to enable TweetSubscriptions in the format described above
 * <p>
 * Also note the set of possible customers is fixed.  Customer must be one of:
 * <p>
 * Google, Twitter, Facebook, Apple, Amazon
 */
public class TweetImpressionFilteringJob {

  private static Logger LOG = LoggerFactory.getLogger(TweetImpressionFilteringJob.class);

  private static long DEDUPE_CACHE_EXPIRATION_TIME_MS = 1_000;

  public static void main(String[] args) throws Exception {

    // Parse command line parameters
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String host = parameterTool.getRequired("host");
    int port = Integer.valueOf(parameterTool.getRequired("port"));

    // Setup the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(1000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000));
    env.setParallelism(1);
    
    // Stream of updates to subscriptions, partitioned by tweetId, read from socket
    DataStream<TweetSubscription> filterUpdateStream = env.socketTextStream(host, port)
      .map(stringToTweetSubscription())
      .keyBy(TweetSubscription.getKeySelector());

    // TweetImpression stream, partitioned by tweetId
    DataStream<TweetImpression> tweetStream = env.addSource(new TweetSourceFunction(false), "TweetImpression Source")
      .keyBy(TweetImpression.getKeySelector());

    // Run the tweet impressions past the filters and emit those that customers have requested
    DataStream<CustomerImpression> filteredStream = tweetStream
      .connect(filterUpdateStream)
      .flatMap(new TweetSubscriptionFilterFunction());

    // Create a seperate sink for each customer
    DataStreamSink<CustomerImpression>[] customerSinks = setupCustomerSinks(filteredStream);

    // Run it
    env.execute();
  }

  private static MapFunction<String, TweetSubscription> stringToTweetSubscription() {
    return new MapFunction<String, TweetSubscription>() {
      @Override
      public TweetSubscription map(String value) throws Exception {
        String[] splits = value.split("\\W+");
        return new TweetSubscription(new Customer(splits[0]), Long.valueOf(splits[1]));
      }
    };
  }

  private static String customerStreamName(Customer customer) {
    return String.format("customer-%s", customer.getName());
  }

  private static OutputSelector<CustomerImpression> customerOutputSelector() {
    return new OutputSelector<CustomerImpression>() {
      @Override
      public Iterable<String> select(CustomerImpression msg) {
        return Lists.newArrayList(customerStreamName(msg.getCustomer()));
      }
    };
  }

  private static DataStreamSink<CustomerImpression>[] setupCustomerSinks(DataStream<CustomerImpression> msgStream) {
    // Split the stream into multiple streams by customer Id
    SplitStream<CustomerImpression> splitStream = msgStream.split(customerOutputSelector());

    // Tie a separate sink to each fork of the split stream
    List<Customer> customers = Customer.getAllCustomers();
    DataStreamSink<CustomerImpression>[] customerSinks = new DataStreamSink[customers.size()];

    int i = 0;
    for (Customer customer : customers) {
      customerSinks[i++] = splitStream
        .select(customerStreamName(customer))
        .addSink(new CustomerSinkFunction(customer));
    }
    return customerSinks;
  }

}
