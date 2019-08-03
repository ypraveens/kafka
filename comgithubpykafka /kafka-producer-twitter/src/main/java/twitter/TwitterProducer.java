package twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = "fLnNsoIXsJnxNNOePZaat0YWq";
    String consumerSecret = "cpgd5dINx2B6Dj8ZHiM37FbiZIegLexbQnjXffARDsF2MCpV21";
    String token = "1157277137046507520-7mlNUsmHWBn7AKO0he9E3bKoDU2spp";
    String secret = "T4xJwOhqmsoWB1ayIhe7i5lKf4lY1W41SHjOBJkSElae1";

    String topic = "twitter_tweets";
    String bootstrapServer = "127.0.0.1:9092";
    String twitter_search_topic = "kafka-streaming";

    public TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Run Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue
                <String>(100000);

        List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport");/*twitter_search_topic*/
        //Create a twitter client
        Client client = createTwitterClient(msgQueue, terms);
        // Attempts to establish a connection.
        client.connect();


        //Create a Kafka Producer
        KafkaProducer<String, String> producer = createKafkaProducer(bootstrapServer);

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread ( () -> {
            logger.info("Stopping application...");
            logger.info("shutting down client from twitter");
            client.stop();
            logger.info("closing producer");
            producer.close();
            logger.info("Done!");
        }));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {
                logger.info(msg);

                //Create the ProducerRecord
                ProducerRecord<String, String> record
                        = new ProducerRecord<>(topic, null, msg);
                //Send
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            logger.error("Somethign bad happened - ", e);
                        }
                    }
                });
            }
        }
        logger.info("End of Twitter producer application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue
            , List<String> terms) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-Producer-Client")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    KafkaProducer<String, String> createKafkaProducer(String bootstrapServer) {
        //Create Producer Properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create a safe producer - idempotent
        prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE) );
        //Kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.
        prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //High throughput producer (at the expense of a bit of latency and CPU usage)
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        return producer;
    }
}
