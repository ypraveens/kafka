package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerGroupsWithThread {
    public static void main(String[] args) {
        new ConsumerGroupsWithThread().run();
    }

    public ConsumerGroupsWithThread() {
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerGroupsWithThread.class.getName());

        //Default Params
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-thread-app";
        String topic = "first_topic";

        //latch for dealing multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the Consumer thread");
        //Create the consumer runnable
        Runnable myConsumerThread = new ConsumerThread(
                bootstrapServer, topic, groupId, latch);

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }  ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private String m_booststrapserver;
        private String m_topic;
        private String m_groupid;

        private CountDownLatch m_latch;
        private KafkaConsumer<String, String> m_consumer;

        private Logger m_logger = LoggerFactory.getLogger(ConsumerThread.class.getName());


        public ConsumerThread(String bootstrapServer
                , String topic
                , String groupId
                ,CountDownLatch latch) {
            this.m_booststrapserver = bootstrapServer;
            this.m_topic = topic;
            this.m_groupid = groupId;
            this.m_latch = latch;

            //Create Consumer Configs
            Properties prop = new Properties();
            prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_booststrapserver);
            prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, m_groupid);
            prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create Consumer
            m_consumer = new KafkaConsumer<String, String>(prop);
            //Subscribe consumer to our topic(s)
            m_consumer.subscribe(Arrays.asList(m_topic));
        }

        @Override
        public void run() {
            try {
                //poll for new data
                while(true) {
                    ConsumerRecords<String, String> records
                            = m_consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records) {
                        m_logger.info("key: " + record.key() + ", Value: " + record.value());
                        m_logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }
            catch (WakeupException e)
            {
                m_logger.info("Received shutdown signal!");
            }
            finally {
                m_consumer.close();
                //tell our main code we're done with consumer.
                m_latch.countDown();
            }
        }

        public void shutdown() {
            //the wakeup() is used to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            m_consumer.wakeup();
        }
    }
}
