package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallbackWithKeys {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerCallbackWithKeys.class);

        //Default Params
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        //Create Producer Properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for(int i =0; i < 10; i ++)
        {
            String key = "Key_" + Integer.toString(i);
            String value = "Hello World " + Integer.toString(i);
            //Create the ProducerRecord
            ProducerRecord<String, String> record
                    = new ProducerRecord<String, String>(topic, key, value);
            //Send - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        logger.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n"+
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing : ", e);
                    }
                }
            });
        }

        //Close : along with flush
        producer.flush();
        producer.close();
    }
}
