package tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        //final Logger logger = LoggerFactory.getLogger(Producer.class);

        //Default Params
        String bootstrapServer = "127.0.0.1:9092";

        //Create Producer Properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        //Create the ProducerRecord
        ProducerRecord<String, String> record
                = new ProducerRecord<String, String>("first_topic", "Hello World!");
        //Send
        producer.send(record);
        //Close : along with flush
        producer.flush();
        producer.close();
    }
}
