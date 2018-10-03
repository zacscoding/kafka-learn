package ch03;

import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author zacconding
 * @Date 2018-10-03
 * @GitHub : https://github.com/zacscoding
 */
public class ProducerCreate {

    public static void main(String[] args) {

    }

    public static KafkaProducer<String, String> createKafkaProducerExample() {
        Properties producerProps = new Properties();
        // producerProps.put("bootstrap.servers", "broker1:port,broker2:port");
        producerProps.put("bootstrap.servers", "192.168.5.78:9092");

        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);

        return producer;
    }

    public static void createProducerRecordExample() {
        KafkaProducer<String, String> producer = createKafkaProducerExample();

        String topicName = "topic";
        String data = "Temp message";

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, data);
        // Future<RecordMetadata> recordMetadata = producer.send(producerRecord);

        producer.send(producerRecord, (recordMetadata, ex) -> {
            if (ex != null) {
                // deal with exception
            } else {
                // deal with RecordMetadata
            }
        });

    }
}
