package ch04;

import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author zacconding
 * @Date 2018-10-22
 * @GitHub : https://github.com/zacscoding
 */
public class ConsumerProperties {

    public static void main(String[] args) {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", "");
        consumerProperties.put("group.id", "Demo");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
    }

}
