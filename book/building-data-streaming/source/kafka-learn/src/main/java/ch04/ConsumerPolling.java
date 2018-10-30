package ch04;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import util.SimpleLogger;

/**
 * @author zacconding
 * @Date 2018-10-30
 * @GitHub : https://github.com/zacscoding
 */
public class ConsumerPolling {

    private static final CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(10);
    private static final String TOPIC = "consumer-test1";
    private static final String BOOTSTRAP_SERVERS = "192.168.5.78:9092";

    public static void main(String[] args) {
        Thread t = new Thread(() -> {
            Properties producerProps = new Properties();
            // producerProps.put("bootstrap.servers", "broker1:port,broker2:port");
            producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
            int count = (int) COUNT_DOWN_LATCH.getCount();
            for (int i = 0; i < count; i++) {
                // 키를 생성하지 않고 데이터를 생성
                ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, "Hello this is record " + i);
                Future<RecordMetadata> recordMetadata = producer.send(data);
                System.out.println("Produce : " + data);
            }

            producer.close();
        });
        t.setDaemon(true);
        t.start();

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumerProperties.put("group.id", "Demo_Group");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList(TOPIC));

        // 동기 커밋
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(2);
            for (ConsumerRecord<String, String> record : records) {
                SimpleLogger.println("offset = {}, key = {}, value : {}", record.offset(), record.key(), record.value());
                COUNT_DOWN_LATCH.countDown();
            }

            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                SimpleLogger.error("Failed to commit", e);
            }

            if (COUNT_DOWN_LATCH.getCount() <= 0) {
                break;
            }
        }

        // 비동기 커밋
        while(!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, String> records = consumer.poll(2);
            for (ConsumerRecord<String, String> record : records) {
                SimpleLogger.println("offset = {}, key = {}, value : {}", record.offset(), record.key(), record.value());
                COUNT_DOWN_LATCH.countDown();
            }

            try {
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {

                    }
                });
            } catch (CommitFailedException e) {
                SimpleLogger.error("Failed to commit", e);
            }
        }
    }

}
