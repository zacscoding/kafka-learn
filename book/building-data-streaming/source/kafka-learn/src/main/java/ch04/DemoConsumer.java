package ch04;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
public class DemoConsumer {

    private static final String BOOTSTRAP_SERVERS = "192.168.5.78:9092";
    private static final String TOPIC = "consumer-test2";
    private static final CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(10);


    public static void main(String[] args) {
        Thread startProducer = new Thread(() -> startProducer());
        startProducer.setDaemon(true);
        startProducer.start();

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumerProperties.put("group.id", "Demo_Group");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("session.timeout.ms", "30000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));

        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(500);
                for (ConsumerRecord<String, String> record : records) {
                    SimpleLogger.println("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                    COUNT_DOWN_LATCH.countDown();
                }

                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                        if (e != null) {
                            SimpleLogger.println("Exception occur while commit offset", e);
                        }
                    }
                });

                if (COUNT_DOWN_LATCH.getCount() < 1) {
                    break;
                }
            }
        } catch (Exception e) {
            SimpleLogger.error("Exception occur while consume", e);
        } finally {
            try {
                kafkaConsumer.commitSync();
            } finally {
                kafkaConsumer.close();
            }
        }
    }

    private static void startProducer() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
        long count = COUNT_DOWN_LATCH.getCount();
        Random random = new Random();

        try {
            for (int i = 0; i < count; i++) {
                TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
                // 키를 생성하지 않고 데이터를 생성
                ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, "Hello this is record " + i);
                Future<RecordMetadata> recordMetadata = producer.send(data);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}
