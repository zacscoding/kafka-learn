package ch03;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * - Create topic
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test1
 *
 * - Consume
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test
 */
public class DemoProducer {

    private static final String TOPIC = "test1";
    private static final String BOOTSTRAP_SERVERS = "192.168.5.78:9092";
    private static final int COUNT = 2000;
    private static final CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(COUNT);

    public static void main(String[] args) throws InterruptedException {
        startConsume();

        Properties producerProps = new Properties();
        // producerProps.put("bootstrap.servers", "broker1:port,broker2:port");
        producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("acks", "all");
        producerProps.put("retries", 1);
        producerProps.put("batch.size", 20000);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 24568545);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);

        for (int i = 0; i < COUNT; i++) {
            // 키를 생성하지 않고 데이터를 생성
            ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, "Hello this is record " + i);
            Future<RecordMetadata> recordMetadata = producer.send(data);
        }

        producer.close();
        COUNT_DOWN_LATCH.await(10, TimeUnit.SECONDS);
    }

    public static void startConsume() {
        Thread t = new Thread(() -> {
            Properties consumerProps = new Properties();
            // producerProps.put("bootstrap.servers", "broker1:port,broker2:port");
            consumerProps.put("bootstrap.servers", "192.168.5.78:9092");
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("group.id", "Demo_Group");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);

            consumer.subscribe(Arrays.asList(TOPIC));

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(500);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(
                        String.format("offset = %d, partition : %d, key : %s, value : %s", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value())
                    );
                    COUNT_DOWN_LATCH.countDown();
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
