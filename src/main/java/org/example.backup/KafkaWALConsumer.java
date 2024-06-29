package org.example.backup;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaWALConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaWALConsumer.class);

    public static void main(String[] args) {
        // Kafka consumer configuration settings
        String topic = "your-topic";  // Replace with your actual topic
        String brokers = "localhost:9092";  // Replace with your actual brokers

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaWALConsumerGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Kafka Consumer subscribes to the list of topics here.
        consumer.subscribe(Collections.singletonList(topic));

        // Print the topic name
        log.info("Subscribed to topic {}", topic);

        // Infinite loop to keep the consumer listening for new messages
        while (true) {
            // Poll the Kafka broker for new messages
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                // Print the offset, key, and value for each record
                log.info("Offset = {}, Key = {}, Value = {}", record.offset(), record.key(), record.value());
            }
            try {
                log.info("KafkaWALConsumer is sleeping for 1 second");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
    }
    }
}
