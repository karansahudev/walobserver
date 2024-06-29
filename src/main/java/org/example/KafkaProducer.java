package org.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Properties;


public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private static final String topic = "test";
    private static final String kafkaBrokers = "localhost:9092";




    public static void main(String[] args) throws IOException {
        log.info("Starting KafkaWALObserver initialization");
        try {
            log.info("Configured topic: (3 and brokers: ()", topic, kafkaBrokers);
            log.info("Loaded JSON schema successfully");
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaWALObserverProducer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            org.apache.kafka.clients.producer.KafkaProducer<Object, Object> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

            while (true){
                Thread.sleep(1000);
                log.info("producing ");
                producer.send(
                        new org.apache.kafka.clients.producer.ProducerRecord<>(topic, "Hello, Kafka!"));
                log.info("Kafka producer initialized successfully");
                log.info("KafkaWALObserver started successfully");
            }


        } catch (Exception e) {
            log.error("Error initializing KafkaWALObserver", e);
        }
    }
}

