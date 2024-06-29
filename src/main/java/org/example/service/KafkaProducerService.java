package org.example.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.config.KafkaProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerService {
    Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    protected <T> void produce(T message){
        log.info("Starting KafkaWALObserver initialization");
        try {
            KafkaProducerConfig config = KafkaProducerConfig.getConfigFromEnv();
            log.info("Configured topic: {} and brokers: {}", config.getTopic(), config.getBootstrapServers());
            log.info("Loaded JSON schema successfully");
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaWALObserverProducer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);
            log.info("producing {}  ", message);
            Future<RecordMetadata> metadata = producer.send(new ProducerRecord<>(config.getBootstrapServers(), message));
            log.info("Metadata: {}", metadata.get());
            log.info("Kafka producer initialized successfully");
            log.info("KafkaWALObserver started successfully");
        } catch (Exception e) {
            log.error("Error initializing KafkaWALObserver", e);
        }
    }
}
