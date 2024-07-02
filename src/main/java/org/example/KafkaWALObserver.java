package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;


public class KafkaWALObserver implements WALCoprocessor, WALObserver {
    Logger log = LoggerFactory.getLogger(KafkaWALObserver.class);
    private KafkaProducer<String, String> producer;
    private String topic;

    private final ObjectMapper mapper = new ObjectMapper();
    private JsonNode jsonSchema;

    private String loadSchemaFromHDFS(String path) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Path filePath = new Path(path);
        try (FSDataInputStream inputStream = fs.open(filePath)) {
            // Use ByteArray Output stream to read data from input stream
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nRead;
            byte[] data = new byte[16384]; // Buffer size can be adjusted according to needs
            while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }
            buffer.flush();
            return buffer.toString(StandardCharsets.UTF_8.name());
        }
    }

//    static Configuration configuration;
//    public static void main(String[] args) {
//        configuration= new Configuration();
//        configuration.set("kafka.topic", "test");
//        configuration.set("kafka.brokers", "localhost:9092");
//
//        KafkaWALObserver observer = new KafkaWALObserver();
//        observer.start(null);
//    }

    @Override
    public void start(CoprocessorEnvironment env) {
        log.info("Starting KafkaWALObserver initialization");
        try {
            Configuration conf = env.getConfiguration();
            this.topic = conf.get("kafka.topic", "hbase-mutations");
            String kafkaBrokers = conf.get("kafka.brokers", "localhost:9092");
            String schemaPath = conf.get("schema.path", "/usr");

            log.info("Configured topic: {} and brokers: {}", topic, kafkaBrokers);
            String schemaJson = loadSchemaFromHDFS(schemaPath);
            this.jsonSchema = mapper.readTree(schemaJson);
            log.info("Loaded JSON schema successfully");
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaWALObserverProducer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
            log.info("Kafka producer initialized successfully");
            log.info("KafkaWALObserver started successfully");
        } catch (Exception e) {
            log.error("Error initializing KafkaWALObserver", e);
        }
    }


    @Override
    public void stop(CoprocessorEnvironment env) {
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public Optional<WALObserver> getWALObserver() {
        return Optional.of(this);
    }

@Override
public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx, RegionInfo info, WALKey logkey, WALEdit logEdit)  {
    log.info("postWAlWrite triggered - WALKey: {}, Number of Cells: {}", logkey, logEdit.getCells().size());
    for (Cell cell : logEdit.getCells()) {
        String family = Bytes.toString(CellUtil.cloneFamily(cell));
        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
        String value = Bytes.toString(CellUtil.cloneValue(cell));
        String rowkey = Bytes.toStringBinary(CellUtil.cloneRow(cell));
        log.info("Cell Details - Rowkey: {}, Family: {}, Qualifier: {}, Value: {}, Timestamp: {}", rowkey, family, qualifier, value, LocalDateTime.now());
        try{
            String json = convertCellToJsonBasedOnSchema(cell, jsonSchema);
            log.info("JSON Conversion Result: {}", json);

            log.info("Sending to Kafka - Topic: {}, Key: {}, JSON: {}", topic, logkey, json);
            Future<RecordMetadata> metadata = producer.send(new ProducerRecord<>(topic, rowkey, json));
            log.info("Metadata: {}", metadata.get());
        } catch (Exception e) {
                log. error ("Error converting cell to JSON or sending to Kafka", e);
        }
    }
}

    private String convertCellToJsonBasedOnSchema(Cell cell, JsonNode schema) throws IOException {
        String cellQualifier = Bytes.toStringBinary(CellUtil.cloneQualifier(cell));
        String cellValue = Bytes.toStringBinary(CellUtil.cloneValue(cell));

        // Retrieve the schema for the entityType
        JsonNode entityTypeSchema = schema.path("properties").path("attributes").path("properties").path("entityType");
        JsonNode hbaseFields = entityTypeSchema.path("hbase_fields");

        // Use LinkedHashMap to maintain the order of insertion
        Map<String, String> values = new LinkedHashMap<>();

        // Check if the current cell's qualifier is one of the relevant HBase fields for entityType
        if (hbaseFields.isArray()) {
            for (JsonNode field : hbaseFields) {
                String fieldName = field.asText();
                if (cellQualifier.equals(fieldName)) {
                    // Since 'entityType' should be determined by the order and the presence of fields,
                    // we store the first matched field's value and break
                    values.put("entityType", cellValue);
                    break;
                }
            }
        }

        // Return the JSON string if 'entityType' was set; otherwise return an empty JSON object
        if (values.isEmpty()) {
            return mapper.writeValueAsString(values);
        } else {
            return "{}";
        }
    }

}
