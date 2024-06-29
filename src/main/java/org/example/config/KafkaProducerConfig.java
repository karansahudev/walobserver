package org.example.config;

public class KafkaProducerConfig {

    private String bootstrapServers;
    private String topic;


    public KafkaProducerConfig(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    public KafkaProducerConfig() {
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }
    public String getTopic() {
        return topic;
    }
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    @Override
    public String toString() {
        return "KafkaProducerConfig{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }

    public static KafkaProducerConfig getConfigFromEnv() {
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String topic = System.getenv("TOPIC");
        return new KafkaProducerConfig(bootstrapServers, topic);
    }
    public static void main(String[] args) {
        KafkaProducerConfig config = KafkaProducerConfig.getConfigFromEnv();
        System.out.println(config);
    }
}
