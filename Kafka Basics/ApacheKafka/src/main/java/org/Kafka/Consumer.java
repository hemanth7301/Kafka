package org.Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String topic = "wikimedia.recentchange";

        KafkaConsumer<String, String> consumer = getKafkaConsumer();

        consumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: {}, Value: {}", record.key(), record.value());
                log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
            }
        }
    }

    private static KafkaConsumer<String, String> getKafkaConsumer() {
        Properties properties = new Properties();

        String groupID = "wikimedia";

        properties.setProperty("bootstrap.servers", "awaited-rattler-9154-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"YXdhaXRlZC1yYXR0bGVyLTkxNTQkh9T9KzPa99cEuVJDM-b1jv1CGo9Tul2zTLQ\"password=\"Nzk4NzljZmEtODM3Zi00MjRiLWJkYjYtZDIwMDU4YzcwY2Y3\";");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupID);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        return new KafkaConsumer<>(properties);
    }
}

