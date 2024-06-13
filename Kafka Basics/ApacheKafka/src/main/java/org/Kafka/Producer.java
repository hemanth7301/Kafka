package org.Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");
        KafkaProducer<String, String> producer = getKafkaProducer();

        for (int i = 1; i <= 3; i++) {
            for (int j = 1; j <= 10; j++) {
                String topic = "wikimedia.recentchange";
                String key = "id_" + i;
                String value = "Hello World " + j;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        log.info("Key: {} | Partition: {}", key, recordMetadata.partition());
                    } else {
                        log.error("Oops!!! Something gone wrong", e);
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.flush();
        producer.close();
    }

    private static KafkaProducer<String, String> getKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "awaited-rattler-9154-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"YXdhaXRlZC1yYXR0bGVyLTkxNTQkh9T9KzPa99cEuVJDM-b1jv1CGo9Tul2zTLQ\"password=\"Nzk4NzljZmEtODM3Zi00MjRiLWJkYjYtZDIwMDU4YzcwY2Y3\";");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}

