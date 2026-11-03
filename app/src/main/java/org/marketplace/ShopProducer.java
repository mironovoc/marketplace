package org.marketplace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Path;
import java.util.Properties;

final class ShopProducer {
    private ShopProducer() {
    }

    static void sendAll() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Path file = Ssl.root().resolve("data").resolve("products.json");
        JsonNode products = mapper.readTree(file.toFile());
        Properties properties = producerProperties("shop");
        String topic = "shop.products";
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (JsonNode product : products) {
                String id = product.get("product_id").asText();
                producer.send(new ProducerRecord<>(topic, id, mapper.writeValueAsString(product))).get();
                System.out.println("отправлен товар " + id + " " + product.get("name").asText());
            }
        }
    }

    static Properties producerProperties(String user) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Ssl.bootstrap(
                "BOOTSTRAP_A", "localhost:19092,localhost:19094,localhost:19096"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        properties.put(ProducerConfig.RETRIES_CONFIG, "10");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "20000");
        Ssl.apply(properties, user);
        return properties;
    }
}
