package org.marketplace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

final class ClientCli {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ClientCli() {
    }

    static void run(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("client search|recommend");
        }
        if ("search".equals(args[1])) {
            if (args.length < 3) {
                throw new IllegalArgumentException("client search <название>");
            }
            search(args[2]);
            return;
        }
        if ("recommend".equals(args[1])) {
            recommend();
            return;
        }
        throw new IllegalArgumentException(args[1]);
    }

    private static void search(String query) throws Exception {
        ObjectNode event = MAPPER.createObjectNode();
        event.put("type", "search");
        event.put("query", query);
        event.put("at", Instant.now().toString());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(ShopProducer.producerProperties("client"))) {
            producer.send(new ProducerRecord<>("client.searches", query, MAPPER.writeValueAsString(event))).get();
        }
        System.out.println("запрос отправлен в Kafka, топик client.searches");
        Path file = Ssl.root().resolve("data").resolve("runtime").resolve("products-clean.jsonl");
        if (!Files.exists(file)) {
            System.out.println("хранилище ещё пустое: " + file);
            return;
        }
        int found = 0;
        for (String line : Files.readAllLines(file)) {
            int brace = line.indexOf('{');
            if (brace < 0) {
                continue;
            }
            JsonNode product = MAPPER.readTree(line.substring(brace));
            String name = product.path("name").asText("");
            if (name.toLowerCase().contains(query.toLowerCase())) {
                found++;
                System.out.println(product.path("product_id").asText() + " " + name + " " + product.path("price").path("amount").asText());
            }
        }
        System.out.println("найдено в файле хранилища: " + found);
    }

    private static void recommend() throws Exception {
        ObjectNode event = MAPPER.createObjectNode();
        event.put("type", "recommendations");
        event.put("user", "demo-user");
        event.put("at", Instant.now().toString());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(ShopProducer.producerProperties("client"))) {
            producer.send(new ProducerRecord<>("client.recommendation-requests", "demo-user", MAPPER.writeValueAsString(event))).get();
        }
        System.out.println("запрос рекомендаций отправлен в Kafka");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Ssl.bootstrap(
                "BOOTSTRAP_A", "localhost:19092,localhost:19094,localhost:19096"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "client-recommendations");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        Ssl.apply(properties, "client");
        List<String> lines = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of("recommendations"));
            long assignDeadline = System.currentTimeMillis() + 15000;
            while (consumer.assignment().isEmpty() && System.currentTimeMillis() < assignDeadline) {
                consumer.poll(Duration.ofMillis(400));
            }
            if (!consumer.assignment().isEmpty()) {
                consumer.seekToBeginning(consumer.assignment());
            }
            long quietSince = System.currentTimeMillis();
            boolean saw = false;
            while (System.currentTimeMillis() - quietSince < 8000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    if (saw && System.currentTimeMillis() - quietSince >= 2000) {
                        break;
                    }
                    continue;
                }
                saw = true;
                quietSince = System.currentTimeMillis();
                for (ConsumerRecord<String, String> record : records) {
                    lines.add(record.value());
                }
            }
        }
        if (lines.isEmpty()) {
            System.out.println("рекомендаций в топике пока нет");
            return;
        }
        int from = Math.max(0, lines.size() - 3);
        for (int i = from; i < lines.size(); i++) {
            System.out.println(lines.get(i));
        }
    }
}
