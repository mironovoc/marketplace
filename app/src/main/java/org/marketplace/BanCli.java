package org.marketplace;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

final class BanCli {
    private static final String TOPIC = "shop.banned";

    private BanCli() {
    }

    static void run(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("ban add|remove|list");
        }
        switch (args[1]) {
            case "add" -> send(args[2], "banned");
            case "remove" -> send(args[2], null);
            case "list" -> list();
            default -> throw new IllegalArgumentException(args[1]);
        }
    }

    private static void send(String productId, String value) throws Exception {
        Properties properties = ShopProducer.producerProperties("admin");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(TOPIC, productId, value)).get();
        }
        if (value == null) {
            System.out.println("товар " + productId + " убран из запрещённых");
        } else {
            System.out.println("товар " + productId + " добавлен в запрещённые");
        }
    }

    private static void list() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Ssl.bootstrap(
                "BOOTSTRAP_A", "localhost:19092,localhost:19094,localhost:19096"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ban-list-" + System.currentTimeMillis());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        Ssl.apply(properties, "admin");
        Map<String, String> banned = new LinkedHashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(TOPIC));
            long quietSince = System.currentTimeMillis();
            boolean saw = false;
            while (System.currentTimeMillis() - quietSince < 4000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    if (saw) {
                        break;
                    }
                    continue;
                }
                saw = true;
                quietSince = System.currentTimeMillis();
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() == null) {
                        banned.remove(record.key());
                    } else {
                        banned.put(record.key(), record.value());
                    }
                }
            }
        }
        if (banned.isEmpty()) {
            System.out.println("запрещённых товаров нет");
            return;
        }
        banned.keySet().forEach(id -> System.out.println(id));
    }
}
