package org.marketplace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

final class LakeLoader {
    private LakeLoader() {
    }

    static void load() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Ssl.bootstrap("BOOTSTRAP_B", "localhost:19192"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "lake-loader");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        Ssl.apply(properties, "lake");
        ObjectMapper mapper = new ObjectMapper();
        WebHdfs hdfs = new WebHdfs();
        hdfs.mkdirs("/data");
        hdfs.mkdirs("/data/products");
        int written = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of("primary.shop.products.clean"));
            long assignDeadline = System.currentTimeMillis() + 20000;
            while (consumer.assignment().isEmpty() && System.currentTimeMillis() < assignDeadline) {
                consumer.poll(Duration.ofMillis(500));
            }
            if (!consumer.assignment().isEmpty()) {
                consumer.seekToBeginning(consumer.assignment());
            }
            long deadline = System.currentTimeMillis() + 60000;
            boolean saw = false;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    if (saw) {
                        break;
                    }
                    continue;
                }
                saw = true;
                for (ConsumerRecord<String, String> record : records) {
                    JsonNode product = mapper.readTree(record.value());
                    String id = product.path("product_id").asText(record.key());
                    hdfs.put("/data/products/" + id + ".json", record.value());
                    written++;
                    System.out.println("HDFS /data/products/" + id + ".json");
                }
            }
        }
        System.out.println("в Data Lake записано товаров: " + written);
        if (written == 0) {
            throw new IllegalStateException("второй кластер ещё не отдал товары");
        }
    }
}
