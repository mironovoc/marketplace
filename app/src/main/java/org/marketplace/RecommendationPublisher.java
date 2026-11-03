package org.marketplace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

final class RecommendationPublisher {
    private RecommendationPublisher() {
    }

    static void publish() throws Exception {
        WebHdfs hdfs = new WebHdfs();
        JsonNode listing = hdfs.list("/data/recommendations");
        ObjectMapper mapper = new ObjectMapper();
        int sent = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(ShopProducer.producerProperties("spark"))) {
            for (JsonNode status : listing.path("FileStatuses").path("FileStatus")) {
                String name = status.path("pathSuffix").asText();
                if (!name.endsWith(".json") || name.startsWith("_")) {
                    continue;
                }
                String body = hdfs.open("/data/recommendations/" + name);
                for (String line : body.split("\\R")) {
                    if (line.isBlank()) {
                        continue;
                    }
                    JsonNode row = mapper.readTree(line);
                    String id = row.path("product_id").asText();
                    producer.send(new ProducerRecord<>("recommendations", id, line)).get();
                    sent++;
                    System.out.println("рекомендация " + id + " " + row.path("name").asText());
                }
            }
        }
        System.out.println("в топик recommendations записано: " + sent);
        if (sent == 0) {
            throw new IllegalStateException("Spark не оставил рекомендаций в HDFS");
        }
    }
}
