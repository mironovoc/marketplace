package org.marketplace;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

final class ProductFilterApp {
    private ProductFilterApp() {
    }

    static void start() throws Exception {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-filter");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Ssl.bootstrap(
                "BOOTSTRAP_A", "localhost:19092,localhost:19094,localhost:19096"));
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, Ssl.root().resolve("data").resolve("runtime").resolve("kafka-streams").toString());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "3");
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        properties.put("consumer.auto.offset.reset", "earliest");
        properties.put("producer.acks", "all");
        Ssl.apply(properties, "streams");

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> banned = builder.table(
                "shop.banned",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.with(Serdes.String(), Serdes.String()));
        builder.stream("shop.products", Consumed.with(Serdes.String(), Serdes.String()))
                .leftJoin(banned, (product, ban) -> ban == null ? product : null)
                .filter((id, product) -> product != null)
                .to("shop.products.clean", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        System.out.println("фильтр запущен: shop.products -> shop.products.clean");
        Thread.currentThread().join();
    }
}
