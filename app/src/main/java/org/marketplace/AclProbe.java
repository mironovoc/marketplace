package org.marketplace;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicAuthorizationException;

final class AclProbe {
    private AclProbe() {
    }

    static void proveShopCannotWriteCleanTopic() throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(ShopProducer.producerProperties("shop"))) {
            producer.send(new ProducerRecord<>("shop.products.clean", "hack", "{\"product_id\":\"hack\"}")).get();
        } catch (Exception exception) {
            if (causedByAuthorization(exception)) {
                System.out.println("OK: магазину запрещена запись в shop.products.clean");
                return;
            }
            throw exception;
        }
        throw new IllegalStateException("магазин смог записать в shop.products.clean, ACL не сработал");
    }

    private static boolean causedByAuthorization(Throwable throwable) {
        while (throwable != null) {
            if (throwable instanceof TopicAuthorizationException) {
                return true;
            }
            String message = throwable.getMessage();
            if (message != null && message.contains("TOPIC_AUTHORIZATION_FAILED")) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }
}
