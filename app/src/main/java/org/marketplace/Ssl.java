package org.marketplace;

import java.nio.file.Path;
import java.util.Properties;

final class Ssl {
    static final String PASSWORD = "your-password";

    private Ssl() {
    }

    static Path root() {
        String configured = System.getenv("MARKETPLACE_HOME");
        if (configured != null && !configured.isBlank()) {
            return Path.of(configured);
        }
        return Path.of(System.getProperty("user.dir"));
    }

    static String bootstrap(String envName, String fallback) {
        String value = System.getenv(envName);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value;
    }

    static void apply(Properties properties, String user) {
        Path secrets = root().resolve("secrets").resolve("clients");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.keystore.location", secrets.resolve(user + ".keystore.p12").toString());
        properties.put("ssl.keystore.password", PASSWORD);
        properties.put("ssl.keystore.type", "PKCS12");
        properties.put("ssl.key.password", PASSWORD);
        properties.put("ssl.truststore.location", secrets.resolve("truststore.jks").toString());
        properties.put("ssl.truststore.password", PASSWORD);
        properties.put("ssl.truststore.type", "JKS");
        properties.put("ssl.endpoint.identification.algorithm", "");
    }
}
