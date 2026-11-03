package org.marketplace;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class AlertLogger {
    private AlertLogger() {
    }

    static void serve() throws Exception {
        Path log = Ssl.root().resolve("evidence").resolve("alerts.log");
        Files.createDirectories(log.getParent());
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 9098), 0);
        server.createContext("/alerts", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String text = new String(body, StandardCharsets.UTF_8);
            Files.writeString(log, text + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("получено оповещение, байт: " + body.length);
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        System.out.println("приёмник оповещений слушает :9098");
        Thread.currentThread().join();
    }
}
