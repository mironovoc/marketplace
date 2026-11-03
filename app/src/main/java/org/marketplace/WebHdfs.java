package org.marketplace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

final class WebHdfs {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
    private final String base;

    WebHdfs() {
        String configured = System.getenv("WEBHDFS");
        this.base = (configured == null || configured.isBlank())
                ? "http://localhost:9870/webhdfs/v1"
                : configured;
    }

    void mkdirs(String path) throws Exception {
        URI uri = URI.create(base + path + "?op=MKDIRS&user.name=root");
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(uri).PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("mkdirs " + path + " -> " + response.statusCode() + " " + response.body());
        }
    }

    void put(String path, String body) throws Exception {
        URI create = URI.create(base + path + "?op=CREATE&overwrite=true&user.name=root");
        HttpResponse<byte[]> first = http.send(
                HttpRequest.newBuilder(create).PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        if (first.statusCode() / 100 == 2) {
            return;
        }
        if (first.statusCode() != 307) {
            throw new IllegalStateException("create " + path + " -> " + first.statusCode() + " " + new String(first.body(), StandardCharsets.UTF_8));
        }
        URI location = URI.create(first.headers().firstValue("location").orElseThrow());
        HttpResponse<byte[]> second = http.send(
                HttpRequest.newBuilder(location).PUT(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        if (second.statusCode() / 100 != 2) {
            throw new IllegalStateException("put " + path + " -> " + second.statusCode());
        }
    }

    JsonNode list(String path) throws Exception {
        URI uri = URI.create(base + path + "?op=LISTSTATUS&user.name=root");
        HttpResponse<String> response = sendFollowing(uri, "GET", null);
        return MAPPER.readTree(response.body());
    }

    String open(String path) throws Exception {
        URI uri = URI.create(base + path + "?op=OPEN&user.name=root");
        return sendFollowing(uri, "GET", null).body();
    }

    private HttpResponse<String> sendFollowing(URI uri, String method, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri);
        if ("PUT".equals(method)) {
            builder.PUT(body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        } else {
            builder.GET();
        }
        HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 307) {
            URI location = URI.create(response.headers().firstValue("location").orElseThrow());
            return sendFollowing(location, method, body);
        }
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException(method + " " + uri + " -> " + response.statusCode() + " " + response.body());
        }
        return response;
    }
}
