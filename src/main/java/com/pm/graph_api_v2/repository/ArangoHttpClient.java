package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.ArangoProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "ARANGODB")
public class ArangoHttpClient {

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ArangoProperties properties;
    private final ObjectMapper objectMapper;

    public ArangoHttpClient(ArangoProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> get(String path) {
        return request("GET", path, null, false);
    }

    public Map<String, Object> post(String path, Object body) {
        return request("POST", path, body, false);
    }

    public Map<String, Object> postAllowConflict(String path, Object body) {
        return request("POST", path, body, true);
    }

    public Map<String, Object> cursor(String query, Map<String, Object> bindVars) {
        return post("/_db/" + properties.getDatabase() + "/_api/cursor", Map.of(
            "query", query,
            "bindVars", bindVars
        ));
    }

    private Map<String, Object> request(String method, String path, Object body, boolean allowConflict) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(uri(path))
                .header("Authorization", authorizationHeader())
                .header("Accept", "application/json");
            if (body == null) {
                builder.method(method, HttpRequest.BodyPublishers.noBody());
            } else {
                builder.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)));
            }

            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 409 && allowConflict) {
                return Map.of();
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("ArangoDB request failed status=" + response.statusCode() + " body=" + response.body());
            }
            if (response.body() == null || response.body().isBlank()) {
                return Map.of();
            }
            Object payload = objectMapper.readValue(response.body(), new TypeReference<>() {
            });
            if (payload instanceof Map<?, ?> map) {
                return (Map<String, Object>) map;
            }
            if (payload instanceof List<?> list) {
                return Map.of("result", list);
            }
            return Map.of("result", payload);
        } catch (Exception ex) {
            throw new IllegalStateException("ArangoDB request failed: " + method + " " + path, ex);
        }
    }

    private URI uri(String path) {
        String endpoint = properties.getEndpoint();
        if (endpoint.endsWith("/") && path.startsWith("/")) {
            return URI.create(endpoint.substring(0, endpoint.length() - 1) + path);
        }
        if (!endpoint.endsWith("/") && !path.startsWith("/")) {
            return URI.create(endpoint + "/" + path);
        }
        return URI.create(endpoint + path);
    }

    private String authorizationHeader() {
        String token = properties.getUsername() + ":" + properties.getRootPassword();
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
