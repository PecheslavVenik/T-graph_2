package com.pm.graph_api_v2.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Locale;

@Validated
@ConfigurationProperties(prefix = "graph.cors")
public class GraphCorsProperties {

    private List<String> allowedOrigins = List.of("http://localhost:3000", "http://localhost:5173");
    private List<String> allowedMethods = List.of("GET", "POST", "OPTIONS");
    private List<String> allowedHeaders = List.of("*");

    @Min(0)
    @Max(86_400)
    private long maxAgeSeconds = 3600;

    public List<String> getAllowedOrigins() {
        return allowedOrigins;
    }

    public void setAllowedOrigins(List<String> allowedOrigins) {
        this.allowedOrigins = normalizeList(allowedOrigins, false);
    }

    public List<String> getAllowedMethods() {
        return allowedMethods;
    }

    public void setAllowedMethods(List<String> allowedMethods) {
        this.allowedMethods = normalizeList(allowedMethods, true);
    }

    public List<String> getAllowedHeaders() {
        return allowedHeaders;
    }

    public void setAllowedHeaders(List<String> allowedHeaders) {
        this.allowedHeaders = normalizeList(allowedHeaders, false);
    }

    public long getMaxAgeSeconds() {
        return maxAgeSeconds;
    }

    public void setMaxAgeSeconds(long maxAgeSeconds) {
        this.maxAgeSeconds = maxAgeSeconds;
    }

    private List<String> normalizeList(List<String> values, boolean upperCase) {
        if (values == null) {
            return List.of();
        }

        return values.stream()
            .filter(value -> value != null && !value.isBlank())
            .map(String::trim)
            .map(value -> upperCase ? value.toUpperCase(Locale.ROOT) : value)
            .distinct()
            .toList();
    }
}
