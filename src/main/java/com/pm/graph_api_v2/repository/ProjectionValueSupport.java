package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.repository.model.EdgeRow;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;

final class ProjectionValueSupport {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ProjectionValueSupport() {
    }

    static EdgeRow mapEdge(Map<String, Object> row) {
        return new EdgeRow(
            stringValue(row.get("edge_id")),
            stringValue(row.get("from_node_id")),
            stringValue(row.get("to_node_id")),
            stringValue(row.get("edge_type")),
            booleanValue(row.get("directed")),
            longValue(row.get("tx_count")),
            doubleValue(row.get("tx_sum")),
            stringValue(row.get("relation_family")),
            doubleValue(row.get("strength_score")),
            longValue(row.get("evidence_count")),
            stringValue(row.get("source_system")),
            instantValue(row.get("first_seen_at")),
            instantValue(row.get("last_seen_at")),
            jsonMap(row.get("attrs_json"))
        );
    }

    static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    static boolean booleanValue(Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        return value != null && Boolean.parseBoolean(String.valueOf(value));
    }

    static long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return 0L;
        }
        return Long.parseLong(String.valueOf(value));
    }

    static double doubleValue(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return 0D;
        }
        return Double.parseDouble(String.valueOf(value));
    }

    static Instant instantValue(Object value) {
        if (value == null || String.valueOf(value).isBlank()) {
            return null;
        }
        return Instant.parse(String.valueOf(value));
    }

    static Map<String, Object> jsonMap(Object value) {
        if (value == null || String.valueOf(value).isBlank()) {
            return Map.of();
        }
        try {
            return OBJECT_MAPPER.readValue(String.valueOf(value), new TypeReference<>() {
            });
        } catch (Exception ignored) {
            return Map.of();
        }
    }
}
