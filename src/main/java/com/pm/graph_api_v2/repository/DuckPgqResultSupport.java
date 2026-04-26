package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.repository.model.EdgeRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
class DuckPgqResultSupport {

    private static final Logger log = LoggerFactory.getLogger(DuckPgqResultSupport.class);

    private final ObjectMapper objectMapper;

    DuckPgqResultSupport(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    EdgeRow mapEdgeRow(ResultSet rs, int ignored) throws SQLException {
        return new EdgeRow(
            rs.getString("edge_id"),
            rs.getString("from_node_id"),
            rs.getString("to_node_id"),
            rs.getString("edge_type"),
            rs.getBoolean("directed"),
            rs.getLong("tx_count"),
            rs.getDouble("tx_sum"),
            rs.getString("relation_family"),
            rs.getDouble("strength_score"),
            rs.getLong("evidence_count"),
            rs.getString("source_system"),
            rs.getTimestamp("first_seen_at") == null ? null : rs.getTimestamp("first_seen_at").toInstant(),
            rs.getTimestamp("last_seen_at") == null ? null : rs.getTimestamp("last_seen_at").toInstant(),
            readJsonMap(rs.getString("attrs_json"))
        );
    }

    int asInt(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    List<Long> parseLongList(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof Array sqlArray) {
            try {
                Object rawArray = sqlArray.getArray();
                if (rawArray instanceof Object[] array) {
                    List<Long> result = new ArrayList<>(array.length);
                    for (Object item : array) {
                        result.add(Long.parseLong(String.valueOf(item)));
                    }
                    return result;
                }
            } catch (SQLException ex) {
                log.debug("Failed to parse SQL array as longs: {}", ex.getMessage());
            }
        }
        if (value instanceof List<?> list) {
            List<Long> result = new ArrayList<>(list.size());
            for (Object item : list) {
                result.add(Long.parseLong(String.valueOf(item)));
            }
            return result;
        }

        String raw = value.toString().trim();
        if (raw.startsWith("[") && raw.endsWith("]")) {
            raw = raw.substring(1, raw.length() - 1).trim();
        }
        if (raw.isBlank()) {
            return List.of();
        }

        return Arrays.stream(raw.split("\\s*,\\s*"))
            .map(String::trim)
            .map(this::stripQuotes)
            .filter(token -> !token.isBlank())
            .map(Long::parseLong)
            .toList();
    }

    List<String> resolveNodeIdsByRowId(Connection connection, List<Long> rowIds) throws SQLException {
        if (rowIds.isEmpty()) {
            return List.of();
        }

        Map<Long, String> idsByRowId = new LinkedHashMap<>();
        String sql = "SELECT rowid, node_id FROM g_nodes WHERE rowid IN (" + longInList(rowIds) + ")";
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                idsByRowId.put(rs.getLong("rowid"), rs.getString("node_id"));
            }
        }

        List<String> ordered = new ArrayList<>(rowIds.size());
        for (Long rowId : rowIds) {
            String nodeId = idsByRowId.get(rowId);
            if (nodeId != null) {
                ordered.add(nodeId);
            }
        }
        return ordered;
    }

    List<String> resolveEdgeIdsByRowId(Connection connection, String tableName, List<Long> rowIds) throws SQLException {
        if (rowIds.isEmpty()) {
            return List.of();
        }

        Map<Long, String> idsByRowId = new LinkedHashMap<>();
        String sql = "SELECT rowid, edge_id FROM " + tableName + " WHERE rowid IN (" + longInList(rowIds) + ")";
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                idsByRowId.put(rs.getLong("rowid"), rs.getString("edge_id"));
            }
        }

        List<String> ordered = new ArrayList<>(rowIds.size());
        for (Long rowId : rowIds) {
            String edgeId = idsByRowId.get(rowId);
            if (edgeId != null) {
                ordered.add(edgeId);
            }
        }
        return ordered;
    }

    private String longInList(List<Long> values) {
        return values.stream()
            .map(String::valueOf)
            .collect(Collectors.joining(","));
    }

    private Map<String, Object> readJsonMap(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(rawJson, new TypeReference<>() { });
        } catch (Exception ex) {
            log.debug("Failed to parse attrs_json '{}': {}", rawJson, ex.getMessage());
            return Map.of();
        }
    }

    private String stripQuotes(String value) {
        if (value.length() >= 2 && ((value.startsWith("\"") && value.endsWith("\""))
            || (value.startsWith("'") && value.endsWith("'")))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
