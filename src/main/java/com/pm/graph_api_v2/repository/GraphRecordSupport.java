package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class GraphRecordSupport {

    private static final Logger log = LoggerFactory.getLogger(GraphRecordSupport.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public GraphRecordSupport(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public String placeholders(int count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

    public NodeRow mapNodeRow(ResultSet rs, Map<String, Map<String, String>> identifiersByNodeId) throws SQLException {
        String nodeId = rs.getString("node_id");
        return new NodeRow(
            nodeId,
            rs.getString("node_type"),
            rs.getString("display_name"),
            rs.getString("party_rk"),
            rs.getString("person_id"),
            rs.getString("phone_no"),
            rs.getString("full_name"),
            rs.getBoolean("is_blacklist"),
            rs.getBoolean("is_vip"),
            rs.getString("employer"),
            rs.getString("city"),
            rs.getString("source_system"),
            rs.getDouble("pagerank_score"),
            rs.getDouble("hub_score"),
            identifiersByNodeId.getOrDefault(nodeId, Map.of()),
            readJsonMap(rs.getString("attrs_json"))
        );
    }

    public EdgeRow mapEdgeRow(ResultSet rs, int ignored) throws SQLException {
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
            toInstant(rs.getTimestamp("first_seen_at")),
            toInstant(rs.getTimestamp("last_seen_at")),
            readJsonMap(rs.getString("attrs_json"))
        );
    }

    public Map<String, Map<String, String>> findIdentifiersByNodeIds(Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return Map.of();
        }

        String sql = "SELECT node_id, id_type, id_value FROM g_identifiers WHERE node_id IN (" + placeholders(nodeIds.size()) + ") ORDER BY node_id, id_type, id_value";

        Map<String, Map<String, String>> identifiersByNodeId = new LinkedHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            String nodeId = rs.getString("node_id");
            identifiersByNodeId
                .computeIfAbsent(nodeId, ignored -> new LinkedHashMap<>())
                .putIfAbsent(rs.getString("id_type").toLowerCase(), rs.getString("id_value"));
        }, nodeIds.toArray());
        return identifiersByNodeId;
    }

    private Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private Map<String, Object> readJsonMap(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return Map.of();
        }

        try {
            return objectMapper.readValue(rawJson, new TypeReference<>() {
            });
        } catch (Exception ex) {
            log.debug("Failed to parse attrs_json '{}': {}", rawJson, ex.getMessage());
            return Map.of();
        }
    }
}
