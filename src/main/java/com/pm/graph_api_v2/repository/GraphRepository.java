package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.SeedRef;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class GraphRepository {

    private static final Logger log = LoggerFactory.getLogger(GraphRepository.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private final RowMapper<NodeRow> nodeRowMapper = this::mapNodeRow;
    private final RowMapper<EdgeRow> edgeRowMapper = this::mapEdgeRow;

    public GraphRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public LinkedHashSet<String> resolveNodeIds(List<SeedRef> seeds) {
        LinkedHashSet<String> nodeIds = new LinkedHashSet<>();
        for (SeedRef seed : seeds) {
            resolveNodeId(seed).ifPresent(nodeIds::add);
        }
        return nodeIds;
    }

    public Optional<String> resolveNodeId(SeedRef seed) {
        String value = seed.value().trim();
        return switch (seed.type()) {
            case NODE_ID -> queryNodeId("SELECT node_id FROM g_nodes WHERE node_id = ? LIMIT 1", value)
                .or(() -> queryNodeId("SELECT node_id FROM g_identifiers WHERE id_type = 'NODE_ID' AND id_value = ? LIMIT 1", value));
            case PARTY_RK -> queryNodeId("SELECT node_id FROM g_nodes WHERE party_rk = ? LIMIT 1", value)
                .or(() -> queryNodeId("SELECT node_id FROM g_identifiers WHERE id_type = 'PARTY_RK' AND id_value = ? LIMIT 1", value));
            case PERSON_ID -> queryNodeId("SELECT node_id FROM g_nodes WHERE person_id = ? LIMIT 1", value)
                .or(() -> queryNodeId("SELECT node_id FROM g_identifiers WHERE id_type = 'PERSON_ID' AND id_value = ? LIMIT 1", value));
            case PHONE_NO -> queryNodeId("SELECT node_id FROM g_nodes WHERE phone_no = ? LIMIT 1", value)
                .or(() -> queryNodeId("SELECT node_id FROM g_identifiers WHERE id_type = 'PHONE_NO' AND id_value = ? LIMIT 1", value));
        };
    }

    public List<NodeRow> findNodesByIds(Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT node_id, party_rk, person_id, phone_no, full_name, is_blacklist, is_vip, employer, attrs_json " +
            "FROM g_nodes WHERE node_id IN (" + placeholders(nodeIds.size()) + ") ORDER BY node_id";

        return jdbcTemplate.query(sql, nodeRowMapper, nodeIds.toArray());
    }

    public List<EdgeRow> findEdgesByIds(Collection<String> edgeIds) {
        if (edgeIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum, attrs_json " +
            "FROM g_edges WHERE edge_id IN (" + placeholders(edgeIds.size()) + ") ORDER BY edge_id";

        return jdbcTemplate.query(sql, edgeRowMapper, edgeIds.toArray());
    }

    public List<String> findDistinctEdgeTypes() {
        return jdbcTemplate.query(
            "SELECT DISTINCT edge_type FROM g_edges ORDER BY edge_type",
            (rs, rowNum) -> rs.getString("edge_type")
        );
    }

    private Optional<String> queryNodeId(String sql, String value) {
        List<String> rows = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("node_id"), value);
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(rows.get(0));
    }

    private String placeholders(int count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

    private NodeRow mapNodeRow(ResultSet rs, int ignored) throws SQLException {
        return new NodeRow(
            rs.getString("node_id"),
            rs.getString("party_rk"),
            rs.getString("person_id"),
            rs.getString("phone_no"),
            rs.getString("full_name"),
            rs.getBoolean("is_blacklist"),
            rs.getBoolean("is_vip"),
            rs.getString("employer"),
            readJsonMap(rs.getString("attrs_json"))
        );
    }

    private EdgeRow mapEdgeRow(ResultSet rs, int ignored) throws SQLException {
        return new EdgeRow(
            rs.getString("edge_id"),
            rs.getString("from_node_id"),
            rs.getString("to_node_id"),
            rs.getString("edge_type"),
            rs.getBoolean("directed"),
            rs.getLong("tx_count"),
            rs.getDouble("tx_sum"),
            readJsonMap(rs.getString("attrs_json"))
        );
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
