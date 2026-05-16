package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.repository.model.ImportEdgeRow;
import com.pm.graph_api_v2.repository.model.ImportGraphData;
import com.pm.graph_api_v2.repository.model.ImportNodeRow;
import com.pm.graph_api_v2.repository.model.ImportWriteResult;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Repository
public class GraphImportWriter {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public GraphImportWriter(JdbcTemplate jdbcTemplate,
                             ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ImportWriteResult importGraph(ImportGraphData graphData) {
        int insertedNodes = 0;
        int updatedNodes = 0;
        int insertedEdges = 0;
        int updatedEdges = 0;

        for (ImportNodeRow row : graphData.nodes()) {
            if (Boolean.TRUE.equals(queryExists("SELECT 1 FROM g_nodes WHERE node_id = ? LIMIT 1", row.nodeId()))) {
                updateImportedNode(row);
                updatedNodes++;
            } else {
                insertImportedNode(row);
                insertedNodes++;
            }
            replaceIdentifiers(row);
        }

        for (ImportEdgeRow row : graphData.edges()) {
            if (Boolean.TRUE.equals(queryExists("SELECT 1 FROM g_edges WHERE edge_id = ? LIMIT 1", row.edgeId()))) {
                updateImportedEdge(row);
                updatedEdges++;
            } else {
                insertImportedEdge(row);
                insertedEdges++;
            }
        }

        return new ImportWriteResult(insertedNodes, updatedNodes, insertedEdges, updatedEdges);
    }

    private Boolean queryExists(String sql, Object... params) {
        List<Integer> rows = jdbcTemplate.query(sql, (rs, rowNum) -> 1, params);
        return !rows.isEmpty();
    }

    private void insertImportedNode(ImportNodeRow row) {
        jdbcTemplate.update(
            """
            INSERT INTO g_nodes (
                node_id, node_type, display_name, party_rk, person_id, phone_no, full_name,
                is_blacklist, is_vip, employer, city, source_system, pagerank_score, hub_score,
                attrs_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            row.nodeId(),
            row.nodeType(),
            row.displayName(),
            row.partyRk(),
            row.personId(),
            row.phoneNo(),
            row.fullName(),
            row.blacklist(),
            row.vip(),
            row.employer(),
            row.city(),
            row.sourceSystem(),
            row.pagerankScore(),
            row.hubScore(),
            toJson(row.attrs())
        );
    }

    private void updateImportedNode(ImportNodeRow row) {
        jdbcTemplate.update(
            """
            UPDATE g_nodes
            SET node_type = ?,
                display_name = ?,
                party_rk = ?,
                person_id = ?,
                phone_no = ?,
                full_name = ?,
                is_blacklist = ?,
                is_vip = ?,
                employer = ?,
                city = ?,
                source_system = ?,
                pagerank_score = ?,
                hub_score = ?,
                attrs_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE node_id = ?
            """,
            row.nodeType(),
            row.displayName(),
            row.partyRk(),
            row.personId(),
            row.phoneNo(),
            row.fullName(),
            row.blacklist(),
            row.vip(),
            row.employer(),
            row.city(),
            row.sourceSystem(),
            row.pagerankScore(),
            row.hubScore(),
            toJson(row.attrs()),
            row.nodeId()
        );
    }

    private void replaceIdentifiers(ImportNodeRow row) {
        jdbcTemplate.update("DELETE FROM g_identifiers WHERE node_id = ?", row.nodeId());
        for (Map.Entry<String, String> identifier : row.identifiers().entrySet()) {
            jdbcTemplate.update(
                "INSERT INTO g_identifiers (node_id, id_type, id_value) VALUES (?, ?, ?)",
                row.nodeId(),
                identifier.getKey(),
                identifier.getValue()
            );
        }
    }

    private void insertImportedEdge(ImportEdgeRow row) {
        jdbcTemplate.update(
            """
            INSERT INTO g_edges (
                edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
                relation_family, strength_score, evidence_count, source_system, first_seen_at,
                last_seen_at, attrs_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            row.edgeId(),
            row.fromNodeId(),
            row.toNodeId(),
            row.edgeType(),
            row.directed(),
            row.txCount(),
            row.txSum(),
            row.relationFamily(),
            row.strengthScore(),
            row.evidenceCount(),
            row.sourceSystem(),
            toTimestamp(row.firstSeenAt()),
            toTimestamp(row.lastSeenAt()),
            toJson(row.attrs())
        );
    }

    private void updateImportedEdge(ImportEdgeRow row) {
        jdbcTemplate.update(
            """
            UPDATE g_edges
            SET from_node_id = ?,
                to_node_id = ?,
                edge_type = ?,
                directed = ?,
                tx_count = ?,
                tx_sum = ?,
                relation_family = ?,
                strength_score = ?,
                evidence_count = ?,
                source_system = ?,
                first_seen_at = ?,
                last_seen_at = ?,
                attrs_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE edge_id = ?
            """,
            row.fromNodeId(),
            row.toNodeId(),
            row.edgeType(),
            row.directed(),
            row.txCount(),
            row.txSum(),
            row.relationFamily(),
            row.strengthScore(),
            row.evidenceCount(),
            row.sourceSystem(),
            toTimestamp(row.firstSeenAt()),
            toTimestamp(row.lastSeenAt()),
            toJson(row.attrs()),
            row.edgeId()
        );
    }

    private String toJson(Map<String, Object> attrs) {
        if (attrs == null || attrs.isEmpty()) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(attrs);
        } catch (Exception ex) {
            return "{}";
        }
    }

    private Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }
}
