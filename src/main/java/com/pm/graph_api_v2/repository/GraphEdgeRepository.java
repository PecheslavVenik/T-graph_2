package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodePair;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Repository
public class GraphEdgeRepository {

    private final JdbcTemplate jdbcTemplate;
    private final GraphRecordSupport graphRecordSupport;

    public GraphEdgeRepository(JdbcTemplate jdbcTemplate,
                               GraphRecordSupport graphRecordSupport) {
        this.jdbcTemplate = jdbcTemplate;
        this.graphRecordSupport = graphRecordSupport;
    }

    public List<EdgeRow> findEdgesByIds(Collection<String> edgeIds) {
        if (edgeIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum, relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json " +
            "FROM g_edges WHERE edge_id IN (" + graphRecordSupport.placeholders(edgeIds.size()) + ")";

        return jdbcTemplate.query(sql, graphRecordSupport::mapEdgeRow, edgeIds.toArray());
    }

    public List<EdgeRow> findAllEdges() {
        String sql = "SELECT edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum, relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json " +
            "FROM g_edges ORDER BY edge_id";

        return jdbcTemplate.query(sql, graphRecordSupport::mapEdgeRow);
    }

    public void forEachEdgeBatch(int batchSize, Consumer<List<EdgeRow>> consumer) {
        int effectiveBatchSize = Math.max(1, batchSize);
        String lastEdgeId = "";
        String sql = "SELECT edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum, relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json " +
            "FROM g_edges WHERE edge_id > ? ORDER BY edge_id LIMIT ?";

        while (true) {
            List<EdgeRow> rows = jdbcTemplate.query(sql, graphRecordSupport::mapEdgeRow, lastEdgeId, effectiveBatchSize);
            if (rows.isEmpty()) {
                return;
            }

            consumer.accept(rows);
            lastEdgeId = rows.get(rows.size() - 1).edgeId();
        }
    }

    public List<EdgeRow> findEdgesByIdsInOrder(List<String> edgeIds) {
        if (edgeIds.isEmpty()) {
            return List.of();
        }

        Map<String, EdgeRow> rowsById = new LinkedHashMap<>();
        for (EdgeRow row : findEdgesByIds(edgeIds)) {
            rowsById.put(row.edgeId(), row);
        }

        List<EdgeRow> ordered = new ArrayList<>(edgeIds.size());
        for (String edgeId : edgeIds) {
            EdgeRow row = rowsById.get(edgeId);
            if (row != null) {
                ordered.add(row);
            }
        }
        return ordered;
    }

    public List<EdgeRow> findEdgesByEndpointPairs(Collection<NodePair> nodePairs, int limit) {
        if (nodePairs.isEmpty() || limit < 1) {
            return List.of();
        }

        List<String> predicates = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        for (NodePair pair : nodePairs) {
            predicates.add("((from_node_id = ? AND to_node_id = ?) OR (directed = FALSE AND from_node_id = ? AND to_node_id = ?))");
            params.add(pair.fromNodeId());
            params.add(pair.toNodeId());
            params.add(pair.toNodeId());
            params.add(pair.fromNodeId());
        }
        params.add(limit);

        String sql = "SELECT edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum, relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json " +
            "FROM g_edges WHERE " + String.join(" OR ", predicates) + " ORDER BY edge_id LIMIT ?";

        return jdbcTemplate.query(sql, graphRecordSupport::mapEdgeRow, params.toArray());
    }
}
