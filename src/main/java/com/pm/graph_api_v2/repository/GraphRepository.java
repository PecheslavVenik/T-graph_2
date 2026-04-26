package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.SeedRef;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.FacetCountRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.NodeNeighborhoodSummaryRow;
import com.pm.graph_api_v2.util.GraphSeedTypes;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

@Repository
public class GraphRepository {

    private final JdbcTemplate jdbcTemplate;
    private final GraphRecordSupport graphRecordSupport;
    private final GraphNeighborhoodSupport graphNeighborhoodSupport;

    public GraphRepository(JdbcTemplate jdbcTemplate,
                           GraphRecordSupport graphRecordSupport,
                           GraphNeighborhoodSupport graphNeighborhoodSupport) {
        this.jdbcTemplate = jdbcTemplate;
        this.graphRecordSupport = graphRecordSupport;
        this.graphNeighborhoodSupport = graphNeighborhoodSupport;
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
        if (value.isBlank()) {
            return Optional.empty();
        }

        String identifierType = GraphSeedTypes.normalize(seed.type());
        if (identifierType == null) {
            return Optional.empty();
        }

        Optional<String> identifierMatch = queryNodeId(
            "SELECT node_id FROM g_identifiers WHERE id_type = ? AND id_value = ? LIMIT 1",
            identifierType,
            value
        );
        if (identifierMatch.isPresent()) {
            return identifierMatch;
        }

        if (GraphSeedTypes.isNodeId(identifierType)) {
            return queryNodeId("SELECT node_id FROM g_nodes WHERE node_id = ? LIMIT 1", value);
        }

        return Optional.empty();
    }

    public List<NodeRow> findNodesByIds(Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return List.of();
        }

        Map<String, Map<String, String>> identifiersByNodeId = graphRecordSupport.findIdentifiersByNodeIds(nodeIds);

        String sql = "SELECT node_id, node_type, display_name, party_rk, person_id, phone_no, full_name, is_blacklist, is_vip, employer, city, source_system, pagerank_score, hub_score, attrs_json " +
            "FROM g_nodes WHERE node_id IN (" + graphRecordSupport.placeholders(nodeIds.size()) + ")";

        return jdbcTemplate.query(sql, (rs, ignored) -> graphRecordSupport.mapNodeRow(rs, identifiersByNodeId), nodeIds.toArray());
    }

    public Optional<NodeRow> findNodeById(String nodeId) {
        List<NodeRow> rows = findNodesByIds(List.of(nodeId));
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rows.get(0));
    }

    public List<NodeRow> findAllNodes() {
        List<String> nodeIds = jdbcTemplate.query(
            "SELECT node_id FROM g_nodes ORDER BY node_id",
            (rs, rowNum) -> rs.getString("node_id")
        );
        return findNodesByIds(nodeIds);
    }

    public void forEachNodeBatch(int batchSize, Consumer<List<NodeRow>> consumer) {
        int effectiveBatchSize = Math.max(1, batchSize);
        String lastNodeId = "";

        while (true) {
            List<String> nodeIds = jdbcTemplate.query(
                "SELECT node_id FROM g_nodes WHERE node_id > ? ORDER BY node_id LIMIT ?",
                (rs, rowNum) -> rs.getString("node_id"),
                lastNodeId,
                effectiveBatchSize
            );
            if (nodeIds.isEmpty()) {
                return;
            }

            consumer.accept(findNodesByIds(nodeIds));
            lastNodeId = nodeIds.get(nodeIds.size() - 1);
        }
    }

    public List<NodeRow> findNodesByIdsInOrder(List<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return List.of();
        }

        Map<String, NodeRow> rowsById = new LinkedHashMap<>();
        for (NodeRow row : findNodesByIds(nodeIds)) {
            rowsById.put(row.nodeId(), row);
        }

        List<NodeRow> ordered = new ArrayList<>(nodeIds.size());
        for (String nodeId : nodeIds) {
            NodeRow row = rowsById.get(nodeId);
            if (row != null) {
                ordered.add(row);
            }
        }
        return ordered;
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

    public List<String> findDistinctEdgeTypes() {
        return jdbcTemplate.query(
            "SELECT DISTINCT edge_type FROM g_edges ORDER BY edge_type",
            (rs, rowNum) -> rs.getString("edge_type")
        );
    }

    public List<String> findDistinctRelationFamilies() {
        return jdbcTemplate.query(
            "SELECT DISTINCT relation_family FROM g_edges WHERE relation_family IS NOT NULL AND relation_family <> '' ORDER BY relation_family",
            (rs, rowNum) -> rs.getString("relation_family")
        );
    }

    public List<String> findDistinctNodeTypes() {
        return jdbcTemplate.query(
            "SELECT DISTINCT node_type FROM g_nodes WHERE node_type IS NOT NULL AND node_type <> '' ORDER BY node_type",
            (rs, rowNum) -> rs.getString("node_type")
        );
    }

    public List<String> findPresentNodeStatuses() {
        List<String> statuses = new ArrayList<>();
        if (Boolean.TRUE.equals(queryExists("SELECT 1 FROM g_nodes WHERE is_blacklist = TRUE LIMIT 1"))) {
            statuses.add("BLACKLIST");
        }
        if (Boolean.TRUE.equals(queryExists("SELECT 1 FROM g_nodes WHERE is_vip = TRUE LIMIT 1"))) {
            statuses.add("VIP");
        }
        return statuses;
    }

    public NodeNeighborhoodSummaryRow summarizeNeighborhood(String nodeId,
                                                            String relationFamily,
                                                            Direction direction) {
        return graphNeighborhoodSupport.summarizeNeighborhood(nodeId, relationFamily, direction);
    }

    public List<FacetCountRow> countRelationFamiliesAroundNode(String nodeId,
                                                               String relationFamily,
                                                               Direction direction) {
        return graphNeighborhoodSupport.countRelationFamiliesAroundNode(nodeId, relationFamily, direction);
    }

    public List<FacetCountRow> countEdgeTypesAroundNode(String nodeId,
                                                        String relationFamily,
                                                        Direction direction) {
        return graphNeighborhoodSupport.countEdgeTypesAroundNode(nodeId, relationFamily, direction);
    }

    public List<FacetCountRow> countNeighborNodeTypesAroundNode(String nodeId,
                                                                String relationFamily,
                                                                Direction direction) {
        return graphNeighborhoodSupport.countNeighborNodeTypesAroundNode(nodeId, relationFamily, direction);
    }

    private Optional<String> queryNodeId(String sql, Object... params) {
        List<String> rows = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("node_id"), params);
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(rows.get(0));
    }

    private Boolean queryExists(String sql) {
        List<Integer> rows = jdbcTemplate.query(sql, (rs, rowNum) -> 1);
        return !rows.isEmpty();
    }

}
