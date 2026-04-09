package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphRelationFamily;
import com.pm.graph_api_v2.dto.SeedRef;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.FacetCountRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.NodeNeighborhoodSummaryRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class GraphRepository {

    private static final Logger log = LoggerFactory.getLogger(GraphRepository.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

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
        if (value.isBlank()) {
            return Optional.empty();
        }

        Optional<String> identifierMatch = queryNodeId(
            "SELECT node_id FROM g_identifiers WHERE id_type = ? AND id_value = ? LIMIT 1",
            seed.type().name(),
            value
        );
        if (identifierMatch.isPresent()) {
            return identifierMatch;
        }

        return switch (seed.type()) {
            case NODE_ID -> queryNodeId("SELECT node_id FROM g_nodes WHERE node_id = ? LIMIT 1", value);
            case PARTY_RK -> queryNodeId("SELECT node_id FROM g_nodes WHERE party_rk = ? LIMIT 1", value);
            case PERSON_ID -> queryNodeId("SELECT node_id FROM g_nodes WHERE person_id = ? LIMIT 1", value);
            case PHONE_NO -> queryNodeId("SELECT node_id FROM g_nodes WHERE phone_no = ? LIMIT 1", value);
            case ACCOUNT_NO, CARD_MASK, TAX_ID, DEVICE_ID, IP, EMAIL -> Optional.empty();
        };
    }

    public List<NodeRow> findNodesByIds(Collection<String> nodeIds) {
        if (nodeIds.isEmpty()) {
            return List.of();
        }

        Map<String, Map<String, String>> identifiersByNodeId = findIdentifiersByNodeIds(nodeIds);

        String sql = "SELECT node_id, node_type, display_name, party_rk, person_id, phone_no, full_name, is_blacklist, is_vip, employer, city, source_system, pagerank_score, hub_score, attrs_json " +
            "FROM g_nodes WHERE node_id IN (" + placeholders(nodeIds.size()) + ")";

        return jdbcTemplate.query(sql, (rs, ignored) -> mapNodeRow(rs, identifiersByNodeId), nodeIds.toArray());
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
            "FROM g_edges WHERE edge_id IN (" + placeholders(edgeIds.size()) + ")";

        return jdbcTemplate.query(sql, this::mapEdgeRow, edgeIds.toArray());
    }

    public List<EdgeRow> findAllEdges() {
        String sql = "SELECT edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum, relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json " +
            "FROM g_edges ORDER BY edge_id";

        return jdbcTemplate.query(sql, this::mapEdgeRow);
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
                                                            GraphRelationFamily relationFamily,
                                                            Direction direction) {
        NeighborhoodQuery selectedScope = neighborhoodQuery(nodeId, relationFamily, direction);
        NeighborhoodQuery outboundScope = neighborhoodQuery(nodeId, relationFamily, Direction.OUTBOUND);
        NeighborhoodQuery inboundScope = neighborhoodQuery(nodeId, relationFamily, Direction.INBOUND);

        NeighborhoodAggregate selected = aggregateNeighborhood(selectedScope);
        NeighborhoodAggregate outbound = aggregateNeighborhood(outboundScope);
        NeighborhoodAggregate inbound = aggregateNeighborhood(inboundScope);

        return new NodeNeighborhoodSummaryRow(
            selected.edgeCount(),
            selected.uniqueNeighborCount(),
            outbound.edgeCount(),
            inbound.edgeCount()
        );
    }

    public List<FacetCountRow> countRelationFamiliesAroundNode(String nodeId,
                                                               GraphRelationFamily relationFamily,
                                                               Direction direction) {
        NeighborhoodQuery scope = neighborhoodQuery(nodeId, relationFamily, direction);
        String sql = scope.cteSql() + """
            SELECT relation_family AS facet_key, COUNT(*) AS facet_count
            FROM candidate_edges
            WHERE relation_family IS NOT NULL AND relation_family <> ''
            GROUP BY relation_family
            ORDER BY facet_count DESC, facet_key
            """;
        return jdbcTemplate.query(sql, this::mapFacetCountRow, scope.params());
    }

    public List<FacetCountRow> countEdgeTypesAroundNode(String nodeId,
                                                        GraphRelationFamily relationFamily,
                                                        Direction direction) {
        NeighborhoodQuery scope = neighborhoodQuery(nodeId, relationFamily, direction);
        String sql = scope.cteSql() + """
            SELECT edge_type AS facet_key, COUNT(*) AS facet_count
            FROM candidate_edges
            WHERE edge_type IS NOT NULL AND edge_type <> ''
            GROUP BY edge_type
            ORDER BY facet_count DESC, facet_key
            """;
        return jdbcTemplate.query(sql, this::mapFacetCountRow, scope.params());
    }

    public List<FacetCountRow> countNeighborNodeTypesAroundNode(String nodeId,
                                                                GraphRelationFamily relationFamily,
                                                                Direction direction) {
        NeighborhoodQuery scope = neighborhoodQuery(nodeId, relationFamily, direction);
        String sql = scope.cteSql() + """
            SELECT COALESCE(n.node_type, 'UNKNOWN') AS facet_key, COUNT(DISTINCT candidate_edges.neighbor_node_id) AS facet_count
            FROM candidate_edges
            LEFT JOIN g_nodes n ON n.node_id = candidate_edges.neighbor_node_id
            GROUP BY COALESCE(n.node_type, 'UNKNOWN')
            ORDER BY facet_count DESC, facet_key
            """;
        return jdbcTemplate.query(sql, this::mapFacetCountRow, scope.params());
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

    private NeighborhoodAggregate aggregateNeighborhood(NeighborhoodQuery scope) {
        String sql = scope.cteSql() + """
            SELECT
                COUNT(*) AS edge_count,
                COUNT(DISTINCT neighbor_node_id) AS unique_neighbor_count
            FROM candidate_edges
            """;

        List<NeighborhoodAggregate> rows = jdbcTemplate.query(sql, (rs, rowNum) -> new NeighborhoodAggregate(
            rs.getInt("edge_count"),
            rs.getInt("unique_neighbor_count")
        ), scope.params());

        if (rows.isEmpty()) {
            return new NeighborhoodAggregate(0, 0);
        }
        return rows.get(0);
    }

    private NeighborhoodQuery neighborhoodQuery(String nodeId,
                                                GraphRelationFamily relationFamily,
                                                Direction direction) {
        List<Object> params = new ArrayList<>();
        String neighborExpression;
        String whereClause;

        switch (direction) {
            case OUTBOUND -> {
                neighborExpression = "CASE WHEN from_node_id = ? THEN to_node_id ELSE from_node_id END";
                whereClause = "(from_node_id = ? OR (directed = FALSE AND to_node_id = ?))";
                params.add(nodeId);
                params.add(nodeId);
                params.add(nodeId);
            }
            case INBOUND -> {
                neighborExpression = "CASE WHEN to_node_id = ? THEN from_node_id ELSE to_node_id END";
                whereClause = "(to_node_id = ? OR (directed = FALSE AND from_node_id = ?))";
                params.add(nodeId);
                params.add(nodeId);
                params.add(nodeId);
            }
            case BOTH -> {
                neighborExpression = "CASE WHEN from_node_id = ? THEN to_node_id ELSE from_node_id END";
                whereClause = "(from_node_id = ? OR to_node_id = ?)";
                params.add(nodeId);
                params.add(nodeId);
                params.add(nodeId);
            }
            default -> throw new IllegalStateException("Unsupported direction: " + direction);
        }

        String relationFilter = "";
        if (!relationFamily.isAllRelations()) {
            relationFilter = " AND relation_family = ?";
            params.add(relationFamily.name());
        }

        String cteSql = """
            WITH candidate_edges AS (
                SELECT
                    edge_id,
                    edge_type,
                    relation_family,
                    %s AS neighbor_node_id
                FROM g_edges
                WHERE %s%s
            )
            """.formatted(neighborExpression, whereClause, relationFilter);

        return new NeighborhoodQuery(cteSql, params.toArray());
    }

    private String placeholders(int count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

    private FacetCountRow mapFacetCountRow(ResultSet rs, int ignored) throws SQLException {
        return new FacetCountRow(rs.getString("facet_key"), rs.getInt("facet_count"));
    }

    private NodeRow mapNodeRow(ResultSet rs, Map<String, Map<String, String>> identifiersByNodeId) throws SQLException {
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

    private EdgeRow mapEdgeRow(ResultSet rs, int ignored) throws SQLException {
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

    private Map<String, Map<String, String>> findIdentifiersByNodeIds(Collection<String> nodeIds) {
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

    private record NeighborhoodQuery(String cteSql, Object[] params) {
    }

    private record NeighborhoodAggregate(int edgeCount, int uniqueNeighborCount) {
    }
}
