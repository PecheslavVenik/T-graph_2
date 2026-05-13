package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.SeedRef;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.FacetCountRow;
import com.pm.graph_api_v2.repository.model.ImportEdgeRow;
import com.pm.graph_api_v2.repository.model.ImportGraphData;
import com.pm.graph_api_v2.repository.model.ImportNodeRow;
import com.pm.graph_api_v2.repository.model.ImportWriteResult;
import com.pm.graph_api_v2.repository.model.NodePair;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.NodeNeighborhoodSummaryRow;
import com.pm.graph_api_v2.repository.model.SqlGraphQueryResult;
import com.pm.graph_api_v2.util.GraphSeedTypes;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

@Repository
public class GraphRepository {

    private static final int QUERY_TIMEOUT_SECONDS = 5;

    private final JdbcTemplate jdbcTemplate;
    private final GraphRecordSupport graphRecordSupport;
    private final GraphNeighborhoodSupport graphNeighborhoodSupport;
    private final ObjectMapper objectMapper;

    public GraphRepository(JdbcTemplate jdbcTemplate,
                           GraphRecordSupport graphRecordSupport,
                           GraphNeighborhoodSupport graphNeighborhoodSupport,
                           ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.graphRecordSupport = graphRecordSupport;
        this.graphNeighborhoodSupport = graphNeighborhoodSupport;
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

    public List<NodeRow> searchNodes(String query, String nodeType, int limit) {
        String normalizedQuery = query.trim().toLowerCase(Locale.ROOT);
        String exactQuery = normalizedQuery;
        String prefixPattern = escapeLike(normalizedQuery) + "%";
        String containsPattern = "%" + escapeLike(normalizedQuery) + "%";
        String nodeTypeFilter = nodeType == null ? "" : nodeType.trim().toUpperCase(Locale.ROOT);

        String searchableFields = """
            LOWER(COALESCE(n.node_id, '')),
            LOWER(COALESCE(n.display_name, '')),
            LOWER(COALESCE(n.party_rk, '')),
            LOWER(COALESCE(n.person_id, '')),
            LOWER(COALESCE(n.phone_no, '')),
            LOWER(COALESCE(n.full_name, '')),
            LOWER(COALESCE(n.employer, '')),
            LOWER(COALESCE(n.city, '')),
            LOWER(COALESCE(n.source_system, '')),
            LOWER(COALESCE(n.attrs_json, '')),
            LOWER(COALESCE(i.id_type, '')),
            LOWER(COALESCE(i.id_value, ''))
            """;
        String exactMatch = anyField(searchableFields, "= ?");
        String prefixMatch = anyField(searchableFields, "LIKE ? ESCAPE '\\'");
        String containsMatch = anyField(searchableFields, "LIKE ? ESCAPE '\\'");

        String sql = """
            SELECT matched.node_id
            FROM (
                SELECT
                    n.node_id,
                    MIN(CASE
                        WHEN %s THEN 0
                        WHEN %s THEN 1
                        ELSE 2
                    END) AS match_rank,
                    MIN(LOWER(COALESCE(n.display_name, n.full_name, n.node_id))) AS sort_name
                FROM g_nodes n
                LEFT JOIN g_identifiers i ON i.node_id = n.node_id
                WHERE (? = '' OR UPPER(COALESCE(n.node_type, '')) = ?)
                  AND (%s)
                GROUP BY n.node_id
            ) matched
            ORDER BY matched.match_rank, matched.sort_name, matched.node_id
            LIMIT ?
            """.formatted(exactMatch, prefixMatch, containsMatch);

        List<Object> params = new ArrayList<>();
        addRepeated(params, exactQuery, fieldCount(searchableFields));
        addRepeated(params, prefixPattern, fieldCount(searchableFields));
        params.add(nodeTypeFilter);
        params.add(nodeTypeFilter);
        addRepeated(params, containsPattern, fieldCount(searchableFields));
        params.add(limit);

        List<String> nodeIds = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("node_id"), params.toArray());
        return findNodesByIdsInOrder(nodeIds);
    }

    public List<String> executeSqlNodeIdQuery(String sql, int limit) {
        String limitedSql = limitedSql(sql, limit);
        return jdbcTemplate.query(connection -> {
            var statement = connection.prepareStatement(limitedSql);
            statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            return statement;
        }, rs -> {
            ResultSetMetaData metaData = rs.getMetaData();
            int nodeIdColumn = firstColumnIndex(metaData, "node_id", "nodeid", "id");
            if (nodeIdColumn < 1) {
                nodeIdColumn = 1;
            }

            LinkedHashSet<String> nodeIds = new LinkedHashSet<>();
            while (rs.next()) {
                addIfPresent(nodeIds, rs.getString(nodeIdColumn));
            }
            return new ArrayList<>(nodeIds);
        });
    }

    public SqlGraphQueryResult executeSqlGraphQuery(String sql, int limit) {
        String limitedSql = limitedSql(sql, limit);
        return jdbcTemplate.query(connection -> {
            var statement = connection.prepareStatement(limitedSql);
            statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            return statement;
        }, rs -> {
            ResultSetMetaData metaData = rs.getMetaData();
            int nodeIdColumn = firstColumnIndex(metaData, "node_id", "nodeid");
            int edgeIdColumn = firstColumnIndex(metaData, "edge_id", "edgeid");
            int fromNodeIdColumn = firstColumnIndex(metaData, "from_node_id", "fromnodeid", "source");
            int toNodeIdColumn = firstColumnIndex(metaData, "to_node_id", "tonodeid", "target");

            LinkedHashSet<String> nodeIds = new LinkedHashSet<>();
            LinkedHashSet<String> edgeIds = new LinkedHashSet<>();
            LinkedHashMap<String, NodePair> nodePairs = new LinkedHashMap<>();
            int rowCount = 0;

            while (rs.next()) {
                rowCount++;
                if (nodeIdColumn > 0) {
                    addIfPresent(nodeIds, rs.getString(nodeIdColumn));
                }
                if (edgeIdColumn > 0) {
                    addIfPresent(edgeIds, rs.getString(edgeIdColumn));
                }
                if (fromNodeIdColumn > 0 && toNodeIdColumn > 0) {
                    String fromNodeId = trimToNull(rs.getString(fromNodeIdColumn));
                    String toNodeId = trimToNull(rs.getString(toNodeIdColumn));
                    if (fromNodeId != null && toNodeId != null) {
                        nodePairs.putIfAbsent(fromNodeId + "\u0000" + toNodeId, new NodePair(fromNodeId, toNodeId));
                    }
                }
            }

            return new SqlGraphQueryResult(
                new ArrayList<>(nodeIds),
                new ArrayList<>(edgeIds),
                new ArrayList<>(nodePairs.values()),
                rowCount
            );
        });
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

    private String anyField(String fields, String operator) {
        return fields.lines()
            .map(String::trim)
            .filter(field -> !field.isBlank())
            .map(field -> field.replaceAll(",$", "") + " " + operator)
            .reduce((left, right) -> left + " OR " + right)
            .orElse("FALSE");
    }

    private int fieldCount(String fields) {
        return (int) fields.lines()
            .map(String::trim)
            .filter(field -> !field.isBlank())
            .count();
    }

    private void addRepeated(List<Object> params, String value, int count) {
        for (int index = 0; index < count; index++) {
            params.add(value);
        }
    }

    private String escapeLike(String value) {
        return value
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_");
    }

    private String limitedSql(String sql, int limit) {
        return "SELECT * FROM (" + sql + ") graph_sql_query LIMIT " + limit;
    }

    private int firstColumnIndex(ResultSetMetaData metaData, String... names) throws SQLException {
        for (int column = 1; column <= metaData.getColumnCount(); column++) {
            String normalized = normalizeColumnLabel(metaData.getColumnLabel(column));
            for (String name : names) {
                if (normalized.equals(normalizeColumnLabel(name))) {
                    return column;
                }
            }
        }
        return -1;
    }

    private String normalizeColumnLabel(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT).replace("_", "");
    }

    private void addIfPresent(LinkedHashSet<String> values, String value) {
        String normalized = trimToNull(value);
        if (normalized != null) {
            values.add(normalized);
        }
    }

    private String trimToNull(String value) {
        if (value == null || value.trim().isBlank()) {
            return null;
        }
        return value.trim();
    }

}
