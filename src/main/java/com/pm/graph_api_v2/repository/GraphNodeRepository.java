package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.SeedRef;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.util.GraphSeedTypes;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

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
public class GraphNodeRepository {

    private final JdbcTemplate jdbcTemplate;
    private final GraphRecordSupport graphRecordSupport;

    public GraphNodeRepository(JdbcTemplate jdbcTemplate,
                               GraphRecordSupport graphRecordSupport) {
        this.jdbcTemplate = jdbcTemplate;
        this.graphRecordSupport = graphRecordSupport;
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

    private Optional<String> queryNodeId(String sql, Object... params) {
        List<String> rows = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("node_id"), params);
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(rows.get(0));
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
}
