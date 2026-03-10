package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class DuckPgqGraphQueryRepository {

    private static final Logger log = LoggerFactory.getLogger(DuckPgqGraphQueryRepository.class);
    private static final String DUCKPGQ_GRAPH_NAME = "aml_graph";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private final RowMapper<EdgeRow> edgeRowMapper = this::mapEdgeRow;

    public DuckPgqGraphQueryRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public boolean isDuckPgqLoaded() {
        return jdbcTemplate.execute((ConnectionCallback<Boolean>) connection -> {
            tryLoadDuckPgq(connection);
            try (PreparedStatement ps = connection.prepareStatement(
                """
                SELECT loaded
                FROM duckdb_extensions()
                WHERE extension_name = 'duckpgq'
                LIMIT 1
                """
            )) {
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() && rs.getBoolean(1);
                }
            }
        });
    }

    public List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                         Direction direction,
                                         List<String> edgeTypes) {
        if (seedNodeIds.isEmpty()) {
            return List.of();
        }

        return jdbcTemplate.execute((ConnectionCallback<List<EdgeRow>>) connection -> {
            ensureDuckPgqReady(connection);

            ExpandQuery expandQuery = buildExpandQuery(seedNodeIds, direction, edgeTypes);
            try (PreparedStatement ps = connection.prepareStatement(expandQuery.sql())) {
                bindParameters(ps, expandQuery.args());
                try (ResultSet rs = ps.executeQuery()) {
                    List<EdgeRow> rows = new ArrayList<>();
                    int rowNum = 0;
                    while (rs.next()) {
                        rows.add(edgeRowMapper.mapRow(rs, rowNum++));
                    }
                    return rows;
                }
            }
        });
    }

    public Optional<PathRow> findShortestPath(String sourceNodeId,
                                              String targetNodeId,
                                              Direction direction,
                                              int maxDepth) {
        return jdbcTemplate.execute((ConnectionCallback<Optional<PathRow>>) connection -> {
            ensureDuckPgqReady(connection);

            String pattern = shortestPattern(direction);
            String sql = String.format(
                """
                WITH path_result AS (
                    FROM GRAPH_TABLE (
                      %s
                      MATCH p = ANY SHORTEST (a WHERE a.node_id = ?)%s{1,%d}(b WHERE b.node_id = ?)
                      COLUMNS (
                        vertices(p) AS vertices_rowid,
                        edges(p) AS edges_rowid,
                        path_length(p) AS hop_count
                      )
                    )
                    ORDER BY hop_count
                    LIMIT 1
                ),
                path_nodes AS (
                    SELECT list(n.node_id ORDER BY u.ord) AS node_ids
                    FROM path_result pr
                    CROSS JOIN UNNEST(pr.vertices_rowid) WITH ORDINALITY AS u(node_rowid, ord)
                    JOIN g_nodes n ON n.rowid = u.node_rowid
                ),
                path_edges AS (
                    SELECT list(e.edge_id ORDER BY u.ord) AS edge_ids
                    FROM path_result pr
                    CROSS JOIN UNNEST(pr.edges_rowid) WITH ORDINALITY AS u(edge_rowid, ord)
                    JOIN g_pgq_edges e ON e.rowid = u.edge_rowid
                )
                SELECT pn.node_ids, pe.edge_ids, pr.hop_count
                FROM path_result pr
                JOIN path_nodes pn ON TRUE
                JOIN path_edges pe ON TRUE
                """,
                DUCKPGQ_GRAPH_NAME,
                pattern,
                maxDepth
            );

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, sourceNodeId);
                ps.setString(2, targetNodeId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }

                    List<String> nodeIds = parseTextList(rs.getObject("node_ids"));
                    List<String> edgeIds = parseTextList(rs.getObject("edge_ids"));
                    int hopCount = asInt(rs.getObject("hop_count"));
                    if (nodeIds.isEmpty()) {
                        return Optional.empty();
                    }
                    return Optional.of(new PathRow(nodeIds, edgeIds, hopCount));
                }
            }
        });
    }

    private ExpandQuery buildExpandQuery(Collection<String> seedNodeIds,
                                         Direction direction,
                                         List<String> edgeTypes) {
        StringBuilder sql = new StringBuilder(
            """
            WITH matched_edges AS (
                SELECT DISTINCT edge_id, edge_type
                FROM GRAPH_TABLE (
                  aml_graph
            """
        );

        switch (direction) {
            case OUTBOUND -> sql.append(
                """
                      MATCH (a)-[e]->(b)
                      COLUMNS (
                        a.node_id AS anchor_node_id,
                        e.edge_id AS edge_id,
                        e.edge_type AS edge_type
                      )
                    ) gt
                    WHERE gt.anchor_node_id IN (
                """
            );
            case INBOUND -> sql.append(
                """
                      MATCH (a)-[e]->(b)
                      COLUMNS (
                        b.node_id AS anchor_node_id,
                        e.edge_id AS edge_id,
                        e.edge_type AS edge_type
                      )
                    ) gt
                    WHERE gt.anchor_node_id IN (
                """
            );
            case BOTH -> sql.append(
                """
                      MATCH (a)-[e]-(b)
                      COLUMNS (
                        a.node_id AS from_anchor_node_id,
                        b.node_id AS to_anchor_node_id,
                        e.edge_id AS edge_id,
                        e.edge_type AS edge_type
                      )
                    ) gt
                    WHERE (
                      gt.from_anchor_node_id IN (
                """
            );
        }

        List<Object> args = new ArrayList<>();
        String placeholders = placeholders(seedNodeIds.size());
        sql.append(placeholders).append(")");
        args.addAll(seedNodeIds);

        if (direction == Direction.BOTH) {
            sql.append(" OR gt.to_anchor_node_id IN (").append(placeholders).append("))");
            args.addAll(seedNodeIds);
        }

        appendEdgeTypeFilter(sql, args, edgeTypes, "gt.edge_type");
        sql.append(
            """
            )
            SELECT ge.edge_id, ge.from_node_id, ge.to_node_id, ge.edge_type, ge.directed, ge.tx_count, ge.tx_sum, ge.attrs_json
            FROM g_edges ge
            JOIN matched_edges me ON me.edge_id = ge.edge_id
            ORDER BY ge.edge_id
            """
        );

        return new ExpandQuery(sql.toString(), args);
    }

    private void ensureDuckPgqReady(Connection connection) throws SQLException {
        if (!tryLoadDuckPgq(connection) && !isExtensionLoaded(connection)) {
            throw new SQLException("duckpgq extension is not loaded on current connection");
        }
        refreshPgqEdgeProjectionIfStale(connection);
        ensureDuckPgqGraph(connection);
    }

    private void refreshPgqEdgeProjectionIfStale(Connection connection) throws SQLException {
        ProjectionState state = projectionState(connection);
        if (!state.stale()) {
            return;
        }

        log.info("Refreshing g_pgq_edges projection: expected={}, actual={}", state.expectedCount(), state.actualCount());
        try (Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM g_pgq_edges");
            statement.execute(
                """
                INSERT INTO g_pgq_edges (
                    pgq_edge_id,
                    edge_id,
                    from_node_id,
                    to_node_id,
                    traversal_from_node_id,
                    traversal_to_node_id,
                    edge_type,
                    directed,
                    tx_count,
                    tx_sum,
                    attrs_json,
                    created_at,
                    updated_at
                )
                SELECT
                    edge_id || ':fwd',
                    edge_id,
                    from_node_id,
                    to_node_id,
                    from_node_id,
                    to_node_id,
                    edge_type,
                    directed,
                    tx_count,
                    tx_sum,
                    attrs_json,
                    created_at,
                    updated_at
                FROM g_edges
                """
            );
            statement.execute(
                """
                INSERT INTO g_pgq_edges (
                    pgq_edge_id,
                    edge_id,
                    from_node_id,
                    to_node_id,
                    traversal_from_node_id,
                    traversal_to_node_id,
                    edge_type,
                    directed,
                    tx_count,
                    tx_sum,
                    attrs_json,
                    created_at,
                    updated_at
                )
                SELECT
                    edge_id || ':rev',
                    edge_id,
                    from_node_id,
                    to_node_id,
                    to_node_id,
                    from_node_id,
                    edge_type,
                    directed,
                    tx_count,
                    tx_sum,
                    attrs_json,
                    created_at,
                    updated_at
                FROM g_edges
                WHERE directed = FALSE
                """
            );
        }
    }

    private ProjectionState projectionState(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
            """
            WITH expected AS (
              SELECT
                COALESCE(COUNT(*), 0) + COALESCE(SUM(CASE WHEN directed = FALSE THEN 1 ELSE 0 END), 0) AS expected_count,
                COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') AS expected_updated_at
              FROM g_edges
            ),
            actual AS (
              SELECT
                COALESCE(COUNT(*), 0) AS actual_count,
                COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01') AS actual_updated_at
              FROM g_pgq_edges
            )
            SELECT expected_count, actual_count, expected_updated_at, actual_updated_at
            FROM expected
            CROSS JOIN actual
            """
        )) {
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return new ProjectionState(0, 0, false);
                }

                long expectedCount = rs.getLong("expected_count");
                long actualCount = rs.getLong("actual_count");
                Timestamp expectedUpdatedAt = rs.getTimestamp("expected_updated_at");
                Timestamp actualUpdatedAt = rs.getTimestamp("actual_updated_at");

                boolean stale = expectedCount != actualCount
                    || (expectedUpdatedAt != null && actualUpdatedAt != null && expectedUpdatedAt.after(actualUpdatedAt));
                return new ProjectionState(expectedCount, actualCount, stale);
            }
        }
    }

    private boolean tryLoadDuckPgq(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("LOAD duckpgq");
            return true;
        } catch (SQLException ex) {
            log.debug("LOAD duckpgq on active connection failed: {}", ex.getMessage());
            return false;
        }
    }

    private boolean isExtensionLoaded(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
            """
            SELECT loaded
            FROM duckdb_extensions()
            WHERE extension_name = 'duckpgq'
            LIMIT 1
            """
        )) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private void ensureDuckPgqGraph(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                String.format(
                    """
                    CREATE PROPERTY GRAPH IF NOT EXISTS %s (
                      VERTEX TABLE g_nodes KEY (node_id),
                      EDGE TABLE g_pgq_edges KEY (pgq_edge_id)
                        SOURCE KEY (traversal_from_node_id) REFERENCES g_nodes (node_id)
                        DESTINATION KEY (traversal_to_node_id) REFERENCES g_nodes (node_id)
                    )
                    """,
                    DUCKPGQ_GRAPH_NAME
                )
            );
        }
    }

    private String shortestPattern(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "-[e]->";
            case INBOUND -> "<-[e]-";
            case BOTH -> "-[e]-";
        };
    }

    private void appendEdgeTypeFilter(StringBuilder sql,
                                      List<Object> args,
                                      List<String> edgeTypes,
                                      String columnName) {
        List<String> normalized = normalizeEdgeTypes(edgeTypes);
        if (normalized.isEmpty()) {
            return;
        }

        sql.append(" AND ").append(columnName).append(" IN (")
            .append(placeholders(normalized.size()))
            .append(")");
        args.addAll(normalized);
    }

    private void bindParameters(PreparedStatement ps, List<Object> args) throws SQLException {
        for (int i = 0; i < args.size(); i++) {
            ps.setObject(i + 1, args.get(i));
        }
    }

    private String placeholders(int count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

    private List<String> normalizeEdgeTypes(List<String> edgeTypes) {
        if (edgeTypes == null || edgeTypes.isEmpty()) {
            return List.of();
        }

        return edgeTypes.stream()
            .filter(value -> value != null && !value.isBlank())
            .map(String::trim)
            .toList();
    }

    private int asInt(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private List<String> parseTextList(Object value) {
        if (value == null) {
            return List.of();
        }

        if (value instanceof Array sqlArray) {
            try {
                Object rawArray = sqlArray.getArray();
                if (rawArray instanceof Object[] array) {
                    return Arrays.stream(array)
                        .map(String::valueOf)
                        .toList();
                }
            } catch (SQLException ex) {
                log.debug("Failed to parse SQL array: {}", ex.getMessage());
            }
        }

        if (value instanceof List<?> list) {
            return list.stream().map(String::valueOf).toList();
        }

        String raw = value.toString().trim();
        if (raw.isBlank()) {
            return List.of();
        }

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
            .toList();
    }

    private String stripQuotes(String value) {
        if (value.length() >= 2) {
            if ((value.startsWith("\"") && value.endsWith("\""))
                || (value.startsWith("'") && value.endsWith("'"))) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
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

    private record ExpandQuery(String sql, List<Object> args) {
    }

    private record ProjectionState(long expectedCount, long actualCount, boolean stale) {
    }
}
