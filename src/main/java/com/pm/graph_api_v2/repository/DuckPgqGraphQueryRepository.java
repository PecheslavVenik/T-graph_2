package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphRelationFamily;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.config.DuckPgqProperties;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "DUCKPGQ", matchIfMissing = true)
public class DuckPgqGraphQueryRepository implements GraphQueryBackend {

    private static final Logger log = LoggerFactory.getLogger(DuckPgqGraphQueryRepository.class);
    private static final String VERTEX_LABEL = "Person";
    private static final String EDGE_LABEL = "Connection";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final DuckPgqProperties duckPgqProperties;
    private final RowMapper<EdgeRow> edgeRowMapper = this::mapEdgeRow;

    public DuckPgqGraphQueryRepository(JdbcTemplate jdbcTemplate,
                                      ObjectMapper objectMapper,
                                      DuckPgqProperties duckPgqProperties) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.duckPgqProperties = duckPgqProperties;
    }

    @Override
    public GraphSource source() {
        return GraphSource.DUCKPGQ;
    }

    public void initialize() {
        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            loadDuckPgq(connection);
            if (duckPgqProperties.isSyncGraphStateOnStartup()) {
                recreateProjectionTables(connection);
                ensureGraphs(connection);
            } else {
                log.info("duckpgq graph-state synchronization is skipped at startup");
            }
            return null;
        });
    }

    public boolean isDuckPgqLoaded() {
        return jdbcTemplate.execute((ConnectionCallback<Boolean>) connection -> {
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

    @Override
    public List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                         GraphRelationFamily relationFamily,
                                         Direction direction,
                                         int candidateLimit) {
        if (seedNodeIds.isEmpty()) {
            return List.of();
        }

        return jdbcTemplate.execute((ConnectionCallback<List<EdgeRow>>) connection -> {
            String sql = buildExpandQuery(graphName(relationFamily), seedNodeIds, direction, candidateLimit);
            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(sql)) {
                List<EdgeRow> rows = new ArrayList<>();
                int rowNum = 0;
                while (rs.next()) {
                    rows.add(edgeRowMapper.mapRow(rs, rowNum++));
                }
                return rows;
            }
        });
    }

    @Override
    public Optional<PathRow> findShortestPath(String sourceNodeId,
                                              String targetNodeId,
                                              GraphRelationFamily relationFamily,
                                              Direction direction,
                                              int maxDepth) {
        return jdbcTemplate.execute((ConnectionCallback<Optional<PathRow>>) connection -> {
            String projectionTable = projectionTable(relationFamily);
            String sql = buildShortestPathQuery(
                graphName(relationFamily),
                sourceNodeId,
                targetNodeId,
                direction
            );

            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(sql)) {
                if (!rs.next()) {
                    return Optional.empty();
                }

                int hopCount = asInt(rs.getObject("hop_count"));
                if (hopCount > Math.max(1, maxDepth)) {
                    return Optional.empty();
                }

                List<Long> nodeRowIds = parseLongList(rs.getObject("vertices_rowid"));
                List<Long> edgeRowIds = parseLongList(rs.getObject("edges_rowid"));
                List<String> nodeIds = resolveNodeIdsByRowId(connection, nodeRowIds);
                List<String> edgeIds = resolveEdgeIdsByRowId(connection, projectionTable, edgeRowIds);
                if (nodeIds.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(new PathRow(nodeIds, edgeIds, hopCount));
            }
        });
    }

    private String buildShortestPathQuery(String graphName,
                                          String sourceNodeId,
                                          String targetNodeId,
                                          Direction direction) {
        return """
            FROM GRAPH_TABLE (
              %s
              MATCH p = ANY SHORTEST (a:%s WHERE a.node_id = %s)%s+(b:%s WHERE b.node_id = %s)
              COLUMNS (
                vertices(p) AS vertices_rowid,
                edges(p) AS edges_rowid,
                path_length(p) AS hop_count
              )
            )
            SELECT *
            """.formatted(
            graphName,
            VERTEX_LABEL,
            sqlStringLiteral(sourceNodeId),
            shortestPattern(direction),
            VERTEX_LABEL,
            sqlStringLiteral(targetNodeId)
        );
    }

    private String buildExpandQuery(String graphName,
                                    Collection<String> seedNodeIds,
                                    Direction direction,
                                    int candidateLimit) {
        String seedFilter = inList(seedNodeIds);
        String matchPattern = switch (direction) {
            case OUTBOUND, INBOUND -> "MATCH (a:%s)-[e:%s]->(b:%s)".formatted(VERTEX_LABEL, EDGE_LABEL, VERTEX_LABEL);
            case BOTH -> "MATCH (a:%s)-[e:%s]-(b:%s)".formatted(VERTEX_LABEL, EDGE_LABEL, VERTEX_LABEL);
        };
        String whereClause = switch (direction) {
            case OUTBOUND -> "WHERE a.node_id IN (%s)".formatted(seedFilter);
            case INBOUND -> "WHERE b.node_id IN (%s)".formatted(seedFilter);
            case BOTH -> "WHERE a.node_id IN (%s) OR b.node_id IN (%s)".formatted(seedFilter, seedFilter);
        };

        return """
            FROM GRAPH_TABLE (
              %s
              %s
              %s
              COLUMNS (
                e.edge_id AS edge_id,
                e.from_node_id AS from_node_id,
                e.to_node_id AS to_node_id,
                e.edge_type AS edge_type,
                e.directed AS directed,
                e.tx_count AS tx_count,
                e.tx_sum AS tx_sum,
                e.relation_family AS relation_family,
                e.strength_score AS strength_score,
                e.evidence_count AS evidence_count,
                e.source_system AS source_system,
                e.first_seen_at AS first_seen_at,
                e.last_seen_at AS last_seen_at,
                e.attrs_json AS attrs_json
              )
            )
            SELECT DISTINCT *
            ORDER BY strength_score DESC, evidence_count DESC, edge_id
            LIMIT %d
            """.formatted(
            graphName,
            matchPattern,
            whereClause,
            Math.max(1, candidateLimit)
        );
    }

    private String inList(Collection<String> values) {
        return values.stream()
            .map(this::sqlStringLiteral)
            .collect(Collectors.joining(","));
    }

    private String sqlStringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private void loadDuckPgq(Connection connection) throws SQLException {
        try (Statement loadStatement = connection.createStatement()) {
            try {
                loadStatement.execute("LOAD duckpgq");
                log.info("duckpgq extension loaded successfully");
                return;
            } catch (SQLException loadException) {
                log.info("LOAD duckpgq failed, trying install flow: {}", loadException.getMessage());
            }
        }

        if (duckPgqProperties.isPreferLatest()) {
            try (Statement installStatement = connection.createStatement()) {
                installStatement.execute("SET custom_extension_repository = '" + duckPgqProperties.getRepositoryUrl() + "'");
                installStatement.execute(duckPgqProperties.isForceInstall() ? "FORCE INSTALL duckpgq" : "INSTALL duckpgq");
            }
            try (Statement loadStatement = connection.createStatement()) {
                loadStatement.execute("LOAD duckpgq");
                log.info("duckpgq extension installed and loaded successfully from latest repository");
                return;
            } catch (SQLException latestLoadException) {
                log.warn("Latest duckpgq install/load failed, falling back to community repository: {}", latestLoadException.getMessage());
            }
        }

        try (Statement installStatement = connection.createStatement()) {
            installStatement.execute("INSTALL duckpgq FROM community");
        }
        try (Statement loadStatement = connection.createStatement()) {
            loadStatement.execute("LOAD duckpgq");
            log.info("duckpgq extension installed and loaded successfully from community repository");
        }
    }

    private void recreateProjectionTables(Connection connection) throws SQLException {
        createProjectionTableIfMissing(connection, GraphRelationFamily.ALL_RELATIONS.projectionTableName());
        for (GraphRelationFamily relationFamily : GraphRelationFamily.values()) {
            if (!relationFamily.isAllRelations()) {
                createProjectionTableIfMissing(connection, relationFamily.projectionTableName());
            }
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM " + GraphRelationFamily.ALL_RELATIONS.projectionTableName());
            for (GraphRelationFamily relationFamily : GraphRelationFamily.values()) {
                if (!relationFamily.isAllRelations()) {
                    statement.execute("DELETE FROM " + relationFamily.projectionTableName());
                }
            }

            insertProjection(statement, GraphRelationFamily.ALL_RELATIONS.projectionTableName(), null);
            for (GraphRelationFamily relationFamily : GraphRelationFamily.values()) {
                if (!relationFamily.isAllRelations()) {
                    insertProjection(statement, relationFamily.projectionTableName(), relationFamily.name());
                }
            }
        }
    }

    private void createProjectionTableIfMissing(Connection connection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                """
                CREATE TABLE IF NOT EXISTS %s (
                    pgq_edge_id VARCHAR PRIMARY KEY,
                    edge_id VARCHAR NOT NULL,
                    from_node_id VARCHAR NOT NULL,
                    to_node_id VARCHAR NOT NULL,
                    traversal_from_node_id VARCHAR NOT NULL,
                    traversal_to_node_id VARCHAR NOT NULL,
                    edge_type VARCHAR NOT NULL,
                    directed BOOLEAN DEFAULT TRUE,
                    tx_count BIGINT DEFAULT 0,
                    tx_sum DOUBLE DEFAULT 0,
                    relation_family VARCHAR,
                    strength_score DOUBLE DEFAULT 0,
                    evidence_count BIGINT DEFAULT 0,
                    source_system VARCHAR,
                    first_seen_at TIMESTAMP,
                    last_seen_at TIMESTAMP,
                    attrs_json VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """.formatted(tableName)
            );
            statement.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS relation_family VARCHAR");
            statement.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS strength_score DOUBLE DEFAULT 0");
            statement.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS evidence_count BIGINT DEFAULT 0");
            statement.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS source_system VARCHAR");
            statement.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP");
            statement.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP");
        }
    }

    private void insertProjection(Statement statement, String tableName, String relationFamily) throws SQLException {
        String relationFilter = relationFamily == null ? "" : " WHERE relation_family = '" + relationFamily + "'";

        statement.execute(
            """
            INSERT INTO %s (
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
                relation_family,
                strength_score,
                evidence_count,
                source_system,
                first_seen_at,
                last_seen_at,
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
                relation_family,
                strength_score,
                evidence_count,
                source_system,
                first_seen_at,
                last_seen_at,
                attrs_json,
                created_at,
                updated_at
            FROM g_edges%s
            """.formatted(tableName, relationFilter)
        );

        statement.execute(
            """
            INSERT INTO %s (
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
                relation_family,
                strength_score,
                evidence_count,
                source_system,
                first_seen_at,
                last_seen_at,
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
                relation_family,
                strength_score,
                evidence_count,
                source_system,
                first_seen_at,
                last_seen_at,
                attrs_json,
                created_at,
                updated_at
            FROM g_edges
            WHERE directed = FALSE%s
            """.formatted(tableName, relationFamily == null ? "" : " AND relation_family = '" + relationFamily + "'")
        );
    }

    private void ensureGraphs(Connection connection) throws SQLException {
        for (GraphRelationFamily relationFamily : GraphRelationFamily.values()) {
            ensureGraph(connection, relationFamily.graphName(), relationFamily.projectionTableName());
        }
    }

    private void ensureGraph(Connection connection, String graphName, String tableName) throws SQLException {
        try (Statement dropStatement = connection.createStatement()) {
            try {
                dropStatement.execute("DROP PROPERTY GRAPH " + graphName);
            } catch (SQLException ex) {
                log.debug("Property graph {} was not dropped before recreation: {}", graphName, ex.getMessage());
            }
        }

        try (Statement createStatement = connection.createStatement()) {
            createStatement.execute(
                """
                CREATE PROPERTY GRAPH %s
                  VERTEX TABLES (
                    g_nodes LABEL %s
                  )
                  EDGE TABLES (
                    %s
                    SOURCE KEY (traversal_from_node_id) REFERENCES g_nodes (node_id)
                    DESTINATION KEY (traversal_to_node_id) REFERENCES g_nodes (node_id)
                    LABEL %s
                  )
                """.formatted(graphName, VERTEX_LABEL, tableName, EDGE_LABEL)
            );
        }
    }

    private String graphName(GraphRelationFamily relationFamily) {
        return relationFamily.graphName();
    }

    private String projectionTable(GraphRelationFamily relationFamily) {
        return relationFamily.projectionTableName();
    }

    private String shortestPattern(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "-[e:%s]->".formatted(EDGE_LABEL);
            case INBOUND -> "<-[e:%s]-".formatted(EDGE_LABEL);
            case BOTH -> "-[e:%s]-".formatted(EDGE_LABEL);
        };
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

    private List<Long> parseLongList(Object value) {
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

    private List<String> parseTextList(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof Array sqlArray) {
            try {
                Object rawArray = sqlArray.getArray();
                if (rawArray instanceof Object[] array) {
                    return Arrays.stream(array).map(String::valueOf).toList();
                }
            } catch (SQLException ex) {
                log.debug("Failed to parse SQL array: {}", ex.getMessage());
            }
        }
        if (value instanceof List<?> list) {
            return list.stream().map(String::valueOf).toList();
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
            .toList();
    }

    private String stripQuotes(String value) {
        if (value.length() >= 2 && ((value.startsWith("\"") && value.endsWith("\""))
            || (value.startsWith("'") && value.endsWith("'")))) {
            return value.substring(1, value.length() - 1);
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
            rs.getString("relation_family"),
            rs.getDouble("strength_score"),
            rs.getLong("evidence_count"),
            rs.getString("source_system"),
            rs.getTimestamp("first_seen_at") == null ? null : rs.getTimestamp("first_seen_at").toInstant(),
            rs.getTimestamp("last_seen_at") == null ? null : rs.getTimestamp("last_seen_at").toInstant(),
            readJsonMap(rs.getString("attrs_json"))
        );
    }

    private List<String> resolveNodeIdsByRowId(Connection connection, List<Long> rowIds) throws SQLException {
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

    private List<String> resolveEdgeIdsByRowId(Connection connection, String tableName, List<Long> rowIds) throws SQLException {
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

}
