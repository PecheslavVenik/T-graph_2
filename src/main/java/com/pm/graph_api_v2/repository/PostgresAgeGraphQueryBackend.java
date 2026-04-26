package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "POSTGRES_AGE")
public class PostgresAgeGraphQueryBackend implements GraphQueryBackend {

    private static final int PATH_BRANCH_LIMIT = 256;

    private final PostgresAgeConnectionFactory connectionFactory;

    public PostgresAgeGraphQueryBackend(PostgresAgeConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public GraphSource source() {
        return GraphSource.POSTGRES_AGE;
    }

    @Override
    public List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                         String relationFamily,
                                         List<String> edgeTypes,
                                         Direction direction,
                                         int candidateLimit) {
        if (seedNodeIds.isEmpty()) {
            return List.of();
        }

        String sql = """
            SELECT DISTINCT ON (edge_id)
                edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum,
                relation_family, strength_score, evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
            FROM %s
            WHERE projection_owner = ?
              AND (%s)
              AND (? OR relation_family = ?)
              AND (? OR edge_type = ANY(?))
            ORDER BY edge_id, COALESCE(strength_score, 0.0) DESC, COALESCE(evidence_count, 0) DESC
            LIMIT ?
            """.formatted(connectionFactory.table("graph_edges"), directionClause(direction));

        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            Array seedArray = connection.createArrayOf("text", seedNodeIds.toArray());
            Array edgeTypeArray = connection.createArrayOf("text", edgeTypes == null ? new String[0] : edgeTypes.toArray());
            boolean allRelations = GraphRelationFamilies.isAllRelations(relationFamily);
            boolean allEdgeTypes = edgeTypes == null || edgeTypes.isEmpty();

            ps.setString(1, Neo4jProjectionSupport.PROJECTION_OWNER);
            ps.setArray(2, seedArray);
            int index = 3;
            if (direction == Direction.BOTH) {
                ps.setArray(3, seedArray);
                index = 4;
            }
            ps.setBoolean(index++, allRelations);
            ps.setString(index++, GraphRelationFamilies.normalize(relationFamily));
            ps.setBoolean(index++, allEdgeTypes);
            ps.setArray(index++, edgeTypeArray);
            ps.setInt(index, Math.max(1, candidateLimit));

            try (ResultSet rs = ps.executeQuery()) {
                java.util.ArrayList<EdgeRow> rows = new java.util.ArrayList<>();
                while (rs.next()) {
                    rows.add(mapEdge(rs));
                }
                return rows;
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("PostgreSQL expand query failed", ex);
        }
    }

    @Override
    public Optional<PathRow> findShortestPath(String sourceNodeId,
                                              String targetNodeId,
                                              String relationFamily,
                                              Direction direction,
                                              int maxDepth) {
        if (direction == Direction.BOTH) {
            return GraphBackendPathSearch.breadthFirst(
                sourceNodeId,
                targetNodeId,
                relationFamily,
                direction,
                maxDepth,
                this::findExpandEdges
            );
        }

        String sql = """
            WITH RECURSIVE search(node_id, node_ids, edge_ids, depth) AS (
                SELECT
                    %1$s AS node_id,
                    ARRAY[?, %1$s]::text[] AS node_ids,
                    ARRAY[e.edge_id]::text[] AS edge_ids,
                    1 AS depth
                FROM (
                    SELECT *
                    FROM %2$s e
                    WHERE e.projection_owner = ?
                      AND %3$s
                      AND (? OR e.relation_family = ?)
                    ORDER BY COALESCE(e.strength_score, 0.0) DESC, COALESCE(e.evidence_count, 0) DESC, e.edge_id ASC
                    LIMIT %6$d
                ) e

                UNION ALL

                SELECT
                    next_node.next_node_id AS node_id,
                    s.node_ids || ARRAY[next_node.next_node_id]::text[] AS node_ids,
                    s.edge_ids || ARRAY[e.edge_id]::text[] AS edge_ids,
                    s.depth + 1 AS depth
                FROM search s
                JOIN LATERAL (
                    SELECT *
                    FROM %2$s e
                    WHERE e.projection_owner = ?
                      AND %4$s
                      AND (? OR e.relation_family = ?)
                    ORDER BY COALESCE(e.strength_score, 0.0) DESC, COALESCE(e.evidence_count, 0) DESC, e.edge_id ASC
                    LIMIT %6$d
                ) e ON true
                CROSS JOIN LATERAL (
                    SELECT %5$s AS next_node_id
                ) next_node
                WHERE s.depth < ?
                  AND NOT next_node.next_node_id = ANY(s.node_ids)
            )
            SELECT node_ids, edge_ids, depth AS hop_count
            FROM search
            WHERE node_id = ?
            ORDER BY depth
            LIMIT 1
            """.formatted(
            nextNodeExpression(direction, "e", sourceNodeId),
            connectionFactory.table("graph_edges"),
            initialDirectionClause(direction),
            recursiveDirectionClause(direction),
            nextNodeExpression(direction, "e", null),
            PATH_BRANCH_LIMIT
        );

        boolean allRelations = GraphRelationFamilies.isAllRelations(relationFamily);
        String normalizedRelationFamily = GraphRelationFamilies.normalize(relationFamily);
        int boundedDepth = Math.max(1, maxDepth);

        try (Connection connection = connectionFactory.openConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sourceNodeId);
            ps.setString(2, Neo4jProjectionSupport.PROJECTION_OWNER);
            ps.setString(3, sourceNodeId);
            int index = 4;
            if (direction == Direction.BOTH) {
                ps.setString(index++, sourceNodeId);
            }
            ps.setBoolean(index++, allRelations);
            ps.setString(index++, normalizedRelationFamily);
            ps.setString(index++, Neo4jProjectionSupport.PROJECTION_OWNER);
            ps.setBoolean(index++, allRelations);
            ps.setString(index++, normalizedRelationFamily);
            ps.setInt(index++, boundedDepth);
            ps.setString(index, targetNodeId);

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new PathRow(
                    stringArray(rs.getArray("node_ids")),
                    stringArray(rs.getArray("edge_ids")),
                    rs.getInt("hop_count")
                ));
            }
        } catch (SQLException ex) {
            throw new IllegalStateException("PostgreSQL shortest-path query failed", ex);
        }
    }

    private String directionClause(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "traversal_from_node_id = ANY(?)";
            case INBOUND -> "traversal_to_node_id = ANY(?)";
            case BOTH -> "(traversal_from_node_id = ANY(?) OR traversal_to_node_id = ANY(?))";
        };
    }

    private String initialDirectionClause(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "e.traversal_from_node_id = ?";
            case INBOUND -> "e.traversal_to_node_id = ?";
            case BOTH -> "(e.traversal_from_node_id = ? OR e.traversal_to_node_id = ?)";
        };
    }

    private String recursiveDirectionClause(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "e.traversal_from_node_id = s.node_id";
            case INBOUND -> "e.traversal_to_node_id = s.node_id";
            case BOTH -> "(e.traversal_from_node_id = s.node_id OR e.traversal_to_node_id = s.node_id)";
        };
    }

    private String nextNodeExpression(Direction direction, String edgeAlias, String seedNodeId) {
        return switch (direction) {
            case OUTBOUND -> edgeAlias + ".traversal_to_node_id";
            case INBOUND -> edgeAlias + ".traversal_from_node_id";
            case BOTH -> seedNodeId == null
                ? "CASE WHEN " + edgeAlias + ".traversal_from_node_id = s.node_id THEN " + edgeAlias + ".traversal_to_node_id ELSE " + edgeAlias + ".traversal_from_node_id END"
                : "CASE WHEN " + edgeAlias + ".traversal_from_node_id = ? THEN " + edgeAlias + ".traversal_to_node_id ELSE " + edgeAlias + ".traversal_from_node_id END";
        };
    }

    private List<String> stringArray(Array array) throws SQLException {
        if (array == null) {
            return List.of();
        }
        Object raw = array.getArray();
        if (raw instanceof String[] values) {
            return Arrays.asList(values);
        }
        return Arrays.stream((Object[]) raw)
            .map(String::valueOf)
            .toList();
    }

    private EdgeRow mapEdge(ResultSet rs) throws SQLException {
        Timestamp firstSeenAt = rs.getTimestamp("first_seen_at");
        Timestamp lastSeenAt = rs.getTimestamp("last_seen_at");
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
            firstSeenAt == null ? null : firstSeenAt.toInstant(),
            lastSeenAt == null ? null : lastSeenAt.toInstant(),
            Map.of()
        );
    }
}
