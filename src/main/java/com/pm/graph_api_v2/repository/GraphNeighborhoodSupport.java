package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.repository.model.FacetCountRow;
import com.pm.graph_api_v2.repository.model.NodeNeighborhoodSummaryRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Component
public class GraphNeighborhoodSupport {

    private final JdbcTemplate jdbcTemplate;

    public GraphNeighborhoodSupport(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public NodeNeighborhoodSummaryRow summarizeNeighborhood(String nodeId,
                                                            String relationFamily,
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
                                                               String relationFamily,
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
                                                        String relationFamily,
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
                                                                String relationFamily,
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
                                                String relationFamily,
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
        if (!GraphRelationFamilies.isAllRelations(relationFamily)) {
            relationFilter = " AND relation_family = ?";
            params.add(GraphRelationFamilies.normalize(relationFamily));
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

    private FacetCountRow mapFacetCountRow(ResultSet rs, int ignored) throws SQLException {
        return new FacetCountRow(rs.getString("facet_key"), rs.getInt("facet_count"));
    }

    private record NeighborhoodQuery(String cteSql, Object[] params) {
    }

    private record NeighborhoodAggregate(int edgeCount, int uniqueNeighborCount) {
    }
}
