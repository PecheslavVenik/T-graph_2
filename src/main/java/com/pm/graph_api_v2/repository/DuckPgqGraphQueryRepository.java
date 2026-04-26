package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "DUCKPGQ", matchIfMissing = true)
public class DuckPgqGraphQueryRepository implements GraphQueryBackend {

    private static final String VERTEX_LABEL = "GraphNode";
    private static final String EDGE_LABEL = "GraphEdge";

    private final JdbcTemplate jdbcTemplate;
    private final DuckPgqResultSupport resultSupport;
    private final RowMapper<EdgeRow> edgeRowMapper;

    public DuckPgqGraphQueryRepository(JdbcTemplate jdbcTemplate,
                                       DuckPgqResultSupport resultSupport) {
        this.jdbcTemplate = jdbcTemplate;
        this.resultSupport = resultSupport;
        this.edgeRowMapper = resultSupport::mapEdgeRow;
    }

    @Override
    public GraphSource source() {
        return GraphSource.DUCKPGQ;
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

        return jdbcTemplate.execute((ConnectionCallback<List<EdgeRow>>) connection -> {
            if (!GraphRelationFamilies.isAllRelations(relationFamily) && !relationFamilyExists(connection, relationFamily)) {
                return List.of();
            }

            String sql = DuckPgqQueryBuilder.buildExpandQuery(
                GraphRelationFamilies.graphName(relationFamily),
                VERTEX_LABEL,
                EDGE_LABEL,
                seedNodeIds,
                edgeTypes,
                direction,
                candidateLimit
            );

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
                                              String relationFamily,
                                              Direction direction,
                                              int maxDepth) {
        return jdbcTemplate.execute((ConnectionCallback<Optional<PathRow>>) connection -> {
            if (!GraphRelationFamilies.isAllRelations(relationFamily) && !relationFamilyExists(connection, relationFamily)) {
                return Optional.empty();
            }

            String projectionTable = GraphRelationFamilies.projectionTableName(relationFamily);
            String sql = DuckPgqQueryBuilder.buildShortestPathQuery(
                GraphRelationFamilies.graphName(relationFamily),
                VERTEX_LABEL,
                EDGE_LABEL,
                sourceNodeId,
                targetNodeId,
                direction
            );

            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(sql)) {
                if (!rs.next()) {
                    return Optional.empty();
                }

                int hopCount = resultSupport.asInt(rs.getObject("hop_count"));
                if (hopCount > Math.max(1, maxDepth)) {
                    return Optional.empty();
                }

                List<Long> nodeRowIds = resultSupport.parseLongList(rs.getObject("vertices_rowid"));
                List<Long> edgeRowIds = resultSupport.parseLongList(rs.getObject("edges_rowid"));
                List<String> nodeIds = resultSupport.resolveNodeIdsByRowId(connection, nodeRowIds);
                List<String> edgeIds = resultSupport.resolveEdgeIdsByRowId(connection, projectionTable, edgeRowIds);
                if (nodeIds.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(new PathRow(nodeIds, edgeIds, hopCount));
            }
        });
    }

    private boolean relationFamilyExists(Connection connection, String relationFamily) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
            """
            SELECT 1
            FROM g_edges
            WHERE relation_family = ?
            LIMIT 1
            """
        )) {
            ps.setString(1, GraphRelationFamilies.normalize(relationFamily));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }
}
