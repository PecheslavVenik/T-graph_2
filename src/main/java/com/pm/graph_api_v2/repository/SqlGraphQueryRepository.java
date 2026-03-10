package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Repository
public class SqlGraphQueryRepository {

    private static final Logger log = LoggerFactory.getLogger(SqlGraphQueryRepository.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private final RowMapper<EdgeRow> edgeRowMapper = this::mapEdgeRow;

    public SqlGraphQueryRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                                         Direction direction,
                                                         List<String> edgeTypes) {
        if (seedNodeIds.isEmpty()) {
            return List.of();
        }

        StringBuilder sql = new StringBuilder(
            "SELECT edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum, attrs_json " +
                "FROM g_edges WHERE 1=1"
        );

        List<Object> args = new ArrayList<>();
        appendDirectionPredicate(sql, args, seedNodeIds, direction, "from_node_id", "to_node_id", "directed");
        appendEdgeTypeFilter(sql, args, edgeTypes, "edge_type");
        sql.append(" ORDER BY edge_id");

        return jdbcTemplate.query(sql.toString(), edgeRowMapper, args.toArray());
    }

    public List<EdgeRow> findNeighborEdges(Collection<String> frontierNodeIds,
                                                           Direction direction,
                                                           List<String> edgeTypes) {
        return findExpandEdges(frontierNodeIds, direction, edgeTypes);
    }

    private void appendDirectionPredicate(StringBuilder sql,
                                          List<Object> args,
                                          Collection<String> nodeIds,
                                          Direction direction,
                                          String fromColumn,
                                          String toColumn,
                                          String directedColumn) {
        String placeholders = placeholders(nodeIds.size());

        switch (direction) {
            case OUTBOUND -> {
                sql.append(" AND (")
                    .append(fromColumn).append(" IN (").append(placeholders).append(")")
                    .append(" OR (").append(directedColumn).append(" = FALSE AND ")
                    .append(toColumn).append(" IN (").append(placeholders).append(")))");
                args.addAll(nodeIds);
                args.addAll(nodeIds);
            }
            case INBOUND -> {
                sql.append(" AND (")
                    .append(toColumn).append(" IN (").append(placeholders).append(")")
                    .append(" OR (").append(directedColumn).append(" = FALSE AND ")
                    .append(fromColumn).append(" IN (").append(placeholders).append(")))");
                args.addAll(nodeIds);
                args.addAll(nodeIds);
            }
            case BOTH -> {
                sql.append(" AND (")
                    .append(fromColumn).append(" IN (").append(placeholders).append(")")
                    .append(" OR ")
                    .append(toColumn).append(" IN (").append(placeholders).append("))");
                args.addAll(nodeIds);
                args.addAll(nodeIds);
            }
        }
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
}
