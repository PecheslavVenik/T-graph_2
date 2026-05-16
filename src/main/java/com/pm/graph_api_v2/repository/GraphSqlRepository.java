package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.repository.model.NodePair;
import com.pm.graph_api_v2.repository.model.SqlGraphQueryResult;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

@Repository
public class GraphSqlRepository {

    private static final int QUERY_TIMEOUT_SECONDS = 5;

    private final JdbcTemplate jdbcTemplate;

    public GraphSqlRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
