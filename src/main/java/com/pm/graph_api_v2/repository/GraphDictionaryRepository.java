package com.pm.graph_api_v2.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class GraphDictionaryRepository {

    private final JdbcTemplate jdbcTemplate;

    public GraphDictionaryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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

    private Boolean queryExists(String sql, Object... params) {
        List<Integer> rows = jdbcTemplate.query(sql, (rs, rowNum) -> 1, params);
        return !rows.isEmpty();
    }
}
