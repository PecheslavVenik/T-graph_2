package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Component
class DuckPgqProjectionManager {

    void recreateProjectionTables(Connection connection) throws SQLException {
        List<String> relationFamilies = listRelationFamilies(connection);

        createProjectionTableIfMissing(connection, GraphRelationFamilies.projectionTableName(GraphRelationFamilies.ALL_RELATIONS));
        for (String relationFamily : relationFamilies) {
            createProjectionTableIfMissing(connection, GraphRelationFamilies.projectionTableName(relationFamily));
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM " + GraphRelationFamilies.projectionTableName(GraphRelationFamilies.ALL_RELATIONS));
            for (String relationFamily : relationFamilies) {
                statement.execute("DELETE FROM " + GraphRelationFamilies.projectionTableName(relationFamily));
            }

            insertProjection(statement, GraphRelationFamilies.projectionTableName(GraphRelationFamilies.ALL_RELATIONS), null);
            for (String relationFamily : relationFamilies) {
                insertProjection(statement, GraphRelationFamilies.projectionTableName(relationFamily), relationFamily);
            }
        }
    }

    void ensureGraphs(Connection connection, String vertexLabel, String edgeLabel) throws SQLException {
        ensureGraph(
            connection,
            GraphRelationFamilies.graphName(GraphRelationFamilies.ALL_RELATIONS),
            GraphRelationFamilies.projectionTableName(GraphRelationFamilies.ALL_RELATIONS),
            vertexLabel,
            edgeLabel
        );
        for (String relationFamily : listRelationFamilies(connection)) {
            ensureGraph(
                connection,
                GraphRelationFamilies.graphName(relationFamily),
                GraphRelationFamilies.projectionTableName(relationFamily),
                vertexLabel,
                edgeLabel
            );
        }
    }

    private List<String> listRelationFamilies(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
            """
            SELECT DISTINCT relation_family
            FROM g_edges
            WHERE relation_family IS NOT NULL AND relation_family <> ''
            ORDER BY relation_family
            """
        );
             ResultSet rs = ps.executeQuery()) {
            List<String> families = new ArrayList<>();
            while (rs.next()) {
                families.add(GraphRelationFamilies.normalize(rs.getString(1)));
            }
            return families;
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
        String relationFilter = relationFamily == null ? "" : " WHERE relation_family = " + DuckPgqQueryBuilder.sqlStringLiteral(relationFamily);

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
            """.formatted(
                tableName,
                relationFamily == null ? "" : " AND relation_family = " + DuckPgqQueryBuilder.sqlStringLiteral(relationFamily)
            )
        );
    }

    private void ensureGraph(Connection connection,
                             String graphName,
                             String tableName,
                             String vertexLabel,
                             String edgeLabel) throws SQLException {
        try (Statement dropStatement = connection.createStatement()) {
            dropStatement.execute("DROP PROPERTY GRAPH IF EXISTS " + graphName);
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
                """.formatted(graphName, vertexLabel, tableName, edgeLabel)
            );
        }
    }
}
