package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.DuckPgqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "DUCKPGQ", matchIfMissing = true)
public class DuckPgqRuntimeManager {

    private static final Logger log = LoggerFactory.getLogger(DuckPgqRuntimeManager.class);
    private static final String VERTEX_LABEL = "GraphNode";
    private static final String EDGE_LABEL = "GraphEdge";

    private final JdbcTemplate jdbcTemplate;
    private final DuckPgqProperties duckPgqProperties;
    private final DuckPgqProjectionManager projectionManager;

    public DuckPgqRuntimeManager(JdbcTemplate jdbcTemplate,
                                 DuckPgqProperties duckPgqProperties,
                                 DuckPgqProjectionManager projectionManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.duckPgqProperties = duckPgqProperties;
        this.projectionManager = projectionManager;
    }

    public void initialize() {
        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            loadDuckPgq(connection);
            if (duckPgqProperties.isSyncGraphStateOnStartup()) {
                projectionManager.recreateProjectionTables(connection);
                projectionManager.ensureGraphs(connection, VERTEX_LABEL, EDGE_LABEL);
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
                installStatement.execute("SET custom_extension_repository = " + DuckPgqQueryBuilder.sqlStringLiteral(duckPgqProperties.getRepositoryUrl()));
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
}
