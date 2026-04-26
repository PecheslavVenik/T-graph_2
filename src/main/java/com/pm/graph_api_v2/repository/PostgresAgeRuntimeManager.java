package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.PostgresAgeProperties;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "POSTGRES_AGE")
public class PostgresAgeRuntimeManager {

    private static final Logger log = LoggerFactory.getLogger(PostgresAgeRuntimeManager.class);
    private static final int SYNC_BATCH_SIZE = 500;

    private final PostgresAgeConnectionFactory connectionFactory;
    private final GraphRepository graphRepository;
    private final PostgresAgeProperties properties;

    public PostgresAgeRuntimeManager(PostgresAgeConnectionFactory connectionFactory,
                                     GraphRepository graphRepository,
                                     PostgresAgeProperties properties) {
        this.connectionFactory = connectionFactory;
        this.graphRepository = graphRepository;
        this.properties = properties;
    }

    public void initialize() throws SQLException {
        try (Connection connection = connectionFactory.openConnection()) {
            ensureSchema(connection);
            if (!properties.isSyncGraphStateOnStartup()) {
                log.info("postgres-age graph-state synchronization is skipped at startup");
                return;
            }
            if (properties.isClearProjectionOnStartup()) {
                clearProjection(connection);
            }
            syncNodes(connection);
            syncEdges(connection);
        }
    }

    public boolean isAvailable() {
        try (Connection connection = connectionFactory.openConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
            return true;
        } catch (Exception ex) {
            log.debug("PostgreSQL connectivity check failed: {}", ex.getMessage());
            return false;
        }
    }

    private void ensureSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + connectionFactory.schema());
            ensureAgeExtensionIfAvailable(statement);
            statement.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    node_id TEXT PRIMARY KEY,
                    projection_owner TEXT NOT NULL,
                    node_type TEXT,
                    display_name TEXT,
                    party_rk TEXT,
                    person_id TEXT,
                    phone_no TEXT,
                    full_name TEXT,
                    is_blacklist BOOLEAN,
                    is_vip BOOLEAN,
                    employer TEXT,
                    city TEXT,
                    source_system TEXT,
                    pagerank_score DOUBLE PRECISION,
                    hub_score DOUBLE PRECISION,
                    attrs_json TEXT
                )
                """.formatted(connectionFactory.table("graph_nodes")));
            statement.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    graph_rel_id TEXT PRIMARY KEY,
                    projection_owner TEXT NOT NULL,
                    edge_id TEXT NOT NULL,
                    from_node_id TEXT NOT NULL,
                    to_node_id TEXT NOT NULL,
                    traversal_from_node_id TEXT NOT NULL,
                    traversal_to_node_id TEXT NOT NULL,
                    edge_type TEXT,
                    directed BOOLEAN,
                    tx_count BIGINT,
                    tx_sum DOUBLE PRECISION,
                    relation_family TEXT,
                    strength_score DOUBLE PRECISION,
                    evidence_count BIGINT,
                    source_system TEXT,
                    first_seen_at TIMESTAMPTZ,
                    last_seen_at TIMESTAMPTZ,
                    attrs_json TEXT
                )
                """.formatted(connectionFactory.table("graph_edges")));
            statement.execute("CREATE INDEX IF NOT EXISTS graph_edges_from_idx ON " + connectionFactory.table("graph_edges") + " (traversal_from_node_id)");
            statement.execute("CREATE INDEX IF NOT EXISTS graph_edges_to_idx ON " + connectionFactory.table("graph_edges") + " (traversal_to_node_id)");
            statement.execute("CREATE INDEX IF NOT EXISTS graph_edges_family_idx ON " + connectionFactory.table("graph_edges") + " (relation_family)");
        }
    }

    private void ensureAgeExtensionIfAvailable(Statement statement) throws SQLException {
        try {
            statement.execute("CREATE EXTENSION IF NOT EXISTS age");
        } catch (SQLException ex) {
            log.warn("Apache AGE extension is not available; continuing with PostgreSQL projection-table backend: {}", ex.getMessage());
        }
    }

    private void clearProjection(Connection connection) throws SQLException {
        try (PreparedStatement deleteEdges = connection.prepareStatement(
            "DELETE FROM " + connectionFactory.table("graph_edges") + " WHERE projection_owner = ?"
        );
             PreparedStatement deleteNodes = connection.prepareStatement(
                 "DELETE FROM " + connectionFactory.table("graph_nodes") + " WHERE projection_owner = ?"
             )) {
            deleteEdges.setString(1, Neo4jProjectionSupport.PROJECTION_OWNER);
            deleteEdges.executeUpdate();
            deleteNodes.setString(1, Neo4jProjectionSupport.PROJECTION_OWNER);
            deleteNodes.executeUpdate();
        }
    }

    private void syncNodes(Connection connection) throws SQLException {
        AtomicLong synced = new AtomicLong();
        String sql = """
            INSERT INTO %s (
                node_id, projection_owner, node_type, display_name, party_rk, person_id, phone_no, full_name,
                is_blacklist, is_vip, employer, city, source_system, pagerank_score, hub_score, attrs_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (node_id) DO UPDATE SET
                projection_owner = EXCLUDED.projection_owner,
                node_type = EXCLUDED.node_type,
                display_name = EXCLUDED.display_name,
                party_rk = EXCLUDED.party_rk,
                person_id = EXCLUDED.person_id,
                phone_no = EXCLUDED.phone_no,
                full_name = EXCLUDED.full_name,
                is_blacklist = EXCLUDED.is_blacklist,
                is_vip = EXCLUDED.is_vip,
                employer = EXCLUDED.employer,
                city = EXCLUDED.city,
                source_system = EXCLUDED.source_system,
                pagerank_score = EXCLUDED.pagerank_score,
                hub_score = EXCLUDED.hub_score,
                attrs_json = EXCLUDED.attrs_json
            """.formatted(connectionFactory.table("graph_nodes"));

        graphRepository.forEachNodeBatch(SYNC_BATCH_SIZE, nodes -> {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (NodeRow node : nodes) {
                    bindNode(ps, node);
                    ps.addBatch();
                }
                ps.executeBatch();
                long total = synced.addAndGet(nodes.size());
                if (total % 50_000 == 0) {
                    log.info("postgres-age projection nodes synced: {}", total);
                }
            } catch (SQLException ex) {
                throw new IllegalStateException("Failed to sync PostgreSQL node batch", ex);
            }
        });
        log.info("postgres-age projection nodes synced: {}", synced.get());
    }

    private void syncEdges(Connection connection) throws SQLException {
        AtomicLong sourceEdges = new AtomicLong();
        AtomicLong projected = new AtomicLong();
        String sql = """
            INSERT INTO %s (
                graph_rel_id, projection_owner, edge_id, from_node_id, to_node_id, traversal_from_node_id,
                traversal_to_node_id, edge_type, directed, tx_count, tx_sum, relation_family, strength_score,
                evidence_count, source_system, first_seen_at, last_seen_at, attrs_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (graph_rel_id) DO UPDATE SET
                projection_owner = EXCLUDED.projection_owner,
                edge_id = EXCLUDED.edge_id,
                from_node_id = EXCLUDED.from_node_id,
                to_node_id = EXCLUDED.to_node_id,
                traversal_from_node_id = EXCLUDED.traversal_from_node_id,
                traversal_to_node_id = EXCLUDED.traversal_to_node_id,
                edge_type = EXCLUDED.edge_type,
                directed = EXCLUDED.directed,
                tx_count = EXCLUDED.tx_count,
                tx_sum = EXCLUDED.tx_sum,
                relation_family = EXCLUDED.relation_family,
                strength_score = EXCLUDED.strength_score,
                evidence_count = EXCLUDED.evidence_count,
                source_system = EXCLUDED.source_system,
                first_seen_at = EXCLUDED.first_seen_at,
                last_seen_at = EXCLUDED.last_seen_at,
                attrs_json = EXCLUDED.attrs_json
            """.formatted(connectionFactory.table("graph_edges"));

        graphRepository.forEachEdgeBatch(SYNC_BATCH_SIZE, edges -> {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (EdgeRow edge : edges) {
                    bindEdge(ps, edge, false);
                    ps.addBatch();
                    projected.incrementAndGet();
                    if (!edge.directed()) {
                        bindEdge(ps, edge, true);
                        ps.addBatch();
                        projected.incrementAndGet();
                    }
                }
                ps.executeBatch();
                long total = sourceEdges.addAndGet(edges.size());
                if (total % 50_000 == 0) {
                    log.info("postgres-age projection source edges scanned: {}, relationships synced: {}", total, projected.get());
                }
            } catch (SQLException ex) {
                throw new IllegalStateException("Failed to sync PostgreSQL edge batch", ex);
            }
        });
        log.info("postgres-age projection source edges scanned: {}, relationships synced: {}", sourceEdges.get(), projected.get());
    }

    private void bindNode(PreparedStatement ps, NodeRow row) throws SQLException {
        ps.setString(1, row.nodeId());
        ps.setString(2, Neo4jProjectionSupport.PROJECTION_OWNER);
        ps.setString(3, row.nodeType());
        ps.setString(4, row.displayName());
        ps.setString(5, row.partyRk());
        ps.setString(6, row.personId());
        ps.setString(7, row.phoneNo());
        ps.setString(8, row.fullName());
        ps.setBoolean(9, row.blacklist());
        ps.setBoolean(10, row.vip());
        ps.setString(11, row.employer());
        ps.setString(12, row.city());
        ps.setString(13, row.sourceSystem());
        ps.setDouble(14, row.pagerankScore());
        ps.setDouble(15, row.hubScore());
        ps.setString(16, "{}");
    }

    private void bindEdge(PreparedStatement ps, EdgeRow row, boolean reverseTraversal) throws SQLException {
        ps.setString(1, row.edgeId() + (reverseTraversal ? ":rev" : ":fwd"));
        ps.setString(2, Neo4jProjectionSupport.PROJECTION_OWNER);
        ps.setString(3, row.edgeId());
        ps.setString(4, row.fromNodeId());
        ps.setString(5, row.toNodeId());
        ps.setString(6, reverseTraversal ? row.toNodeId() : row.fromNodeId());
        ps.setString(7, reverseTraversal ? row.fromNodeId() : row.toNodeId());
        ps.setString(8, row.edgeType());
        ps.setBoolean(9, row.directed());
        ps.setLong(10, row.txCount());
        ps.setDouble(11, row.txSum());
        ps.setString(12, row.relationFamily());
        ps.setDouble(13, row.strengthScore());
        ps.setLong(14, row.evidenceCount());
        ps.setString(15, row.sourceSystem());
        ps.setTimestamp(16, row.firstSeenAt() == null ? null : Timestamp.from(row.firstSeenAt()));
        ps.setTimestamp(17, row.lastSeenAt() == null ? null : Timestamp.from(row.lastSeenAt()));
        ps.setString(18, "{}");
    }
}
