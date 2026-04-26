package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.Neo4jProperties;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "NEO4J")
public class Neo4jRuntimeManager {

    private static final Logger log = LoggerFactory.getLogger(Neo4jRuntimeManager.class);
    private static final int SYNC_BATCH_SIZE = 500;
    private static final int CLEAR_BATCH_SIZE = 5_000;

    private final Driver driver;
    private final GraphRepository graphRepository;
    private final Neo4jProjectionSupport projectionSupport;
    private final Neo4jProperties neo4jProperties;

    public Neo4jRuntimeManager(Driver driver,
                               GraphRepository graphRepository,
                               Neo4jProjectionSupport projectionSupport,
                               Neo4jProperties neo4jProperties) {
        this.driver = driver;
        this.graphRepository = graphRepository;
        this.projectionSupport = projectionSupport;
        this.neo4jProperties = neo4jProperties;
    }

    public void initialize() {
        driver.verifyConnectivity();
        if (!neo4jProperties.isSyncGraphStateOnStartup()) {
            log.info("neo4j graph-state synchronization is skipped at startup");
            return;
        }

        try (Session session = newSession()) {
            session.run(
                "CREATE CONSTRAINT graph_node_id IF NOT EXISTS FOR (n:" + Neo4jQuerySupport.NODE_LABEL + ") REQUIRE n.node_id IS UNIQUE"
            ).consume();
            if (neo4jProperties.isClearProjectionOnStartup()) {
                clearProjection(session);
            }

            syncNodes(session);
            syncRelationships(session);
        }
    }

    private void clearProjection(Session session) {
        AtomicLong clearedNodes = new AtomicLong();
        while (true) {
            long deleted = session.run(
                """
                MATCH (n:%s {projection_owner: $projectionOwner})
                WITH n LIMIT %d
                DETACH DELETE n
                RETURN count(*) AS deleted
                """.formatted(Neo4jQuerySupport.NODE_LABEL, CLEAR_BATCH_SIZE),
                Values.parameters("projectionOwner", Neo4jProjectionSupport.PROJECTION_OWNER)
            ).single().get("deleted").asLong();
            if (deleted == 0) {
                break;
            }
            long total = clearedNodes.addAndGet(deleted);
            if (total % 50_000 == 0) {
                log.info("neo4j projection nodes cleared: {}", total);
            }
        }
        log.info("neo4j projection nodes cleared: {}", clearedNodes.get());
    }

    private void syncNodes(Session session) {
        AtomicLong syncedNodes = new AtomicLong();
        graphRepository.forEachNodeBatch(SYNC_BATCH_SIZE, nodes -> {
            List<Map<String, Object>> batch = nodes.stream()
                .map(projectionSupport::toNodeProjection)
                .toList();
            if (batch.isEmpty()) {
                return;
            }

            session.run(
                """
                UNWIND $batch AS row
                MERGE (n:%s {node_id: row.node_id})
                SET n += row
                """.formatted(Neo4jQuerySupport.NODE_LABEL),
                Values.parameters("batch", batch)
            ).consume();

            long total = syncedNodes.addAndGet(batch.size());
            if (total % 50_000 == 0) {
                log.info("neo4j projection nodes synced: {}", total);
            }
        });
        log.info("neo4j projection nodes synced: {}", syncedNodes.get());
    }

    private void syncRelationships(Session session) {
        AtomicLong sourceEdges = new AtomicLong();
        AtomicLong projectedRelationships = new AtomicLong();
        List<Map<String, Object>> batch = new ArrayList<>(SYNC_BATCH_SIZE);

        graphRepository.forEachEdgeBatch(SYNC_BATCH_SIZE, edges -> {
            for (EdgeRow edge : edges) {
                appendRelationshipProjection(session, batch, projectedRelationships, edge, false);
                if (!edge.directed()) {
                    appendRelationshipProjection(session, batch, projectedRelationships, edge, true);
                }
            }

            long total = sourceEdges.addAndGet(edges.size());
            if (total % 50_000 == 0) {
                log.info("neo4j projection source edges scanned: {}, relationships synced: {}", total, projectedRelationships.get());
            }
        });

        flushRelationshipBatch(session, batch);
        log.info("neo4j projection source edges scanned: {}, relationships synced: {}", sourceEdges.get(), projectedRelationships.get());
    }

    private void appendRelationshipProjection(Session session,
                                              List<Map<String, Object>> batch,
                                              AtomicLong projectedRelationships,
                                              EdgeRow edge,
                                              boolean reverseTraversal) {
        batch.add(projectionSupport.toRelationshipProjection(edge, reverseTraversal));
        projectedRelationships.incrementAndGet();
        if (batch.size() >= SYNC_BATCH_SIZE) {
            flushRelationshipBatch(session, batch);
        }
    }

    private void flushRelationshipBatch(Session session, List<Map<String, Object>> batch) {
        if (batch.isEmpty()) {
            return;
        }

        session.run(
            """
            UNWIND $batch AS row
            MATCH (from:%s {node_id: row.traversal_from_node_id})
            MATCH (to:%s {node_id: row.traversal_to_node_id})
            MERGE (from)-[r:%s {graph_rel_id: row.graph_rel_id}]->(to)
            SET r = row
            """.formatted(Neo4jQuerySupport.NODE_LABEL, Neo4jQuerySupport.NODE_LABEL, Neo4jQuerySupport.REL_TYPE),
            Values.parameters("batch", List.copyOf(batch))
        ).consume();
        batch.clear();
    }

    public boolean isAvailable() {
        try {
            driver.verifyConnectivity();
            return true;
        } catch (Exception ex) {
            log.debug("Neo4j connectivity check failed: {}", ex.getMessage());
            return false;
        }
    }

    private Session newSession() {
        String database = neo4jProperties.getDatabase();
        if (database == null || database.isBlank()) {
            return driver.session();
        }
        return driver.session(SessionConfig.forDatabase(database));
    }
}
