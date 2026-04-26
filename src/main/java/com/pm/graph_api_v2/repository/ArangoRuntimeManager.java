package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.ArangoProperties;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "ARANGODB")
public class ArangoRuntimeManager {

    private static final Logger log = LoggerFactory.getLogger(ArangoRuntimeManager.class);
    private static final int SYNC_BATCH_SIZE = 500;

    private final ArangoHttpClient arango;
    private final ArangoProperties properties;
    private final GraphRepository graphRepository;
    private final ObjectMapper objectMapper;

    public ArangoRuntimeManager(ArangoHttpClient arango,
                                ArangoProperties properties,
                                GraphRepository graphRepository,
                                ObjectMapper objectMapper) {
        this.arango = arango;
        this.properties = properties;
        this.graphRepository = graphRepository;
        this.objectMapper = objectMapper;
    }

    public void initialize() {
        ensureSchema();
        if (!properties.isSyncGraphStateOnStartup()) {
            log.info("arangodb graph-state synchronization is skipped at startup");
            return;
        }
        if (properties.isClearProjectionOnStartup()) {
            clearProjection();
        }
        syncNodes();
        syncEdges();
    }

    public boolean isAvailable() {
        try {
            arango.get("/_api/version");
            return true;
        } catch (Exception ex) {
            log.debug("ArangoDB connectivity check failed: {}", ex.getMessage());
            return false;
        }
    }

    private void ensureSchema() {
        arango.postAllowConflict("/_api/database", Map.of("name", properties.getDatabase()));
        arango.postAllowConflict("/_db/" + properties.getDatabase() + "/_api/collection", Map.of("name", "graph_nodes"));
        arango.postAllowConflict("/_db/" + properties.getDatabase() + "/_api/collection", Map.of(
            "name", "graph_edges",
            "type", 3
        ));
        arango.postAllowConflict("/_db/" + properties.getDatabase() + "/_api/index?collection=graph_nodes", Map.of(
            "type", "persistent",
            "fields", List.of("projection_owner")
        ));
        arango.postAllowConflict("/_db/" + properties.getDatabase() + "/_api/index?collection=graph_edges", Map.of(
            "type", "persistent",
            "fields", List.of("projection_owner", "relation_family")
        ));
    }

    private void clearProjection() {
        arango.cursor("""
            FOR e IN graph_edges
              FILTER e.projection_owner == @projectionOwner
              REMOVE e IN graph_edges
            """, Map.of("projectionOwner", Neo4jProjectionSupport.PROJECTION_OWNER));
        arango.cursor("""
            FOR n IN graph_nodes
              FILTER n.projection_owner == @projectionOwner
              REMOVE n IN graph_nodes
            """, Map.of("projectionOwner", Neo4jProjectionSupport.PROJECTION_OWNER));
    }

    private void syncNodes() {
        AtomicLong synced = new AtomicLong();
        graphRepository.forEachNodeBatch(SYNC_BATCH_SIZE, nodes -> {
            List<Map<String, Object>> batch = nodes.stream().map(this::nodeDocument).toList();
            arango.post("/_db/" + properties.getDatabase() + "/_api/document/graph_nodes?overwriteMode=replace", batch);
            long total = synced.addAndGet(batch.size());
            if (total % 50_000 == 0) {
                log.info("arangodb projection nodes synced: {}", total);
            }
        });
        log.info("arangodb projection nodes synced: {}", synced.get());
    }

    private void syncEdges() {
        AtomicLong sourceEdges = new AtomicLong();
        AtomicLong projected = new AtomicLong();
        List<Map<String, Object>> batch = new ArrayList<>(SYNC_BATCH_SIZE);
        graphRepository.forEachEdgeBatch(SYNC_BATCH_SIZE, edges -> {
            for (EdgeRow edge : edges) {
                batch.add(edgeDocument(edge, false));
                projected.incrementAndGet();
                if (!edge.directed()) {
                    batch.add(edgeDocument(edge, true));
                    projected.incrementAndGet();
                }
                if (batch.size() >= SYNC_BATCH_SIZE) {
                    flushEdgeBatch(batch);
                }
            }
            long total = sourceEdges.addAndGet(edges.size());
            if (total % 50_000 == 0) {
                log.info("arangodb projection source edges scanned: {}, relationships synced: {}", total, projected.get());
            }
        });
        flushEdgeBatch(batch);
        log.info("arangodb projection source edges scanned: {}, relationships synced: {}", sourceEdges.get(), projected.get());
    }

    private void flushEdgeBatch(List<Map<String, Object>> batch) {
        if (batch.isEmpty()) {
            return;
        }
        arango.post("/_db/" + properties.getDatabase() + "/_api/document/graph_edges?overwriteMode=replace", List.copyOf(batch));
        batch.clear();
    }

    private Map<String, Object> nodeDocument(NodeRow row) {
        return new java.util.LinkedHashMap<>(Map.ofEntries(
            Map.entry("_key", arangoKey(row.nodeId())),
            Map.entry("node_id", row.nodeId()),
            Map.entry("projection_owner", Neo4jProjectionSupport.PROJECTION_OWNER),
            Map.entry("node_type", nullToEmpty(row.nodeType())),
            Map.entry("display_name", nullToEmpty(row.displayName())),
            Map.entry("source_system", nullToEmpty(row.sourceSystem())),
            Map.entry("pagerank_score", row.pagerankScore()),
            Map.entry("hub_score", row.hubScore())
        ));
    }

    private Map<String, Object> edgeDocument(EdgeRow row, boolean reverseTraversal) {
        String graphRelId = row.edgeId() + (reverseTraversal ? ":rev" : ":fwd");
        java.util.LinkedHashMap<String, Object> document = new java.util.LinkedHashMap<>();
        document.put("_key", arangoKey(graphRelId));
        document.put("_from", "graph_nodes/" + arangoKey(reverseTraversal ? row.toNodeId() : row.fromNodeId()));
        document.put("_to", "graph_nodes/" + arangoKey(reverseTraversal ? row.fromNodeId() : row.toNodeId()));
        document.put("graph_rel_id", graphRelId);
        document.put("projection_owner", Neo4jProjectionSupport.PROJECTION_OWNER);
        document.put("edge_id", row.edgeId());
        document.put("from_node_id", row.fromNodeId());
        document.put("to_node_id", row.toNodeId());
        document.put("traversal_from_node_id", reverseTraversal ? row.toNodeId() : row.fromNodeId());
        document.put("traversal_to_node_id", reverseTraversal ? row.fromNodeId() : row.toNodeId());
        document.put("edge_type", row.edgeType());
        document.put("directed", row.directed());
        document.put("tx_count", row.txCount());
        document.put("tx_sum", row.txSum());
        document.put("relation_family", row.relationFamily());
        document.put("strength_score", row.strengthScore());
        document.put("evidence_count", row.evidenceCount());
        document.put("source_system", row.sourceSystem());
        document.put("first_seen_at", row.firstSeenAt() == null ? null : row.firstSeenAt().toString());
        document.put("last_seen_at", row.lastSeenAt() == null ? null : row.lastSeenAt().toString());
        document.put("attrs_json", writeJson(row.attrs()));
        return document;
    }

    private String arangoKey(String raw) {
        return raw.replaceAll("[^A-Za-z0-9_\\-]", "_");
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private String writeJson(Map<String, Object> value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize ArangoDB attrs", ex);
        }
    }
}
