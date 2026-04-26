package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.JanusGraphProperties;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "JANUSGRAPH")
public class JanusGraphRuntimeManager {

    private static final Logger log = LoggerFactory.getLogger(JanusGraphRuntimeManager.class);
    private static final int SYNC_BATCH_SIZE = 250;
    private static final long CONNECTIVITY_TIMEOUT_MILLIS = 180_000;
    private static final long CONNECTIVITY_RETRY_MILLIS = 5_000;
    private static final String NODE_ID_INDEX = "graphNodeByNodeId";
    private static final String NODE_OWNER_INDEX = "graphNodeByProjectionOwner";
    private static final String EDGE_REL_ID_INDEX = "connectedByGraphRelId";
    private static final String EDGE_OWNER_INDEX = "connectedByProjectionOwner";

    private final Client client;
    private final JanusGraphProperties properties;
    private final GraphRepository graphRepository;
    private final Neo4jProjectionSupport projectionSupport;
    private final Map<String, Object> vertexIdsByNodeId = new HashMap<>();

    public JanusGraphRuntimeManager(Client client,
                                    JanusGraphProperties properties,
                                    GraphRepository graphRepository,
                                    Neo4jProjectionSupport projectionSupport) {
        this.client = client;
        this.properties = properties;
        this.graphRepository = graphRepository;
        this.projectionSupport = projectionSupport;
    }

    public void initialize() {
        waitForConnectivity();
        if (!properties.isSyncGraphStateOnStartup()) {
            log.info("janusgraph graph-state synchronization is skipped at startup");
            return;
        }
        if (properties.isClearProjectionOnStartup()) {
            submit("""
                g.V().hasLabel('GraphNode').has('projection_owner', projectionOwner).drop().iterate()
                """, Map.of("projectionOwner", Neo4jProjectionSupport.PROJECTION_OWNER));
        }
        ensureSchema();
        syncNodes(properties.isClearProjectionOnStartup());
        syncEdges(properties.isClearProjectionOnStartup());
    }

    public boolean isAvailable() {
        try {
            verifyConnectivity();
            return true;
        } catch (Exception ex) {
            log.debug("JanusGraph connectivity check failed: {}", ex.getMessage());
            return false;
        }
    }

    private void waitForConnectivity() {
        long deadline = System.currentTimeMillis() + CONNECTIVITY_TIMEOUT_MILLIS;
        RuntimeException lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                verifyConnectivity();
                return;
            } catch (RuntimeException ex) {
                lastFailure = ex;
                log.info("janusgraph is not ready yet: {}", ex.getMessage());
                sleepBeforeRetry();
            }
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
    }

    private void sleepBeforeRetry() {
        try {
            Thread.sleep(CONNECTIVITY_RETRY_MILLIS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for JanusGraph", ex);
        }
    }

    private void verifyConnectivity() {
        submit("g.V().limit(1).count().next()", Map.of());
    }

    private void ensureSchema() {
        submit("""
            def mgmt = graph.openManagement()
            try {
              def graphNode = mgmt.getVertexLabel('GraphNode')
              if (graphNode == null) {
                graphNode = mgmt.makeVertexLabel('GraphNode').make()
              }
              def connected = mgmt.getEdgeLabel('CONNECTED')
              if (connected == null) {
                connected = mgmt.makeEdgeLabel('CONNECTED').make()
              }
              def nodeId = mgmt.getPropertyKey('node_id')
              if (nodeId == null) {
                nodeId = mgmt.makePropertyKey('node_id')
                  .dataType(String.class)
                  .cardinality(org.janusgraph.core.Cardinality.SINGLE)
                  .make()
              }
              def projectionOwner = mgmt.getPropertyKey('projection_owner')
              if (projectionOwner == null) {
                projectionOwner = mgmt.makePropertyKey('projection_owner')
                  .dataType(String.class)
                  .cardinality(org.janusgraph.core.Cardinality.SINGLE)
                  .make()
              }
              def graphRelId = mgmt.getPropertyKey('graph_rel_id')
              if (graphRelId == null) {
                graphRelId = mgmt.makePropertyKey('graph_rel_id')
                  .dataType(String.class)
                  .cardinality(org.janusgraph.core.Cardinality.SINGLE)
                  .make()
              }
              if (mgmt.getGraphIndex(nodeIdIndex) == null) {
                mgmt.buildIndex(nodeIdIndex, org.apache.tinkerpop.gremlin.structure.Vertex.class)
                  .addKey(nodeId)
                  .buildCompositeIndex()
              }
              if (mgmt.getGraphIndex(nodeOwnerIndex) == null) {
                mgmt.buildIndex(nodeOwnerIndex, org.apache.tinkerpop.gremlin.structure.Vertex.class)
                  .addKey(projectionOwner)
                  .buildCompositeIndex()
              }
              if (mgmt.getGraphIndex(edgeRelIdIndex) == null) {
                mgmt.buildIndex(edgeRelIdIndex, org.apache.tinkerpop.gremlin.structure.Edge.class)
                  .addKey(graphRelId)
                  .buildCompositeIndex()
              }
              if (mgmt.getGraphIndex(edgeOwnerIndex) == null) {
                mgmt.buildIndex(edgeOwnerIndex, org.apache.tinkerpop.gremlin.structure.Edge.class)
                  .addKey(projectionOwner)
                  .buildCompositeIndex()
              }
              mgmt.commit()
            } catch (Throwable t) {
              mgmt.rollback()
              throw t
            }
            """,
            Map.of(
                "nodeIdIndex", NODE_ID_INDEX,
                "nodeOwnerIndex", NODE_OWNER_INDEX,
                "edgeRelIdIndex", EDGE_REL_ID_INDEX,
                "edgeOwnerIndex", EDGE_OWNER_INDEX
            ));
    }

    private void syncNodes(boolean createOnly) {
        if (createOnly) {
            vertexIdsByNodeId.clear();
        }
        AtomicLong synced = new AtomicLong();
        graphRepository.forEachNodeBatch(SYNC_BATCH_SIZE, nodes -> {
            List<Map<String, Object>> batch = nodes.stream().map(projectionSupport::toNodeProjection).toList();
            if (createOnly) {
                List<Result> results = submitResults("""
                    def created = []
                    for (row in batch) {
                      def node = g.addV('GraphNode').property('node_id', row.node_id).next()
                      row.each { k, v -> if (v != null) { node.property(k, v) } }
                      created.add([node_id: row.node_id, vertex_id: node.id()])
                    }
                    created
                    """, Map.of("batch", batch));
                cacheVertexIds(results);
            } else {
                submit("""
                    for (row in batch) {
                      def existing = g.V().has('GraphNode', 'node_id', row.node_id).tryNext()
                      def node = existing.orElseGet { g.addV('GraphNode').property('node_id', row.node_id).next() }
                      row.each { k, v -> if (v != null) { node.property(k, v) } }
                    }
                    """,
                    Map.of("batch", batch));
            }
            long total = synced.addAndGet(batch.size());
            if (total % 50_000 == 0) {
                log.info("janusgraph projection nodes synced: {}", total);
            }
        });
        log.info("janusgraph projection nodes synced: {}", synced.get());
    }

    private void syncEdges(boolean createOnly) {
        AtomicLong sourceEdges = new AtomicLong();
        AtomicLong projected = new AtomicLong();
        List<Map<String, Object>> batch = new ArrayList<>(SYNC_BATCH_SIZE);
        graphRepository.forEachEdgeBatch(SYNC_BATCH_SIZE, edges -> {
            for (EdgeRow edge : edges) {
                projected.addAndGet(addEdgeProjection(batch, edge, false, createOnly));
                if (!edge.directed()) {
                    projected.addAndGet(addEdgeProjection(batch, edge, true, createOnly));
                }
                if (batch.size() >= SYNC_BATCH_SIZE) {
                    flushEdges(batch, createOnly);
                }
            }
            long total = sourceEdges.addAndGet(edges.size());
            if (total % 50_000 == 0) {
                log.info("janusgraph projection source edges scanned: {}, relationships synced: {}", total, projected.get());
            }
        });
        flushEdges(batch, createOnly);
        log.info("janusgraph projection source edges scanned: {}, relationships synced: {}", sourceEdges.get(), projected.get());
    }

    private int addEdgeProjection(List<Map<String, Object>> batch,
                                  EdgeRow edge,
                                  boolean reverseTraversal,
                                  boolean createOnly) {
        Map<String, Object> projection = new HashMap<>(projectionSupport.toRelationshipProjection(edge, reverseTraversal));
        if (createOnly && !attachVertexIds(projection)) {
            return 0;
        }
        batch.add(projection);
        return 1;
    }

    private boolean attachVertexIds(Map<String, Object> projection) {
        Object fromNodeId = projection.get("traversal_from_node_id");
        Object toNodeId = projection.get("traversal_to_node_id");
        Object fromVertexId = vertexIdsByNodeId.get(String.valueOf(fromNodeId));
        Object toVertexId = vertexIdsByNodeId.get(String.valueOf(toNodeId));
        if (fromVertexId == null || toVertexId == null) {
            return false;
        }
        projection.put("_janus_from_vertex_id", fromVertexId);
        projection.put("_janus_to_vertex_id", toVertexId);
        return true;
    }

    private void flushEdges(List<Map<String, Object>> batch, boolean createOnly) {
        if (batch.isEmpty()) {
            return;
        }
        submit(createOnly ? """
                for (row in batch) {
                  def from = g.V(row._janus_from_vertex_id).tryNext()
                  def to = g.V(row._janus_to_vertex_id).tryNext()
                  if (from.isPresent() && to.isPresent()) {
                    def edge = from.get().addEdge('CONNECTED', to.get(), 'graph_rel_id', row.graph_rel_id)
                    row.each { k, v -> if (v != null && !k.startsWith('_janus_')) { edge.property(k, v) } }
                  }
                }
                """ : """
                for (row in batch) {
                  def from = g.V().has('GraphNode', 'node_id', row.traversal_from_node_id).tryNext()
                  def to = g.V().has('GraphNode', 'node_id', row.traversal_to_node_id).tryNext()
                  if (from.isPresent() && to.isPresent()) {
                    def existing = g.V(from.get()).outE('CONNECTED').has('graph_rel_id', row.graph_rel_id).tryNext()
                    def edge = existing.orElseGet { from.get().addEdge('CONNECTED', to.get(), 'graph_rel_id', row.graph_rel_id) }
                    row.each { k, v -> if (v != null) { edge.property(k, v) } }
                  }
                }
                """,
            Map.of("batch", List.copyOf(batch)));
        batch.clear();
    }

    private void cacheVertexIds(List<Result> results) {
        for (Result result : results) {
            cacheVertexIdPayload(result.getObject());
        }
    }

    private void cacheVertexIdPayload(Object raw) {
        if (raw instanceof List<?> rows) {
            rows.forEach(this::cacheVertexIdPayload);
            return;
        }
        if (raw instanceof Map<?, ?> row) {
            Object nodeId = row.get("node_id");
            Object vertexId = row.get("vertex_id");
            if (nodeId != null && vertexId != null) {
                vertexIdsByNodeId.put(String.valueOf(nodeId), vertexId);
            }
        }
    }

    private void submit(String script, Map<String, Object> bindings) {
        submitResults(script, bindings);
    }

    private List<Result> submitResults(String script, Map<String, Object> bindings) {
        try {
            return client.submit(script, bindings).all().get();
        } catch (Exception ex) {
            throw new IllegalStateException("JanusGraph Gremlin script failed", ex);
        }
    }
}
