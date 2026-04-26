package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "JANUSGRAPH")
public class JanusGraphQueryBackend implements GraphQueryBackend {

    private final Client client;

    public JanusGraphQueryBackend(Client client) {
        this.client = client;
    }

    @Override
    public GraphSource source() {
        return GraphSource.JANUSGRAPH;
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
        List<String> seeds = List.copyOf(seedNodeIds);

        String script = traversalPrefix(direction, seeds.size() == 1) + """
            .has('projection_owner', projectionOwner)
            .filter { allRelations || it.get().property('relation_family').orElse(null) == relationFamily }
            .filter { edgeTypes.isEmpty() || edgeTypes.contains(it.get().property('edge_type').orElse(null)) }
            .order()
            .by('strength_score', org.apache.tinkerpop.gremlin.process.traversal.Order.desc)
            .by('evidence_count', org.apache.tinkerpop.gremlin.process.traversal.Order.desc)
            .by('edge_id', org.apache.tinkerpop.gremlin.process.traversal.Order.asc)
            .limit(candidateLimit)
            .valueMap()
            .toList()
            """;

        try {
            List<Result> results = client.submit(script, Map.of(
                "seedNodeId", seeds.getFirst(),
                "seedNodeIds", seeds,
                "projectionOwner", Neo4jProjectionSupport.PROJECTION_OWNER,
                "allRelations", GraphRelationFamilies.isAllRelations(relationFamily),
                "relationFamily", GraphRelationFamilies.normalize(relationFamily),
                "edgeTypes", edgeTypes == null ? List.of() : List.copyOf(edgeTypes),
                "candidateLimit", Math.max(1, candidateLimit)
            )).all().get();
            if (results.isEmpty()) {
                return List.of();
            }
            Object raw = results.get(0).getObject();
            if (!(raw instanceof List<?> rows)) {
                return List.of();
            }
            return rows.stream()
                .filter(Map.class::isInstance)
                .map(row -> ProjectionValueSupport.mapEdge(flattenValueMap((Map<?, ?>) row)))
                .toList();
        } catch (Exception ex) {
            throw new IllegalStateException("JanusGraph expand query failed", ex);
        }
    }

    @Override
    public Optional<PathRow> findShortestPath(String sourceNodeId,
                                              String targetNodeId,
                                              String relationFamily,
                                              Direction direction,
                                              int maxDepth) {
        if (sourceNodeId.equals(targetNodeId)) {
            return Optional.of(new PathRow(List.of(sourceNodeId), List.of(), 0));
        }
        String script = """
            def value = { raw -> raw instanceof List ? (raw.isEmpty() ? null : raw[0]) : raw }
            def queue = [[node_id: sourceNodeId, node_ids: [sourceNodeId], edge_ids: []]]
            def seen = new java.util.LinkedHashSet()
            seen.add(sourceNodeId)
            def found = null
            for (int depth = 0; depth < maxDepth && found == null && !queue.isEmpty(); depth++) {
              def nextQueue = []
              for (state in queue) {
                def rows = g.V().has('node_id', state.node_id)
                  .%s
                  .has('projection_owner', projectionOwner)
                  .filter { allRelations || it.get().property('relation_family').orElse(null) == relationFamily }
                  .order()
                  .by('strength_score', org.apache.tinkerpop.gremlin.process.traversal.Order.desc)
                  .by('evidence_count', org.apache.tinkerpop.gremlin.process.traversal.Order.desc)
                  .by('edge_id', org.apache.tinkerpop.gremlin.process.traversal.Order.asc)
                  .limit(candidateLimit)
                  .valueMap()
                  .toList()
                for (row in rows) {
                  %s
                  if (nextNodeId == null || state.node_ids.contains(nextNodeId)) {
                    continue
                  }
                  def nodeIds = []
                  nodeIds.addAll(state.node_ids)
                  nodeIds.add(nextNodeId)
                  def edgeIds = []
                  edgeIds.addAll(state.edge_ids)
                  edgeIds.add(value(row.edge_id))
                  if (nextNodeId == targetNodeId) {
                    found = [node_ids: nodeIds, edge_ids: edgeIds, hop_count: edgeIds.size()]
                    break
                  }
                  if (seen.add(nextNodeId)) {
                    nextQueue.add([node_id: nextNodeId, node_ids: nodeIds, edge_ids: edgeIds])
                  }
                }
                if (found != null) {
                  break
                }
              }
              queue = nextQueue
            }
            found == null ? [node_ids: [], edge_ids: [], hop_count: 0] : found
            """.formatted(shortestPathEdgeStep(direction), shortestPathNextNodeStatement(direction));

        try {
            List<Result> results = client.submit(script, Map.of(
                "sourceNodeId", sourceNodeId,
                "targetNodeId", targetNodeId,
                "projectionOwner", Neo4jProjectionSupport.PROJECTION_OWNER,
                "allRelations", GraphRelationFamilies.isAllRelations(relationFamily),
                "relationFamily", GraphRelationFamilies.normalize(relationFamily),
                "maxDepth", Math.max(1, maxDepth),
                "candidateLimit", 10_000
            )).all().get();
            if (results.isEmpty() || !(results.get(0).getObject() instanceof Map<?, ?> row)) {
                return Optional.empty();
            }
            List<String> nodeIds = stringList(row.get("node_ids"));
            List<String> edgeIds = stringList(row.get("edge_ids"));
            if (nodeIds.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new PathRow(nodeIds, edgeIds, edgeIds.size()));
        } catch (Exception ex) {
            throw new IllegalStateException("JanusGraph shortest-path query failed", ex);
        }
    }

    private String traversalPrefix(Direction direction, boolean singleSeed) {
        String nodeLookup = singleSeed
            ? "g.V().has('node_id', seedNodeId)"
            : "g.V().has('node_id', within(seedNodeIds))";
        return switch (direction) {
            case OUTBOUND -> nodeLookup + ".outE('CONNECTED')";
            case INBOUND -> nodeLookup + ".inE('CONNECTED')";
            case BOTH -> nodeLookup + ".bothE('CONNECTED')";
        };
    }

    private String shortestPathEdgeStep(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "outE('CONNECTED')";
            case INBOUND -> "inE('CONNECTED')";
            case BOTH -> "bothE('CONNECTED')";
        };
    }

    private String shortestPathNextNodeStatement(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "def nextNodeId = value(row.traversal_to_node_id)";
            case INBOUND -> "def nextNodeId = value(row.traversal_from_node_id)";
            case BOTH -> """
                def fromNodeId = value(row.traversal_from_node_id)
                def toNodeId = value(row.traversal_to_node_id)
                def nextNodeId = fromNodeId == state.node_id ? toNodeId : (toNodeId == state.node_id ? fromNodeId : null)
                """;
        };
    }

    private Map<String, Object> flattenValueMap(Map<?, ?> raw) {
        Map<String, Object> flattened = new LinkedHashMap<>();
        raw.forEach((key, value) -> {
            if (value instanceof List<?> values && !values.isEmpty()) {
                flattened.put(String.valueOf(key), values.get(0));
            } else {
                flattened.put(String.valueOf(key), value);
            }
        });
        return flattened;
    }

    private List<String> stringList(Object raw) {
        if (!(raw instanceof List<?> values)) {
            return List.of();
        }
        return values.stream()
            .map(String::valueOf)
            .toList();
    }
}
