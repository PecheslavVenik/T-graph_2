package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "ARANGODB")
public class ArangoGraphQueryBackend implements GraphQueryBackend {

    private final ArangoHttpClient arango;

    public ArangoGraphQueryBackend(ArangoHttpClient arango) {
        this.arango = arango;
    }

    @Override
    public GraphSource source() {
        return GraphSource.ARANGODB;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                         String relationFamily,
                                         List<String> edgeTypes,
                                         Direction direction,
                                         int candidateLimit) {
        if (seedNodeIds.isEmpty()) {
            return List.of();
        }

        String query = """
            FOR seedNodeId IN @seedNodeIds
              FOR v, e IN 1..1 %s CONCAT('graph_nodes/', seedNodeId) graph_edges
                FILTER e.projection_owner == @projectionOwner
                FILTER @allRelations OR e.relation_family == @relationFamily
                FILTER LENGTH(@edgeTypes) == 0 OR e.edge_type IN @edgeTypes
                SORT TO_NUMBER(e.strength_score) DESC, TO_NUMBER(e.evidence_count) DESC, e.edge_id ASC
                LIMIT @candidateLimit
                RETURN {
                  edge_id: e.edge_id,
                  from_node_id: e.from_node_id,
                  to_node_id: e.to_node_id,
                  edge_type: e.edge_type,
                  directed: e.directed,
                  tx_count: e.tx_count,
                  tx_sum: e.tx_sum,
                  relation_family: e.relation_family,
                  strength_score: e.strength_score,
                  evidence_count: e.evidence_count,
                  source_system: e.source_system,
                  first_seen_at: e.first_seen_at,
                  last_seen_at: e.last_seen_at,
                  attrs_json: e.attrs_json
                }
            """.formatted(arangoDirection(direction));

        Map<String, Object> response = arango.cursor(query, Map.of(
            "seedNodeIds", List.copyOf(seedNodeIds),
            "projectionOwner", Neo4jProjectionSupport.PROJECTION_OWNER,
            "allRelations", GraphRelationFamilies.isAllRelations(relationFamily),
            "relationFamily", GraphRelationFamilies.normalize(relationFamily),
            "edgeTypes", edgeTypes == null ? List.of() : List.copyOf(edgeTypes),
            "candidateLimit", Math.max(1, candidateLimit)
        ));
        Object result = response.get("result");
        if (!(result instanceof List<?> rows)) {
            return List.of();
        }
        return rows.stream()
            .filter(Map.class::isInstance)
            .map(row -> ProjectionValueSupport.mapEdge((Map<String, Object>) row))
            .toList();
    }

    @Override
    public Optional<PathRow> findShortestPath(String sourceNodeId,
                                              String targetNodeId,
                                              String relationFamily,
                                              Direction direction,
                                              int maxDepth) {
        return GraphBackendPathSearch.breadthFirst(
            sourceNodeId,
            targetNodeId,
            relationFamily,
            direction,
            maxDepth,
            this::findExpandEdges
        );
    }

    private String arangoDirection(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "OUTBOUND";
            case INBOUND -> "INBOUND";
            case BOTH -> "ANY";
        };
    }
}
