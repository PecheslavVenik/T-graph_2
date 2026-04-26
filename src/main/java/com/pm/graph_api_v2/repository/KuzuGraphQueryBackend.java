package com.pm.graph_api_v2.repository;

import com.kuzudb.FlatTuple;
import com.kuzudb.QueryResult;
import com.kuzudb.Value;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "KUZU")
public class KuzuGraphQueryBackend implements GraphQueryBackend {

    private final KuzuRuntimeManager runtimeManager;

    public KuzuGraphQueryBackend(KuzuRuntimeManager runtimeManager) {
        this.runtimeManager = runtimeManager;
    }

    @Override
    public GraphSource source() {
        return GraphSource.KUZU;
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

        String sql = """
            %s
            WHERE %s
              AND (%s)
              AND (%s)
            RETURN DISTINCT
              r.edge_id AS edge_id,
              r.from_node_id AS from_node_id,
              r.to_node_id AS to_node_id,
              r.edge_type AS edge_type,
              r.directed AS directed,
              r.tx_count AS tx_count,
              r.tx_sum AS tx_sum,
              r.relation_family AS relation_family,
              r.strength_score AS strength_score,
              r.evidence_count AS evidence_count,
              r.source_system AS source_system,
              r.first_seen_at AS first_seen_at,
              r.last_seen_at AS last_seen_at,
              r.attrs_json AS attrs_json
            ORDER BY strength_score DESC, evidence_count DESC, edge_id ASC
            LIMIT %d
            """.formatted(
            matchClause(direction),
            seedClause(direction, seedNodeIds),
            relationClause(relationFamily),
            edgeTypeClause(edgeTypes),
            Math.max(1, candidateLimit)
        );

        try (QueryResult result = runtimeManager.query(sql)) {
            java.util.ArrayList<EdgeRow> rows = new java.util.ArrayList<>();
            while (result.hasNext()) {
                FlatTuple tuple = result.getNext();
                rows.add(ProjectionValueSupport.mapEdge(tupleMap(result, tuple)));
            }
            return rows;
        }
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

    private String matchClause(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "MATCH (a:GraphNode)-[r:CONNECTED]->(b:GraphNode)";
            case INBOUND -> "MATCH (a:GraphNode)<-[r:CONNECTED]-(b:GraphNode)";
            case BOTH -> "MATCH (a:GraphNode)-[r:CONNECTED]-(b:GraphNode)";
        };
    }

    private String seedClause(Direction direction, Collection<String> seedNodeIds) {
        String seedList = listLiteral(seedNodeIds);
        return switch (direction) {
            case OUTBOUND, INBOUND -> "a.node_id IN " + seedList;
            case BOTH -> "(a.node_id IN " + seedList + " OR b.node_id IN " + seedList + ")";
        };
    }

    private String relationClause(String relationFamily) {
        if (GraphRelationFamilies.isAllRelations(relationFamily)) {
            return "true";
        }
        return "r.relation_family = " + KuzuRuntimeManager.literal(GraphRelationFamilies.normalize(relationFamily));
    }

    private String edgeTypeClause(List<String> edgeTypes) {
        if (edgeTypes == null || edgeTypes.isEmpty()) {
            return "true";
        }
        return "r.edge_type IN " + listLiteral(edgeTypes);
    }

    private String listLiteral(Collection<String> values) {
        return "[" + values.stream()
            .map(KuzuRuntimeManager::literal)
            .collect(java.util.stream.Collectors.joining(", ")) + "]";
    }

    private Map<String, Object> tupleMap(QueryResult result, FlatTuple tuple) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (long index = 0; index < result.getNumColumns(); index++) {
            Value value = tuple.getValue(index);
            row.put(result.getColumnName(index), value.isNull() ? null : value.getValue());
        }
        return row;
    }
}
