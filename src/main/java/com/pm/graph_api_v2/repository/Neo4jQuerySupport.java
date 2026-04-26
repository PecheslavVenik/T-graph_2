package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

import java.util.Collection;
import java.util.List;

public final class Neo4jQuerySupport {

    public static final String NODE_LABEL = "GraphNode";
    public static final String REL_TYPE = "CONNECTED";

    private Neo4jQuerySupport() {
    }

    public static String buildExpandQuery(Direction direction,
                                          String relationFamily,
                                          List<String> edgeTypes,
                                          int candidateLimit) {
        return """
            MATCH %s
            WHERE (%s)%s%s
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
            ORDER BY coalesce(r.strength_score, 0.0) DESC, coalesce(r.evidence_count, 0) DESC, r.edge_id ASC
            LIMIT %d
            """.formatted(
            expandPattern(direction),
            expandWhereClause(direction),
            relationFilterClause(relationFamily),
            edgeTypeFilterClause(edgeTypes),
            Math.max(1, candidateLimit)
        );
    }

    public static String buildShortestPathQuery(Direction direction, int maxDepth) {
        return """
            MATCH (source:%s {node_id: $sourceNodeId})
            MATCH (target:%s {node_id: $targetNodeId})
            MATCH p = (source)%s(target)
            WHERE all(r IN relationships(p) WHERE $allRelations OR r.relation_family = $relationFamily)
            RETURN
              [n IN nodes(p) | n.node_id] AS node_ids,
              [r IN relationships(p) | r.edge_id] AS edge_ids,
              length(p) AS hop_count
            ORDER BY hop_count ASC
            LIMIT 1
            """.formatted(
            NODE_LABEL,
            NODE_LABEL,
            shortestPathPattern(direction, maxDepth)
        );
    }

    public static Value expandParameters(Collection<String> seedNodeIds, String relationFamily, List<String> edgeTypes) {
        return Values.parameters(
            "seedNodeIds", List.copyOf(seedNodeIds),
            "relationFamily", GraphRelationFamilies.normalize(relationFamily),
            "allRelations", GraphRelationFamilies.isAllRelations(relationFamily),
            "edgeTypes", edgeTypes == null ? List.of() : List.copyOf(edgeTypes)
        );
    }

    public static Value pathParameters(String sourceNodeId, String targetNodeId, String relationFamily) {
        return Values.parameters(
            "sourceNodeId", sourceNodeId,
            "targetNodeId", targetNodeId,
            "relationFamily", GraphRelationFamilies.normalize(relationFamily),
            "allRelations", GraphRelationFamilies.isAllRelations(relationFamily)
        );
    }

    private static String expandPattern(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "(a:" + NODE_LABEL + ")-[r:" + REL_TYPE + "]->(b:" + NODE_LABEL + ")";
            case INBOUND -> "(a:" + NODE_LABEL + ")<-[r:" + REL_TYPE + "]-(b:" + NODE_LABEL + ")";
            case BOTH -> "(a:" + NODE_LABEL + ")-[r:" + REL_TYPE + "]-(b:" + NODE_LABEL + ")";
        };
    }

    private static String expandWhereClause(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "a.node_id IN $seedNodeIds";
            case INBOUND -> "a.node_id IN $seedNodeIds";
            case BOTH -> "a.node_id IN $seedNodeIds OR b.node_id IN $seedNodeIds";
        };
    }

    private static String shortestPathPattern(Direction direction, int maxDepth) {
        int boundedDepth = Math.max(1, maxDepth);
        return switch (direction) {
            case OUTBOUND -> "-[:" + REL_TYPE + "*1.." + boundedDepth + "]->";
            case INBOUND -> "<-[:" + REL_TYPE + "*1.." + boundedDepth + "]-";
            case BOTH -> "-[:" + REL_TYPE + "*1.." + boundedDepth + "]-";
        };
    }

    private static String relationFilterClause(String relationFamily) {
        if (GraphRelationFamilies.isAllRelations(relationFamily)) {
            return "";
        }
        return " AND r.relation_family = $relationFamily";
    }

    private static String edgeTypeFilterClause(List<String> edgeTypes) {
        if (edgeTypes == null || edgeTypes.isEmpty()) {
            return "";
        }
        return " AND r.edge_type IN $edgeTypes";
    }
}
