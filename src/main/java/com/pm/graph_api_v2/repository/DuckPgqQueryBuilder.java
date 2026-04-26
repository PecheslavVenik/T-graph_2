package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

final class DuckPgqQueryBuilder {

    private DuckPgqQueryBuilder() {
    }

    static String buildShortestPathQuery(String graphName,
                                         String vertexLabel,
                                         String edgeLabel,
                                         String sourceNodeId,
                                         String targetNodeId,
                                         Direction direction) {
        return """
            FROM GRAPH_TABLE (
              %s
              MATCH p = ANY SHORTEST (a:%s WHERE a.node_id = %s)%s+(b:%s WHERE b.node_id = %s)
              COLUMNS (
                vertices(p) AS vertices_rowid,
                edges(p) AS edges_rowid,
                path_length(p) AS hop_count
              )
            )
            SELECT *
            """.formatted(
            graphName,
            vertexLabel,
            sqlStringLiteral(sourceNodeId),
            shortestPattern(direction, edgeLabel),
            vertexLabel,
            sqlStringLiteral(targetNodeId)
        );
    }

    static String buildExpandQuery(String graphName,
                                   String vertexLabel,
                                   String edgeLabel,
                                   Collection<String> seedNodeIds,
                                   List<String> edgeTypes,
                                   Direction direction,
                                   int candidateLimit) {
        String seedFilter = inList(seedNodeIds);
        String edgeTypeFilter = edgeTypes == null || edgeTypes.isEmpty()
            ? ""
            : " AND e.edge_type IN (%s)".formatted(inList(edgeTypes));
        String matchPattern = switch (direction) {
            case OUTBOUND, INBOUND -> "MATCH (a:%s)-[e:%s]->(b:%s)".formatted(vertexLabel, edgeLabel, vertexLabel);
            case BOTH -> "MATCH (a:%s)-[e:%s]-(b:%s)".formatted(vertexLabel, edgeLabel, vertexLabel);
        };
        String whereClause = switch (direction) {
            case OUTBOUND -> "WHERE a.node_id IN (%s)%s".formatted(seedFilter, edgeTypeFilter);
            case INBOUND -> "WHERE b.node_id IN (%s)%s".formatted(seedFilter, edgeTypeFilter);
            case BOTH -> "WHERE (a.node_id IN (%s) OR b.node_id IN (%s))%s".formatted(seedFilter, seedFilter, edgeTypeFilter);
        };

        return """
            FROM GRAPH_TABLE (
              %s
              %s
              %s
              COLUMNS (
                e.edge_id AS edge_id,
                e.from_node_id AS from_node_id,
                e.to_node_id AS to_node_id,
                e.edge_type AS edge_type,
                e.directed AS directed,
                e.tx_count AS tx_count,
                e.tx_sum AS tx_sum,
                e.relation_family AS relation_family,
                e.strength_score AS strength_score,
                e.evidence_count AS evidence_count,
                e.source_system AS source_system,
                e.first_seen_at AS first_seen_at,
                e.last_seen_at AS last_seen_at,
                e.attrs_json AS attrs_json
              )
            )
            SELECT DISTINCT *
            ORDER BY strength_score DESC, evidence_count DESC, edge_id
            LIMIT %d
            """.formatted(
            graphName,
            matchPattern,
            whereClause,
            Math.max(1, candidateLimit)
        );
    }

    static String sqlStringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String inList(Collection<String> values) {
        return values.stream()
            .map(DuckPgqQueryBuilder::sqlStringLiteral)
            .collect(Collectors.joining(","));
    }

    private static String shortestPattern(Direction direction, String edgeLabel) {
        return switch (direction) {
            case OUTBOUND -> "-[e:%s]->".formatted(edgeLabel);
            case INBOUND -> "<-[e:%s]-".formatted(edgeLabel);
            case BOTH -> "-[e:%s]-".formatted(edgeLabel);
        };
    }
}
