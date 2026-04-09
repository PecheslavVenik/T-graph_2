package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.Neo4jProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphRelationFamily;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "NEO4J")
public class Neo4jGraphQueryBackend implements GraphQueryBackend {

    private static final Logger log = LoggerFactory.getLogger(Neo4jGraphQueryBackend.class);
    private static final String NODE_LABEL = "GraphNode";
    private static final String REL_TYPE = "CONNECTED";
    private static final int SYNC_BATCH_SIZE = 500;

    private final Driver driver;
    private final GraphRepository graphRepository;
    private final ObjectMapper objectMapper;
    private final Neo4jProperties neo4jProperties;

    public Neo4jGraphQueryBackend(Driver driver,
                                  GraphRepository graphRepository,
                                  ObjectMapper objectMapper,
                                  Neo4jProperties neo4jProperties) {
        this.driver = driver;
        this.graphRepository = graphRepository;
        this.objectMapper = objectMapper;
        this.neo4jProperties = neo4jProperties;
    }

    @Override
    public GraphSource source() {
        return GraphSource.NEO4J;
    }

    public void initialize() {
        driver.verifyConnectivity();
        if (!neo4jProperties.isSyncGraphStateOnStartup()) {
            log.info("neo4j graph-state synchronization is skipped at startup");
            return;
        }

        List<NodeRow> nodes = graphRepository.findAllNodes();
        List<EdgeRow> edges = graphRepository.findAllEdges();

        try (Session session = newSession()) {
            session.run(
                "CREATE CONSTRAINT graph_node_id IF NOT EXISTS FOR (n:" + NODE_LABEL + ") REQUIRE n.node_id IS UNIQUE"
            ).consume();
            session.run("MATCH (n:" + NODE_LABEL + ") DETACH DELETE n").consume();

            for (List<Map<String, Object>> batch : partition(nodes.stream().map(this::toNodeProjection).toList())) {
                session.run(
                    """
                    UNWIND $batch AS row
                    MERGE (n:%s {node_id: row.node_id})
                    SET n += row
                    """.formatted(NODE_LABEL),
                    Values.parameters("batch", batch)
                ).consume();
            }

            List<Map<String, Object>> relationships = new ArrayList<>();
            for (EdgeRow edge : edges) {
                relationships.add(toRelationshipProjection(edge, false));
                if (!edge.directed()) {
                    relationships.add(toRelationshipProjection(edge, true));
                }
            }

            for (List<Map<String, Object>> batch : partition(relationships)) {
                session.run(
                    """
                    UNWIND $batch AS row
                    MATCH (from:%s {node_id: row.traversal_from_node_id})
                    MATCH (to:%s {node_id: row.traversal_to_node_id})
                    CREATE (from)-[r:%s]->(to)
                    SET r = row
                    """.formatted(NODE_LABEL, NODE_LABEL, REL_TYPE),
                    Values.parameters("batch", batch)
                ).consume();
            }
        }
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

    @Override
    public List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                         GraphRelationFamily relationFamily,
                                         Direction direction,
                                         int candidateLimit) {
        if (seedNodeIds.isEmpty()) {
            return List.of();
        }

        String query = """
            MATCH %s
            WHERE (%s)%s
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
            Math.max(1, candidateLimit)
        );

        try (Session session = newSession()) {
            return session.run(query, expandParameters(seedNodeIds, relationFamily))
                .list(this::mapEdgeRow);
        }
    }

    @Override
    public Optional<PathRow> findShortestPath(String sourceNodeId,
                                              String targetNodeId,
                                              GraphRelationFamily relationFamily,
                                              Direction direction,
                                              int maxDepth) {
        String query = """
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

        try (Session session = newSession()) {
            var result = session.run(query, pathParameters(sourceNodeId, targetNodeId, relationFamily));
            if (!result.hasNext()) {
                return Optional.empty();
            }

            var record = result.next();
            List<String> nodeIds = record.get("node_ids").asList(Value::asString);
            List<String> edgeIds = record.get("edge_ids").asList(Value::asString);
            int hopCount = record.get("hop_count").asInt();
            return Optional.of(new PathRow(nodeIds, edgeIds, hopCount));
        }
    }

    private Session newSession() {
        String database = neo4jProperties.getDatabase();
        if (database == null || database.isBlank()) {
            return driver.session();
        }
        return driver.session(SessionConfig.forDatabase(database));
    }

    private String expandPattern(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "(a:" + NODE_LABEL + ")-[r:" + REL_TYPE + "]->(b:" + NODE_LABEL + ")";
            case INBOUND -> "(a:" + NODE_LABEL + ")<-[r:" + REL_TYPE + "]-(b:" + NODE_LABEL + ")";
            case BOTH -> "(a:" + NODE_LABEL + ")-[r:" + REL_TYPE + "]-(b:" + NODE_LABEL + ")";
        };
    }

    private String expandWhereClause(Direction direction) {
        return switch (direction) {
            case OUTBOUND -> "a.node_id IN $seedNodeIds";
            case INBOUND -> "a.node_id IN $seedNodeIds";
            case BOTH -> "a.node_id IN $seedNodeIds OR b.node_id IN $seedNodeIds";
        };
    }

    private String shortestPathPattern(Direction direction, int maxDepth) {
        int boundedDepth = Math.max(1, maxDepth);
        return switch (direction) {
            case OUTBOUND -> "-[:" + REL_TYPE + "*1.." + boundedDepth + "]->";
            case INBOUND -> "<-[:" + REL_TYPE + "*1.." + boundedDepth + "]-";
            case BOTH -> "-[:" + REL_TYPE + "*1.." + boundedDepth + "]-";
        };
    }

    private String relationFilterClause(GraphRelationFamily relationFamily) {
        if (relationFamily.isAllRelations()) {
            return "";
        }
        return " AND r.relation_family = $relationFamily";
    }

    private Value expandParameters(Collection<String> seedNodeIds, GraphRelationFamily relationFamily) {
        return Values.parameters(
            "seedNodeIds", List.copyOf(seedNodeIds),
            "relationFamily", relationFamily.name(),
            "allRelations", relationFamily.isAllRelations()
        );
    }

    private Value pathParameters(String sourceNodeId, String targetNodeId, GraphRelationFamily relationFamily) {
        return Values.parameters(
            "sourceNodeId", sourceNodeId,
            "targetNodeId", targetNodeId,
            "relationFamily", relationFamily.name(),
            "allRelations", relationFamily.isAllRelations()
        );
    }

    private Map<String, Object> toNodeProjection(NodeRow row) {
        Map<String, Object> projection = new LinkedHashMap<>();
        projection.put("node_id", row.nodeId());
        projection.put("node_type", row.nodeType());
        projection.put("display_name", row.displayName());
        projection.put("party_rk", row.partyRk());
        projection.put("person_id", row.personId());
        projection.put("phone_no", row.phoneNo());
        projection.put("full_name", row.fullName());
        projection.put("is_blacklist", row.blacklist());
        projection.put("is_vip", row.vip());
        projection.put("employer", row.employer());
        projection.put("city", row.city());
        projection.put("source_system", row.sourceSystem());
        projection.put("pagerank_score", row.pagerankScore());
        projection.put("hub_score", row.hubScore());
        projection.put("attrs_json", writeJson(row.attrs()));
        projection.entrySet().removeIf(entry -> entry.getValue() == null);
        return projection;
    }

    private Map<String, Object> toRelationshipProjection(EdgeRow row, boolean reverseTraversal) {
        Map<String, Object> projection = new LinkedHashMap<>();
        projection.put("graph_rel_id", row.edgeId() + (reverseTraversal ? ":rev" : ":fwd"));
        projection.put("edge_id", row.edgeId());
        projection.put("from_node_id", row.fromNodeId());
        projection.put("to_node_id", row.toNodeId());
        projection.put("traversal_from_node_id", reverseTraversal ? row.toNodeId() : row.fromNodeId());
        projection.put("traversal_to_node_id", reverseTraversal ? row.fromNodeId() : row.toNodeId());
        projection.put("edge_type", row.edgeType());
        projection.put("directed", row.directed());
        projection.put("tx_count", row.txCount());
        projection.put("tx_sum", row.txSum());
        projection.put("relation_family", row.relationFamily());
        projection.put("strength_score", row.strengthScore());
        projection.put("evidence_count", row.evidenceCount());
        projection.put("source_system", row.sourceSystem());
        projection.put("first_seen_at", row.firstSeenAt() == null ? null : row.firstSeenAt().toString());
        projection.put("last_seen_at", row.lastSeenAt() == null ? null : row.lastSeenAt().toString());
        projection.put("attrs_json", writeJson(row.attrs()));
        projection.entrySet().removeIf(entry -> entry.getValue() == null);
        return projection;
    }

    private List<List<Map<String, Object>>> partition(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return List.of();
        }

        List<List<Map<String, Object>>> batches = new ArrayList<>();
        for (int index = 0; index < rows.size(); index += SYNC_BATCH_SIZE) {
            batches.add(rows.subList(index, Math.min(rows.size(), index + SYNC_BATCH_SIZE)));
        }
        return batches;
    }

    private EdgeRow mapEdgeRow(org.neo4j.driver.Record record) {
        return new EdgeRow(
            nullableString(record, "edge_id"),
            nullableString(record, "from_node_id"),
            nullableString(record, "to_node_id"),
            nullableString(record, "edge_type"),
            nullableBoolean(record, "directed"),
            nullableLong(record, "tx_count"),
            nullableDouble(record, "tx_sum"),
            nullableString(record, "relation_family"),
            nullableDouble(record, "strength_score"),
            nullableLong(record, "evidence_count"),
            nullableString(record, "source_system"),
            parseInstant(nullableString(record, "first_seen_at")),
            parseInstant(nullableString(record, "last_seen_at")),
            readJsonMap(nullableString(record, "attrs_json"))
        );
    }

    private String nullableString(org.neo4j.driver.Record record, String key) {
        Value value = record.get(key);
        return value == null || value.isNull() ? null : value.asString();
    }

    private boolean nullableBoolean(org.neo4j.driver.Record record, String key) {
        Value value = record.get(key);
        return value != null && !value.isNull() && value.asBoolean();
    }

    private long nullableLong(org.neo4j.driver.Record record, String key) {
        Value value = record.get(key);
        return value == null || value.isNull() ? 0L : value.asLong();
    }

    private double nullableDouble(org.neo4j.driver.Record record, String key) {
        Value value = record.get(key);
        return value == null || value.isNull() ? 0D : value.asDouble();
    }

    private Instant parseInstant(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return Instant.parse(raw);
    }

    private Map<String, Object> readJsonMap(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return Map.of();
        }

        try {
            return objectMapper.readValue(rawJson, new TypeReference<>() {
            });
        } catch (Exception ex) {
            log.debug("Failed to parse Neo4j attrs_json '{}': {}", rawJson, ex.getMessage());
            return Map.of();
        }
    }

    private String writeJson(Map<String, Object> value) {
        if (value == null || value.isEmpty()) {
            return null;
        }

        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize Neo4j projection payload", ex);
        }
    }
}
