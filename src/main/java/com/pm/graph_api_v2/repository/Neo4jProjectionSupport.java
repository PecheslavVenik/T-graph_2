package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class Neo4jProjectionSupport {

    private static final Logger log = LoggerFactory.getLogger(Neo4jProjectionSupport.class);
    public static final String PROJECTION_OWNER = "graph_api_v2";

    private final ObjectMapper objectMapper;

    public Neo4jProjectionSupport(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> toNodeProjection(NodeRow row) {
        Map<String, Object> projection = new LinkedHashMap<>();
        projection.put("node_id", row.nodeId());
        projection.put("projection_owner", PROJECTION_OWNER);
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

    public Map<String, Object> toRelationshipProjection(EdgeRow row, boolean reverseTraversal) {
        Map<String, Object> projection = new LinkedHashMap<>();
        projection.put("graph_rel_id", row.edgeId() + (reverseTraversal ? ":rev" : ":fwd"));
        projection.put("projection_owner", PROJECTION_OWNER);
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

    public List<List<Map<String, Object>>> partition(List<Map<String, Object>> rows, int batchSize) {
        if (rows.isEmpty()) {
            return List.of();
        }

        List<List<Map<String, Object>>> batches = new ArrayList<>();
        for (int index = 0; index < rows.size(); index += batchSize) {
            batches.add(rows.subList(index, Math.min(rows.size(), index + batchSize)));
        }
        return batches;
    }

    public EdgeRow mapEdgeRow(Record record) {
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

    private String nullableString(Record record, String key) {
        Value value = record.get(key);
        return value == null || value.isNull() ? null : value.asString();
    }

    private boolean nullableBoolean(Record record, String key) {
        Value value = record.get(key);
        return value != null && !value.isNull() && value.asBoolean();
    }

    private long nullableLong(Record record, String key) {
        Value value = record.get(key);
        return value == null || value.isNull() ? 0L : value.asLong();
    }

    private double nullableDouble(Record record, String key) {
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
