package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.util.StableIdUtil;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

@Component
public class GraphDtoMapper {

    public GraphNodeDto toNodeDto(NodeRow row, boolean includeAttributes) {
        String nodeId = StableIdUtil.stableNodeId(row.nodeId(), row.partyRk(), row.personId(), row.phoneNo());
        Map<String, String> identifiers = new LinkedHashMap<>(row.identifiers());

        Set<String> statuses = new LinkedHashSet<>();
        if (row.blacklist()) {
            statuses.add("BLACKLIST");
        }
        if (row.vip()) {
            statuses.add("VIP");
        }

        Map<String, Object> attributes = new LinkedHashMap<>();
        if (includeAttributes) {
            attributes.putAll(row.attrs());
            putAttributeIfPresent(attributes, "employer", row.employer());
            putAttributeIfPresent(attributes, "city", row.city());
            attributes.put("pagerankScore", row.pagerankScore());
            attributes.put("hubScore", row.hubScore());
        }

        String displayName = firstNonBlank(
            row.displayName(),
            row.fullName(),
            identifiers.get("party_rk"),
            identifiers.get("person_id"),
            identifiers.get("phone_no"),
            nodeId
        );

        return new GraphNodeDto(nodeId, row.nodeType(), displayName, identifiers, statuses, attributes);
    }

    public GraphEdgeDto toEdgeDto(EdgeRow row, boolean includeAttributes) {
        String edgeId = StableIdUtil.stableEdgeId(
            row.edgeId(),
            row.fromNodeId(),
            row.toNodeId(),
            row.edgeType(),
            row.directed(),
            row.edgeType() + ":" + row.txCount()
        );

        Map<String, Object> attributes = new LinkedHashMap<>();
        if (includeAttributes) {
            attributes.putAll(row.attrs());
            attributes.put("txCount", row.txCount());
            attributes.put("txSum", row.txSum());
            attributes.put("strengthScore", row.strengthScore());
            attributes.put("evidenceCount", row.evidenceCount());
        }

        return new GraphEdgeDto(
            edgeId,
            row.fromNodeId(),
            row.toNodeId(),
            row.edgeType(),
            row.relationFamily(),
            row.directed(),
            row.strengthScore(),
            row.sourceSystem(),
            row.firstSeenAt(),
            row.lastSeenAt(),
            attributes
        );
    }

    private void putAttributeIfPresent(Map<String, Object> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, value);
        }
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "unknown";
    }
}
