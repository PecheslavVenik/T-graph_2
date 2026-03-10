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

        Map<String, String> identifiers = new LinkedHashMap<>();
        putIdentifierIfPresent(identifiers, "party_rk", row.partyRk());
        putIdentifierIfPresent(identifiers, "person_id", row.personId());
        putIdentifierIfPresent(identifiers, "phone_no", row.phoneNo());

        Set<String> statuses = new LinkedHashSet<>();
        if (row.blacklist()) {
            statuses.add("BLACKLIST");
        }
        if (row.vip()) {
            statuses.add("VIP");
        }
        if (row.employer() != null && !row.employer().isBlank()) {
            statuses.add("EMPLOYED");
        }

        Map<String, Object> attributes = new LinkedHashMap<>();
        if (includeAttributes) {
            attributes.putAll(row.attrs());
            putAttributeIfPresent(attributes, "employer", row.employer());
        }

        String displayName = firstNonBlank(row.fullName(), row.partyRk(), row.personId(), row.phoneNo(), nodeId);

        return new GraphNodeDto(nodeId, displayName, identifiers, statuses, attributes);
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
        }

        return new GraphEdgeDto(
            edgeId,
            row.fromNodeId(),
            row.toNodeId(),
            row.edgeType(),
            row.directed(),
            row.txSum(),
            attributes
        );
    }

    private void putIdentifierIfPresent(Map<String, String> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, value);
        }
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
