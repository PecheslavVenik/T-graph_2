package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

@Component
public class GraphRequestNormalizer {

    private final GraphProperties graphProperties;

    public GraphRequestNormalizer(GraphProperties graphProperties) {
        this.graphProperties = graphProperties;
    }

    public String relationFamilyOrDefault(String relationFamily) {
        String normalized = GraphRelationFamilies.normalize(relationFamily);
        return normalized == null ? graphProperties.getDefaultRelationFamily() : normalized;
    }

    public String relationFamilyOrAll(String relationFamily) {
        String normalized = GraphRelationFamilies.normalize(relationFamily);
        return normalized == null ? GraphRelationFamilies.ALL_RELATIONS : normalized;
    }

    public List<String> edgeTypes(List<String> edgeTypes) {
        if (edgeTypes == null || edgeTypes.isEmpty()) {
            return List.of();
        }

        return edgeTypes.stream()
            .filter(value -> value != null && !value.isBlank())
            .map(value -> value.trim().toUpperCase(Locale.ROOT))
            .distinct()
            .toList();
    }

    public Direction directionOrBoth(Direction direction) {
        return direction == null ? Direction.BOTH : direction;
    }

    public boolean includeAttributes(Boolean includeAttributes) {
        return includeAttributes == null || includeAttributes;
    }

    public int orDefault(Integer value, int fallback) {
        return value == null ? fallback : value;
    }
}
