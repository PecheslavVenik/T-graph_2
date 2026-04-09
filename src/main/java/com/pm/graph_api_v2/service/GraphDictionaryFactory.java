package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.dto.GraphDictionaryResponse;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class GraphDictionaryFactory {

    public GraphDictionaryResponse create(List<String> edgeTypes,
                                          List<String> relationFamilies,
                                          List<String> nodeTypes,
                                          List<String> nodeStatuses) {
        return new GraphDictionaryResponse(
            edgeTypes,
            relationFamilies,
            nodeTypes,
            nodeStatuses,
            buildStyleHints(edgeTypes, relationFamilies, nodeTypes, nodeStatuses)
        );
    }

    private Map<String, String> buildStyleHints(List<String> edgeTypes,
                                                List<String> relationFamilies,
                                                List<String> nodeTypes,
                                                List<String> nodeStatuses) {
        LinkedHashMap<String, String> hints = new LinkedHashMap<>();
        appendHints(hints, nodeStatuses, "status");
        appendHints(hints, nodeTypes, "node-type");
        appendHints(hints, edgeTypes, "edge-type");
        appendHints(hints, relationFamilies, "relation-family");
        return hints;
    }

    private void appendHints(Map<String, String> hints, List<String> values, String category) {
        for (String value : values) {
            hints.putIfAbsent(value, "legend:" + category + ":" + normalize(value));
        }
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT).replace('_', '-');
    }
}
