package com.pm.graph_api_v2.dto;

import java.util.List;
import java.util.Map;

public record GraphDictionaryResponse(
    List<String> edgeTypes,
    List<String> relationFamilies,
    List<String> nodeTypes,
    List<String> nodeStatuses,
    Map<String, String> styleHints
) {
}
