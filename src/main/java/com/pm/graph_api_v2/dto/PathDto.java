package com.pm.graph_api_v2.dto;

import java.util.List;

public record PathDto(
    List<String> orderedNodeIds,
    List<String> orderedEdgeIds,
    int hopCount
) {
}
