package com.pm.graph_api_v2.repository.model;

import java.util.List;

public record SqlGraphQueryResult(
    List<String> nodeIds,
    List<String> edgeIds,
    List<NodePair> nodePairs,
    int rowCount
) {
}
