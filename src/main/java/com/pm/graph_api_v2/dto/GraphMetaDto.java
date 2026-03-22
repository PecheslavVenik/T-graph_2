package com.pm.graph_api_v2.dto;

import java.util.List;

public record GraphMetaDto(
    boolean truncated,
    long executionMs,
    GraphSource source,
    String relationFamily,
    String rankingStrategy,
    int candidateEdgeCount,
    int returnedNodeCount,
    int returnedEdgeCount,
    List<String> warnings
) {
}
