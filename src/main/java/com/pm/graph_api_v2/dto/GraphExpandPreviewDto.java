package com.pm.graph_api_v2.dto;

public record GraphExpandPreviewDto(
    int defaultMaxNeighborsPerSeed,
    int defaultMaxNodes,
    int defaultMaxEdges,
    boolean wouldTruncateByNeighborBudget
) {
}
