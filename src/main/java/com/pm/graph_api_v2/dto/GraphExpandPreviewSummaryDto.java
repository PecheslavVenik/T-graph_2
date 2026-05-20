package com.pm.graph_api_v2.dto;

public record GraphExpandPreviewSummaryDto(
    int adjacentEdgeCount,
    int uniqueNeighborCount,
    int newNodeCount,
    int newEdgeCount
) {
}
