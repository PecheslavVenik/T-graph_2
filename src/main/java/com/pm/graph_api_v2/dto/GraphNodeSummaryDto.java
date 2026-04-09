package com.pm.graph_api_v2.dto;

public record GraphNodeSummaryDto(
    Direction requestedDirection,
    String relationFamily,
    int adjacentEdgeCount,
    int uniqueNeighborCount,
    int outboundEdgeCount,
    int inboundEdgeCount
) {
}
