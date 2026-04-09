package com.pm.graph_api_v2.repository.model;

public record NodeNeighborhoodSummaryRow(
    int adjacentEdgeCount,
    int uniqueNeighborCount,
    int outboundEdgeCount,
    int inboundEdgeCount
) {
}
