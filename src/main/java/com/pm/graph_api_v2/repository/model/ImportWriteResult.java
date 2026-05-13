package com.pm.graph_api_v2.repository.model;

public record ImportWriteResult(
    int insertedNodeCount,
    int updatedNodeCount,
    int insertedEdgeCount,
    int updatedEdgeCount
) {
}
