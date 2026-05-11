package com.pm.graph_api_v2.dto;

public record GraphNodeSearchMetaDto(
    String query,
    String nodeType,
    int limit,
    int returnedNodeCount,
    boolean truncated
) {
}
