package com.pm.graph_api_v2.dto;

public record GraphMetaDto(
    boolean truncated,
    long executionMs,
    GraphSource source
) {
}
