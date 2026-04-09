package com.pm.graph_api_v2.dto;

import jakarta.validation.constraints.NotBlank;

import java.time.Instant;
import java.util.Map;

public record GraphEdgeDto(
    @NotBlank String edgeId,
    @NotBlank String fromNodeId,
    @NotBlank String toNodeId,
    @NotBlank String type,
    String relationFamily,
    boolean directed,
    Double weight,
    String sourceSystem,
    Instant firstSeenAt,
    Instant lastSeenAt,
    Map<String, Object> attributes
) {
}
