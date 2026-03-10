package com.pm.graph_api_v2.dto;

import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record GraphEdgeDto(
    @NotBlank String edgeId,
    @NotBlank String fromNodeId,
    @NotBlank String toNodeId,
    @NotBlank String type,
    boolean directed,
    Double weight,
    Map<String, Object> attributes
) {
}
