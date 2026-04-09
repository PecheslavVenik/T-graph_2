package com.pm.graph_api_v2.dto;

import jakarta.validation.constraints.NotBlank;

import java.util.Map;
import java.util.Set;

public record GraphNodeDto(
    @NotBlank String nodeId,
    String nodeType,
    @NotBlank String displayName,
    Map<String, String> identifiers,
    Set<String> statuses,
    Map<String, Object> attributes
) {
}
