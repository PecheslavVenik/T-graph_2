package com.pm.graph_api_v2.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.List;

public record GraphExpandExcludeDto(
    @Size(max = 5000) List<@NotBlank String> nodeIds,
    @Size(max = 5000) List<@NotBlank String> edgeIds
) {
}
