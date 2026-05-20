package com.pm.graph_api_v2.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.Map;

public record GraphExpandFiltersDto(
    @Size(max = 100) List<@NotBlank String> relationFamilies,
    @Size(max = 100) List<@NotBlank String> edgeTypes,
    @Size(max = 100) List<@NotBlank String> nodeTypes,
    Map<String, @Valid GraphAttributeFilterDto> nodeAttributes,
    Map<String, @Valid GraphAttributeFilterDto> edgeAttributes
) {
}
