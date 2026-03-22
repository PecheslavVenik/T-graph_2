package com.pm.graph_api_v2.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

public record GraphExpandRequest(
    @NotEmpty @Size(max = 100) List<@Valid SeedRef> seeds,
    GraphRelationFamily relationFamily,
    @NotNull Direction direction,
    @Min(1) @Max(1000) Integer maxNeighborsPerSeed,
    @Min(1) @Max(1000) Integer maxNodes,
    @Min(1) @Max(5000) Integer maxEdges,
    Boolean includeAttributes
) {
}
