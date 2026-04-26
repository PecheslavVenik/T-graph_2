package com.pm.graph_api_v2.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record ShortestPathRequest(
    @NotNull @Valid SeedRef source,
    @NotNull @Valid SeedRef target,
    String relationFamily,
    @NotNull Direction direction,
    @Min(1) @Max(10) Integer maxDepth
) {
}
