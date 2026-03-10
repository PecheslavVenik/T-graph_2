package com.pm.graph_api_v2.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record SeedRef(
    @NotNull SeedType type,
    @NotBlank String value
) {
}
