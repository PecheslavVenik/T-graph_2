package com.pm.graph_api_v2.dto;

import jakarta.validation.constraints.NotBlank;

public record SeedRef(
    @NotBlank String type,
    @NotBlank String value
) {
}
