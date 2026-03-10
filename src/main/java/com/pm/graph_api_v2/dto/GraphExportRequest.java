package com.pm.graph_api_v2.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.List;

public record GraphExportRequest(
    @NotNull @Size(min = 1, max = 1000) List<@Valid GraphNodeDto> nodes,
    @NotNull @Size(max = 5000) List<@Valid GraphEdgeDto> edges,
    GraphMetaDto meta
) {
}
