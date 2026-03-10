package com.pm.graph_api_v2.dto;

import java.util.List;

public record GraphEnvelopeDto(
    List<GraphNodeDto> nodes,
    List<GraphEdgeDto> edges,
    GraphMetaDto meta
) {
}
