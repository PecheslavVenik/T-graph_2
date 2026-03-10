package com.pm.graph_api_v2.integration;

import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;

import java.time.Instant;
import java.util.List;

public record ExternalGraphIngestRequest(
    String source,
    Instant producedAt,
    List<GraphNodeDto> nodes,
    List<GraphEdgeDto> edges
) {
}
