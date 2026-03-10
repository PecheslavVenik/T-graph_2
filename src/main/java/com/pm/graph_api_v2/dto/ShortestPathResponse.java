package com.pm.graph_api_v2.dto;

import java.util.List;

public record ShortestPathResponse(
    PathDto path,
    List<GraphNodeDto> nodes,
    List<GraphEdgeDto> edges,
    GraphMetaDto meta
) {
}
