package com.pm.graph_api_v2.dto;

import java.util.List;

public record GraphNodeSearchResponse(
    List<GraphNodeDto> nodes,
    GraphNodeSearchMetaDto meta
) {
}
