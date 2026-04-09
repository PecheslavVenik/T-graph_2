package com.pm.graph_api_v2.dto;

import java.util.List;

public record GraphNodeSummaryResponse(
    GraphNodeDto node,
    GraphNodeSummaryDto summary,
    List<GraphFacetCountDto> relationFamilies,
    List<GraphFacetCountDto> edgeTypes,
    List<GraphFacetCountDto> neighborNodeTypes,
    GraphExpandPreviewDto expandPreview
) {
}
