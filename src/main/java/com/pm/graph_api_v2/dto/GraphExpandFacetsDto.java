package com.pm.graph_api_v2.dto;

import java.util.List;
import java.util.Map;

public record GraphExpandFacetsDto(
    List<GraphFacetCountDto> relationFamilies,
    List<GraphFacetCountDto> edgeTypes,
    List<GraphFacetCountDto> neighborNodeTypes,
    Map<String, List<GraphFacetCountDto>> nodeAttributes,
    Map<String, List<GraphFacetCountDto>> edgeAttributes
) {
}
