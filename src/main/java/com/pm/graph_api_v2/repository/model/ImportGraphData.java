package com.pm.graph_api_v2.repository.model;

import java.util.List;

public record ImportGraphData(
    List<ImportNodeRow> nodes,
    List<ImportEdgeRow> edges
) {
}
