package com.pm.graph_api_v2.repository.model;

import java.util.List;

public record PathRow(
    List<String> nodeIds,
    List<String> edgeIds,
    int hopCount
) {
}
