package com.pm.graph_api_v2.repository.model;

import java.util.Map;

public record EdgeRow(
    String edgeId,
    String fromNodeId,
    String toNodeId,
    String edgeType,
    boolean directed,
    long txCount,
    double txSum,
    String relationFamily,
    double strengthScore,
    long evidenceCount,
    Map<String, Object> attrs
) {
}
