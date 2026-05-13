package com.pm.graph_api_v2.dto;

import java.util.List;
import java.util.Map;

public record GraphImportResponse(
    String fileName,
    String status,
    int parsedNodeCount,
    int parsedEdgeCount,
    int inferredNodeCount,
    int invalidRowCount,
    int insertedNodeCount,
    int updatedNodeCount,
    int insertedEdgeCount,
    int updatedEdgeCount,
    List<GraphImportErrorDto> errors,
    List<String> warnings,
    List<Map<String, String>> sampleRows
) {
}
