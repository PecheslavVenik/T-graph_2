package com.pm.graph_api_v2.dto;

public record GraphImportErrorDto(
    int rowNumber,
    String section,
    String message
) {
}
