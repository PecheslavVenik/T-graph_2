package com.pm.graph_api_v2.dto;

public record GraphImportErrorDto(
    int rowNumber,
    String section,
    String field,
    String value,
    String message
) {
    public GraphImportErrorDto(int rowNumber, String section, String message) {
        this(rowNumber, section, null, null, message);
    }
}
