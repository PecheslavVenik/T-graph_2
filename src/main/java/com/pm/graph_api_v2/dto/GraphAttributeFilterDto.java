package com.pm.graph_api_v2.dto;

import java.util.List;

public record GraphAttributeFilterDto(
    Object eq,
    List<Object> in,
    Double gte,
    Double lte
) {
}
