package com.pm.graph_api_v2.repository.model;

import java.util.Map;

public record NodeRow(
    String nodeId,
    String partyRk,
    String personId,
    String phoneNo,
    String fullName,
    boolean blacklist,
    boolean vip,
    String employer,
    String city,
    double pagerankScore,
    double hubScore,
    Map<String, Object> attrs
) {
}
