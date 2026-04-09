package com.pm.graph_api_v2.dto;

public enum GraphRelationFamily {
    PERSON_KNOWS_PERSON("knows"),
    PERSON_RELATIVE_PERSON("relative"),
    PERSON_SAME_CITY_PERSON("same_city"),
    ACCOUNT_FLOW("account_flow"),
    CUSTOMER_OWNERSHIP("customer_ownership"),
    SHARED_INFRASTRUCTURE("shared_infrastructure"),
    CORPORATE_CONTROL("corporate_control"),
    ALL_RELATIONS("all");

    private final String projectionSuffix;

    GraphRelationFamily(String projectionSuffix) {
        this.projectionSuffix = projectionSuffix;
    }

    public boolean isAllRelations() {
        return this == ALL_RELATIONS;
    }

    public String projectionTableName() {
        return isAllRelations() ? "g_pgq_edges" : "g_pgq_edges_" + projectionSuffix;
    }

    public String graphName() {
        return isAllRelations() ? "aml_graph_all" : "aml_graph_" + projectionSuffix;
    }
}
