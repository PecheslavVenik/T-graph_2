package com.pm.graph_api_v2.util;

import java.util.Locale;

public final class GraphRelationFamilies {

    public static final String ALL_RELATIONS = "ALL_RELATIONS";

    private GraphRelationFamilies() {
    }

    public static String normalize(String relationFamily) {
        if (relationFamily == null) {
            return null;
        }

        String normalized = relationFamily.trim().toUpperCase(Locale.ROOT);
        return normalized.isBlank() ? null : normalized;
    }

    public static boolean isAllRelations(String relationFamily) {
        return ALL_RELATIONS.equals(normalize(relationFamily));
    }

    public static String projectionTableName(String relationFamily) {
        if (isAllRelations(relationFamily)) {
            return "g_pgq_edges";
        }
        return "g_pgq_edges_" + slug(normalize(relationFamily));
    }

    public static String graphName(String relationFamily) {
        if (isAllRelations(relationFamily)) {
            return "aml_graph_all";
        }
        return "aml_graph_" + slug(normalize(relationFamily));
    }

    private static String slug(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }

        String slug = value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_");
        slug = slug.replaceAll("_+", "_");
        slug = slug.replaceAll("^_|_$", "");
        return slug.isBlank() ? "unknown" : slug;
    }
}
