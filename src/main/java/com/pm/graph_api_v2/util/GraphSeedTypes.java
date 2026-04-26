package com.pm.graph_api_v2.util;

import java.util.Locale;
import java.util.Map;

public final class GraphSeedTypes {

    private static final Map<String, String> ALIASES = Map.ofEntries(
        Map.entry("ID", "NODE_ID"),
        Map.entry("NODE", "NODE_ID"),
        Map.entry("PHONE", "PHONE_NO"),
        Map.entry("PHONE_NUMBER", "PHONE_NO"),
        Map.entry("PARTY", "PARTY_RK")
    );

    private GraphSeedTypes() {
    }

    public static String normalize(String rawType) {
        if (rawType == null) {
            return null;
        }

        String normalized = rawType.trim().toUpperCase(Locale.ROOT);
        if (normalized.isBlank()) {
            return null;
        }
        return ALIASES.getOrDefault(normalized, normalized);
    }

    public static boolean isNodeId(String rawType) {
        return "NODE_ID".equals(normalize(rawType));
    }
}
