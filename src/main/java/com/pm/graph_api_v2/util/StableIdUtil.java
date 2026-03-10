package com.pm.graph_api_v2.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class StableIdUtil {

    private StableIdUtil() {
    }

    public static String stableNodeId(String existingNodeId,
                                      String partyRk,
                                      String personId,
                                      String phoneNo) {
        if (hasText(existingNodeId)) {
            return existingNodeId;
        }

        if (hasText(partyRk)) {
            return "party:" + normalize(partyRk);
        }

        String composite = normalize(personId) + "|" + normalize(phoneNo);
        return "node:" + hash16(composite);
    }

    public static String stableEdgeId(String existingEdgeId,
                                      String fromNodeId,
                                      String toNodeId,
                                      String type,
                                      boolean directed,
                                      String sourceRef) {
        if (hasText(existingEdgeId)) {
            return existingEdgeId;
        }

        String composite = String.join("|",
            normalize(fromNodeId),
            normalize(toNodeId),
            normalize(type),
            Boolean.toString(directed),
            normalize(sourceRef));

        return "edge:" + hash16(composite);
    }

    private static String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String hash16(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", bytes[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable", ex);
        }
    }
}
