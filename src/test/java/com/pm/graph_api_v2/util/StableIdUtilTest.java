package com.pm.graph_api_v2.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StableIdUtilTest {

    @Test
    void stableNodeId_shouldBeDeterministic() {
        String first = StableIdUtil.stableNodeId(null, "PARTY_1001", "PERS_1", "+70000000001");
        String second = StableIdUtil.stableNodeId(null, "PARTY_1001", "PERS_1", "+70000000001");

        assertEquals(first, second);
        assertEquals("party:party_1001", first);
    }

    @Test
    void stableEdgeId_shouldBeDeterministic() {
        String first = StableIdUtil.stableEdgeId(null, "A", "B", "TRANSFER", true, "tx:1");
        String second = StableIdUtil.stableEdgeId(null, "A", "B", "TRANSFER", true, "tx:1");

        assertEquals(first, second);
    }
}
