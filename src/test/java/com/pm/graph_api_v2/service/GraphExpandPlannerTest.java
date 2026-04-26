package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.repository.GraphRepository;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GraphExpandPlannerTest {

    @Test
    @SuppressWarnings("unchecked")
    void plan_shouldLimitUniqueNeighborsWithoutDroppingParallelEdges() {
        GraphRepository graphRepository = mock(GraphRepository.class);
        when(graphRepository.findNodesByIds(anyCollection())).thenAnswer(invocation -> {
            Collection<String> nodeIds = invocation.getArgument(0);
            return nodeIds.stream().map(this::node).toList();
        });

        GraphExpandPlanner planner = new GraphExpandPlanner(
            graphRepository,
            new GraphDtoMapper(),
            new GraphProperties()
        );

        GraphExpandPlanner.ExpandPlan plan = planner.plan(
            new LinkedHashSet<>(List.of("S")),
            List.of(
                edge("E1", "S", "N1", 1.0),
                edge("E2", "S", "N1", 0.9),
                edge("E3", "S", "N2", 0.8)
            ),
            Direction.OUTBOUND,
            1,
            10,
            10
        );

        assertThat(plan.edges())
            .extracting(EdgeRow::edgeId)
            .containsExactly("E1", "E2");
        assertThat(plan.nodeIds()).containsExactly("S", "N1");
        assertThat(plan.perSeedTruncated()).isTrue();
    }

    private NodeRow node(String nodeId) {
        return new NodeRow(
            nodeId,
            "TEST",
            nodeId,
            null,
            null,
            null,
            nodeId,
            false,
            false,
            null,
            null,
            "test",
            0D,
            0D,
            Map.of(),
            Map.of()
        );
    }

    private EdgeRow edge(String edgeId, String fromNodeId, String toNodeId, double strengthScore) {
        return new EdgeRow(
            edgeId,
            fromNodeId,
            toNodeId,
            "TEST_EDGE",
            true,
            0,
            0D,
            "TEST_RELATION",
            strengthScore,
            0,
            "test",
            null,
            null,
            Map.of()
        );
    }
}
