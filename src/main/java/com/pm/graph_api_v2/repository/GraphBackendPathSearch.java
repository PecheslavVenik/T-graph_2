package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

final class GraphBackendPathSearch {

    private static final int PER_NODE_CANDIDATE_LIMIT = 10_000;

    private GraphBackendPathSearch() {
    }

    static Optional<PathRow> breadthFirst(String sourceNodeId,
                                          String targetNodeId,
                                          String relationFamily,
                                          Direction direction,
                                          int maxDepth,
                                          ExpandLookup expandLookup) {
        if (sourceNodeId == null || sourceNodeId.isBlank() || targetNodeId == null || targetNodeId.isBlank()) {
            return Optional.empty();
        }
        if (sourceNodeId.equals(targetNodeId)) {
            return Optional.of(new PathRow(List.of(sourceNodeId), List.of(), 0));
        }

        ArrayDeque<PathState> queue = new ArrayDeque<>();
        queue.add(new PathState(sourceNodeId, List.of(sourceNodeId), List.of()));
        Set<String> globallySeen = new HashSet<>();
        globallySeen.add(sourceNodeId);

        int boundedDepth = Math.max(1, maxDepth);
        while (!queue.isEmpty()) {
            PathState current = queue.removeFirst();
            if (current.edgeIds().size() >= boundedDepth) {
                continue;
            }

            List<EdgeRow> edges = expandLookup.findExpandEdges(
                List.of(current.nodeId()),
                relationFamily,
                List.of(),
                direction,
                PER_NODE_CANDIDATE_LIMIT
            );

            for (EdgeRow edge : edges) {
                String nextNodeId = neighborFor(edge, current.nodeId(), direction);
                if (nextNodeId == null || current.nodeIds().contains(nextNodeId)) {
                    continue;
                }

                List<String> nextNodeIds = new ArrayList<>(current.nodeIds());
                nextNodeIds.add(nextNodeId);
                List<String> nextEdgeIds = new ArrayList<>(current.edgeIds());
                nextEdgeIds.add(edge.edgeId());

                if (targetNodeId.equals(nextNodeId)) {
                    return Optional.of(new PathRow(nextNodeIds, nextEdgeIds, nextEdgeIds.size()));
                }

                if (globallySeen.add(nextNodeId)) {
                    queue.addLast(new PathState(nextNodeId, nextNodeIds, nextEdgeIds));
                }
            }
        }
        return Optional.empty();
    }

    private static String neighborFor(EdgeRow edge, String nodeId, Direction direction) {
        return switch (direction) {
            case OUTBOUND -> outboundNeighbor(edge, nodeId);
            case INBOUND -> inboundNeighbor(edge, nodeId);
            case BOTH -> {
                String outbound = outboundNeighbor(edge, nodeId);
                yield outbound == null ? inboundNeighbor(edge, nodeId) : outbound;
            }
        };
    }

    private static String outboundNeighbor(EdgeRow edge, String nodeId) {
        if (edge.fromNodeId().equals(nodeId)) {
            return edge.toNodeId();
        }
        if (!edge.directed() && edge.toNodeId().equals(nodeId)) {
            return edge.fromNodeId();
        }
        return null;
    }

    private static String inboundNeighbor(EdgeRow edge, String nodeId) {
        if (edge.toNodeId().equals(nodeId)) {
            return edge.fromNodeId();
        }
        if (!edge.directed() && edge.fromNodeId().equals(nodeId)) {
            return edge.toNodeId();
        }
        return null;
    }

    @FunctionalInterface
    interface ExpandLookup {
        List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                      String relationFamily,
                                      List<String> edgeTypes,
                                      Direction direction,
                                      int candidateLimit);
    }

    private record PathState(String nodeId, List<String> nodeIds, List<String> edgeIds) {
    }
}
