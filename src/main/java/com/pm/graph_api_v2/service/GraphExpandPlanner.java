package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.repository.GraphRepository;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class GraphExpandPlanner {

    public static final String RANKING_STRATEGY = "GENERIC_ONE_HOP_RANKING";

    private final GraphRepository graphRepository;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphProperties graphProperties;

    public GraphExpandPlanner(GraphRepository graphRepository,
                              GraphDtoMapper graphDtoMapper,
                              GraphProperties graphProperties) {
        this.graphRepository = graphRepository;
        this.graphDtoMapper = graphDtoMapper;
        this.graphProperties = graphProperties;
    }

    public ExpandPlan plan(LinkedHashSet<String> seedNodeIds,
                           List<EdgeRow> candidateEdges,
                           Direction direction,
                           int maxNeighborsPerSeed,
                           int maxNodes,
                           int maxEdges) {
        Map<String, NodeRow> rankingNodesById = loadRankingNodes(seedNodeIds, candidateEdges);
        List<EdgeRow> rankedEdges = rankCandidateEdges(candidateEdges, seedNodeIds, direction, rankingNodesById);
        PerSeedLimitResult perSeedLimit = applyPerSeedLimit(rankedEdges, seedNodeIds, direction, maxNeighborsPerSeed);
        LimitSelection selection = applyGlobalLimits(perSeedLimit.edges(), seedNodeIds, maxNodes, maxEdges);

        return new ExpandPlan(
            rankingNodesById,
            selection.nodeIds(),
            selection.edges(),
            perSeedLimit.truncated(),
            selection.truncated()
        );
    }

    public GraphEdgeDto toEdgeDto(EdgeRow row,
                                  boolean includeAttributes,
                                  Set<String> seedNodeIds,
                                  Direction direction,
                                  Map<String, NodeRow> nodesById) {
        GraphEdgeDto baseEdge = graphDtoMapper.toEdgeDto(row, includeAttributes);
        if (!includeAttributes) {
            return baseEdge;
        }

        Map<String, Object> attributes = new LinkedHashMap<>();
        if (baseEdge.attributes() != null) {
            attributes.putAll(baseEdge.attributes());
        }
        attributes.put("rankingScore", scoreEdge(row, seedNodeIds, direction, nodesById));

        return new GraphEdgeDto(
            baseEdge.edgeId(),
            baseEdge.fromNodeId(),
            baseEdge.toNodeId(),
            baseEdge.type(),
            baseEdge.relationFamily(),
            baseEdge.directed(),
            baseEdge.weight(),
            baseEdge.sourceSystem(),
            baseEdge.firstSeenAt(),
            baseEdge.lastSeenAt(),
            attributes
        );
    }

    private Map<String, NodeRow> loadRankingNodes(Set<String> seedNodeIds, List<EdgeRow> candidateEdges) {
        LinkedHashSet<String> nodeIds = new LinkedHashSet<>(seedNodeIds);
        for (EdgeRow edge : candidateEdges) {
            nodeIds.add(edge.fromNodeId());
            nodeIds.add(edge.toNodeId());
        }

        Map<String, NodeRow> nodesById = new LinkedHashMap<>();
        for (NodeRow row : graphRepository.findNodesByIds(nodeIds)) {
            nodesById.put(row.nodeId(), row);
        }
        return nodesById;
    }

    private List<EdgeRow> rankCandidateEdges(List<EdgeRow> candidateEdges,
                                             Set<String> seedNodeIds,
                                             Direction direction,
                                             Map<String, NodeRow> nodesById) {
        Comparator<EdgeRow> comparator = Comparator
            .comparingDouble((EdgeRow edge) -> scoreEdge(edge, seedNodeIds, direction, nodesById))
            .reversed()
            .thenComparing(Comparator.comparingDouble(EdgeRow::strengthScore).reversed())
            .thenComparing(EdgeRow::edgeId);

        return candidateEdges.stream().sorted(comparator).toList();
    }

    private double scoreEdge(EdgeRow edge,
                             Set<String> seedNodeIds,
                             Direction direction,
                             Map<String, NodeRow> nodesById) {
        List<String> relatedSeeds = relatedSeeds(edge, seedNodeIds, direction);
        if (relatedSeeds.isEmpty()) {
            return Double.NEGATIVE_INFINITY;
        }

        return relatedSeeds.stream()
            .mapToDouble(seedNodeId -> rankOneHopEdge(edge, seedNodeId, nodesById))
            .max()
            .orElse(Double.NEGATIVE_INFINITY);
    }

    private double rankOneHopEdge(EdgeRow edge, String anchorSeedNodeId, Map<String, NodeRow> nodesById) {
        String neighborNodeId = anchorSeedNodeId.equals(edge.fromNodeId()) ? edge.toNodeId() : edge.fromNodeId();
        NodeRow neighborNode = nodesById.get(neighborNodeId);

        double neighborPageRank = neighborNode == null ? 0.0d : neighborNode.pagerankScore();
        double hubPenalty = neighborNode == null ? 0.0d : neighborNode.hubScore() * graphProperties.getHubPenaltyPercent() / 100.0d;
        double evidenceBoost = Math.min(edge.evidenceCount(), 10L) * 0.02d;
        double flowBoost = Math.min(edge.txCount(), 25L) * 0.005d;

        return edge.strengthScore() * 0.55d
            + neighborPageRank * 0.30d
            + evidenceBoost
            + flowBoost
            - hubPenalty;
    }

    private PerSeedLimitResult applyPerSeedLimit(List<EdgeRow> candidateEdges,
                                                 Set<String> seedNodeIds,
                                                 Direction direction,
                                                 int maxNeighborsPerSeed) {
        Map<String, Set<String>> neighborsPerSeed = new LinkedHashMap<>();
        Set<String> acceptedEdgeIds = new LinkedHashSet<>();
        List<EdgeRow> accepted = new ArrayList<>();
        boolean truncated = false;

        for (EdgeRow edge : candidateEdges) {
            List<String> relatedSeeds = relatedSeeds(edge, seedNodeIds, direction);
            if (relatedSeeds.isEmpty()) {
                continue;
            }

            List<String> acceptedSeeds = relatedSeeds.stream()
                .filter(seed -> canIncludeNeighbor(neighborsPerSeed, seed, neighborForSeed(edge, seed), maxNeighborsPerSeed))
                .toList();

            if (acceptedSeeds.isEmpty()) {
                truncated = true;
                continue;
            }
            if (!acceptedEdgeIds.add(edge.edgeId())) {
                continue;
            }

            accepted.add(edge);
            for (String seed : acceptedSeeds) {
                neighborsPerSeed
                    .computeIfAbsent(seed, ignored -> new LinkedHashSet<>())
                    .add(neighborForSeed(edge, seed));
            }
        }

        return new PerSeedLimitResult(accepted, truncated);
    }

    private boolean canIncludeNeighbor(Map<String, Set<String>> neighborsPerSeed,
                                       String seedNodeId,
                                       String neighborNodeId,
                                       int maxNeighborsPerSeed) {
        Set<String> neighbors = neighborsPerSeed.get(seedNodeId);
        return neighbors == null || neighbors.contains(neighborNodeId) || neighbors.size() < maxNeighborsPerSeed;
    }

    private String neighborForSeed(EdgeRow edge, String seedNodeId) {
        return seedNodeId.equals(edge.fromNodeId()) ? edge.toNodeId() : edge.fromNodeId();
    }

    private LimitSelection applyGlobalLimits(List<EdgeRow> candidateEdges,
                                             LinkedHashSet<String> seedNodeIds,
                                             int maxNodes,
                                             int maxEdges) {
        LinkedHashSet<String> nodeIds = new LinkedHashSet<>(seedNodeIds);
        List<EdgeRow> edges = new ArrayList<>();
        boolean truncated = false;

        for (EdgeRow edge : candidateEdges) {
            if (edges.size() >= maxEdges) {
                truncated = true;
                break;
            }

            int additionalNodes = 0;
            if (!nodeIds.contains(edge.fromNodeId())) {
                additionalNodes++;
            }
            if (!nodeIds.contains(edge.toNodeId())) {
                additionalNodes++;
            }
            if (nodeIds.size() + additionalNodes > maxNodes) {
                truncated = true;
                continue;
            }

            nodeIds.add(edge.fromNodeId());
            nodeIds.add(edge.toNodeId());
            edges.add(edge);
        }

        return new LimitSelection(nodeIds, edges, truncated);
    }

    private List<String> relatedSeeds(EdgeRow edge, Set<String> seedNodeIds, Direction direction) {
        List<String> seeds = new ArrayList<>(2);
        switch (direction) {
            case OUTBOUND -> {
                if (seedNodeIds.contains(edge.fromNodeId())) {
                    seeds.add(edge.fromNodeId());
                }
                if (!edge.directed() && seedNodeIds.contains(edge.toNodeId())) {
                    seeds.add(edge.toNodeId());
                }
            }
            case INBOUND -> {
                if (seedNodeIds.contains(edge.toNodeId())) {
                    seeds.add(edge.toNodeId());
                }
                if (!edge.directed() && seedNodeIds.contains(edge.fromNodeId())) {
                    seeds.add(edge.fromNodeId());
                }
            }
            case BOTH -> {
                if (seedNodeIds.contains(edge.fromNodeId())) {
                    seeds.add(edge.fromNodeId());
                }
                if (seedNodeIds.contains(edge.toNodeId())) {
                    seeds.add(edge.toNodeId());
                }
            }
        }
        return seeds;
    }

    public record ExpandPlan(Map<String, NodeRow> nodesById,
                             LinkedHashSet<String> nodeIds,
                             List<EdgeRow> edges,
                             boolean perSeedTruncated,
                             boolean globalTruncated) {
    }

    private record PerSeedLimitResult(List<EdgeRow> edges, boolean truncated) {
    }

    private record LimitSelection(LinkedHashSet<String> nodeIds, List<EdgeRow> edges, boolean truncated) {
    }
}
