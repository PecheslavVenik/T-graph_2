package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.DuckPgqProperties;
import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphEnvelopeDto;
import com.pm.graph_api_v2.dto.GraphExpandRequest;
import com.pm.graph_api_v2.dto.GraphExpandResponse;
import com.pm.graph_api_v2.dto.GraphMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.exception.ApiBadRequestException;
import com.pm.graph_api_v2.exception.ApiNotFoundException;
import com.pm.graph_api_v2.exception.ApiServiceUnavailableException;
import com.pm.graph_api_v2.metrics.GraphMetrics;
import com.pm.graph_api_v2.repository.DuckPgqGraphQueryRepository;
import com.pm.graph_api_v2.repository.GraphRepository;
import com.pm.graph_api_v2.repository.SqlGraphQueryRepository;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class GraphExpandService {

    private static final Logger log = LoggerFactory.getLogger(GraphExpandService.class);
    private static final Set<String> PRESERVED_NODE_ATTRIBUTE_KEYS = Set.of(
        "x", "y", "fx", "fy", "vx", "vy", "position", "pinned", "selected", "collapsed", "hidden"
    );
    private static final Set<String> PRESERVED_EDGE_ATTRIBUTE_KEYS = Set.of("selected", "collapsed", "hidden");

    private final GraphRepository graphRepository;
    private final DuckPgqGraphQueryRepository duckPgqGraphQueryRepository;
    private final SqlGraphQueryRepository sqlGraphQueryRepository;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphProperties graphProperties;
    private final DuckPgqProperties duckPgqProperties;
    private final GraphMetrics graphMetrics;

    public GraphExpandService(GraphRepository graphRepository,
                              DuckPgqGraphQueryRepository duckPgqGraphQueryRepository,
                              SqlGraphQueryRepository sqlGraphQueryRepository,
                              GraphDtoMapper graphDtoMapper,
                              GraphProperties graphProperties,
                              DuckPgqProperties duckPgqProperties,
                              GraphMetrics graphMetrics) {
        this.graphRepository = graphRepository;
        this.duckPgqGraphQueryRepository = duckPgqGraphQueryRepository;
        this.sqlGraphQueryRepository = sqlGraphQueryRepository;
        this.graphDtoMapper = graphDtoMapper;
        this.graphProperties = graphProperties;
        this.duckPgqProperties = duckPgqProperties;
        this.graphMetrics = graphMetrics;
    }

    public GraphExpandResponse expand(GraphExpandRequest request) {
        long startedAt = System.nanoTime();
        GraphSource source = GraphSource.SQL_FALLBACK;
        Timer.Sample sample = graphMetrics.startTimer();

        try {
            int maxNeighborsPerSeed = orDefault(request.maxNeighborsPerSeed(), graphProperties.getDefaultMaxNeighborsPerSeed());
            int maxNodes = orDefault(request.maxNodes(), graphProperties.getDefaultMaxNodes());
            int maxEdges = orDefault(request.maxEdges(), graphProperties.getDefaultMaxEdges());
            boolean includeAttributes = request.includeAttributes() == null || request.includeAttributes();

            LinkedHashSet<String> seedNodeIds = graphRepository.resolveNodeIds(request.seeds());
            if (seedNodeIds.isEmpty()) {
                throw new ApiNotFoundException("No seed nodes were resolved from provided identifiers");
            }

            if (seedNodeIds.size() > maxNodes) {
                throw new ApiBadRequestException("maxNodes is lower than number of resolved seed nodes");
            }

            EdgeFetchResult edgeFetchResult = fetchCandidateEdges(seedNodeIds, request.direction(), request.edgeTypes());
            source = edgeFetchResult.source();

            List<EdgeRow> perSeedLimited = applyPerSeedLimit(
                edgeFetchResult.edges(),
                seedNodeIds,
                request.direction(),
                maxNeighborsPerSeed
            );

            LimitSelection selection = applyGlobalLimits(perSeedLimited, seedNodeIds, maxNodes, maxEdges);
            List<NodeRow> nodeRows = graphRepository.findNodesByIds(selection.nodeIds());

            List<GraphNodeDto> nodes = nodeRows.stream()
                .map(row -> graphDtoMapper.toNodeDto(row, includeAttributes))
                .toList();

            List<GraphEdgeDto> edges = selection.edges().stream()
                .map(row -> graphDtoMapper.toEdgeDto(row, includeAttributes))
                .toList();

            GraphMetaDto freshMeta = new GraphMetaDto(
                selection.truncated(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
                source
            );

            GraphExpandResponse response = new GraphExpandResponse(nodes, edges, freshMeta);
            if (request.existingGraph() != null) {
                response = mergeWithExisting(request.existingGraph(), response);
            }

            graphMetrics.recordNodeCount(response.nodes().size());
            return response;
        } finally {
            graphMetrics.stopTimer(sample, "expand", source);
        }
    }

    private GraphExpandResponse mergeWithExisting(GraphEnvelopeDto existingGraph, GraphExpandResponse freshGraph) {
        List<GraphNodeDto> mergedNodes = mergeNodes(existingGraph.nodes(), freshGraph.nodes());
        List<GraphEdgeDto> mergedEdges = mergeEdges(existingGraph.edges(), freshGraph.edges());

        boolean previouslyTruncated = existingGraph.meta() != null && existingGraph.meta().truncated();
        GraphMetaDto mergedMeta = new GraphMetaDto(
            freshGraph.meta().truncated() || previouslyTruncated,
            freshGraph.meta().executionMs(),
            freshGraph.meta().source()
        );

        return new GraphExpandResponse(mergedNodes, mergedEdges, mergedMeta);
    }

    private List<GraphNodeDto> mergeNodes(List<GraphNodeDto> existingNodes, List<GraphNodeDto> freshNodes) {
        Map<String, GraphNodeDto> merged = new LinkedHashMap<>();
        for (GraphNodeDto node : safeNodes(existingNodes)) {
            if (node != null && hasText(node.nodeId())) {
                merged.put(node.nodeId(), node);
            }
        }

        for (GraphNodeDto node : safeNodes(freshNodes)) {
            if (node == null || !hasText(node.nodeId())) {
                continue;
            }
            GraphNodeDto existing = merged.get(node.nodeId());
            if (existing == null) {
                merged.put(node.nodeId(), node);
                continue;
            }
            merged.put(node.nodeId(), mergeNode(existing, node));
        }

        return new ArrayList<>(merged.values());
    }

    private GraphNodeDto mergeNode(GraphNodeDto existingNode, GraphNodeDto freshNode) {
        String displayName = hasText(freshNode.displayName()) ? freshNode.displayName() : existingNode.displayName();
        Map<String, String> identifiers = mergeStringMaps(existingNode.identifiers(), freshNode.identifiers());
        Set<String> statuses = mergeStatuses(existingNode.statuses(), freshNode.statuses());
        Map<String, Object> attributes = mergeAttributes(
            existingNode.attributes(),
            freshNode.attributes(),
            PRESERVED_NODE_ATTRIBUTE_KEYS
        );

        return new GraphNodeDto(freshNode.nodeId(), displayName, identifiers, statuses, attributes);
    }

    private List<GraphEdgeDto> mergeEdges(List<GraphEdgeDto> existingEdges, List<GraphEdgeDto> freshEdges) {
        Map<String, GraphEdgeDto> merged = new LinkedHashMap<>();
        for (GraphEdgeDto edge : safeEdges(existingEdges)) {
            if (edge != null && hasText(edge.edgeId())) {
                merged.put(edge.edgeId(), edge);
            }
        }

        for (GraphEdgeDto edge : safeEdges(freshEdges)) {
            if (edge == null || !hasText(edge.edgeId())) {
                continue;
            }
            GraphEdgeDto existing = merged.get(edge.edgeId());
            if (existing == null) {
                merged.put(edge.edgeId(), edge);
                continue;
            }
            merged.put(edge.edgeId(), mergeEdge(existing, edge));
        }

        return new ArrayList<>(merged.values());
    }

    private GraphEdgeDto mergeEdge(GraphEdgeDto existingEdge, GraphEdgeDto freshEdge) {
        String fromNodeId = hasText(freshEdge.fromNodeId()) ? freshEdge.fromNodeId() : existingEdge.fromNodeId();
        String toNodeId = hasText(freshEdge.toNodeId()) ? freshEdge.toNodeId() : existingEdge.toNodeId();
        String type = hasText(freshEdge.type()) ? freshEdge.type() : existingEdge.type();
        Double weight = freshEdge.weight() != null ? freshEdge.weight() : existingEdge.weight();
        Map<String, Object> attributes = mergeAttributes(
            existingEdge.attributes(),
            freshEdge.attributes(),
            PRESERVED_EDGE_ATTRIBUTE_KEYS
        );

        return new GraphEdgeDto(
            freshEdge.edgeId(),
            fromNodeId,
            toNodeId,
            type,
            freshEdge.directed(),
            weight,
            attributes
        );
    }

    private List<GraphNodeDto> safeNodes(List<GraphNodeDto> nodes) {
        return nodes == null ? List.of() : nodes;
    }

    private List<GraphEdgeDto> safeEdges(List<GraphEdgeDto> edges) {
        return edges == null ? List.of() : edges;
    }

    private Map<String, String> mergeStringMaps(Map<String, String> existingValues, Map<String, String> freshValues) {
        if ((existingValues == null || existingValues.isEmpty()) && (freshValues == null || freshValues.isEmpty())) {
            return Map.of();
        }

        Map<String, String> merged = new LinkedHashMap<>();
        if (existingValues != null) {
            existingValues.forEach((key, value) -> {
                if (hasText(key) && hasText(value)) {
                    merged.put(key, value);
                }
            });
        }
        if (freshValues != null) {
            freshValues.forEach((key, value) -> {
                if (hasText(key) && hasText(value)) {
                    merged.put(key, value);
                }
            });
        }
        return merged;
    }

    private Set<String> mergeStatuses(Set<String> existingStatuses, Set<String> freshStatuses) {
        if ((existingStatuses == null || existingStatuses.isEmpty()) && (freshStatuses == null || freshStatuses.isEmpty())) {
            return Set.of();
        }

        Set<String> merged = new LinkedHashSet<>();
        if (existingStatuses != null) {
            existingStatuses.stream().filter(this::hasText).forEach(merged::add);
        }
        if (freshStatuses != null) {
            freshStatuses.stream().filter(this::hasText).forEach(merged::add);
        }
        return merged;
    }

    private Map<String, Object> mergeAttributes(Map<String, Object> existingValues,
                                                Map<String, Object> freshValues,
                                                Set<String> preservedKeys) {
        if ((existingValues == null || existingValues.isEmpty()) && (freshValues == null || freshValues.isEmpty())) {
            return Map.of();
        }

        Map<String, Object> merged = new LinkedHashMap<>();
        if (existingValues != null) {
            merged.putAll(existingValues);
        }
        if (freshValues != null) {
            merged.putAll(freshValues);
        }
        if (existingValues != null) {
            for (String key : preservedKeys) {
                if (existingValues.containsKey(key)) {
                    merged.put(key, existingValues.get(key));
                }
            }
        }
        return merged;
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private EdgeFetchResult fetchCandidateEdges(Set<String> seedNodeIds,
                                                Direction direction,
                                                List<String> edgeTypes) {
        if (canTryDuckPgq()) {
            try {
                // Prefer native graph pattern matching when duckpgq is present.
                List<EdgeRow> edges = duckPgqGraphQueryRepository.findExpandEdges(seedNodeIds, direction, edgeTypes);
                return new EdgeFetchResult(edges, GraphSource.DUCKPGQ);
            } catch (Exception ex) {
                if (strictDuckPgqMode()) {
                    throw new ApiServiceUnavailableException("duckpgq query failed and SQL fallback is disabled");
                }
                // Hard fallback keeps API operational even if extension query fails at runtime.
                log.warn("duckpgq expand query failed, switching to SQL fallback: {}", ex.getMessage());
            }
        } else if (strictDuckPgqMode()) {
            throw new ApiServiceUnavailableException("duckpgq is required but unavailable");
        }

        return new EdgeFetchResult(
            sqlGraphQueryRepository.findExpandEdges(seedNodeIds, direction, edgeTypes),
            GraphSource.SQL_FALLBACK
        );
    }

    private boolean canTryDuckPgq() {
        return duckPgqProperties.isEnabled()
            && !duckPgqProperties.isForceFallback()
            && duckPgqGraphQueryRepository.isDuckPgqLoaded();
    }

    private boolean strictDuckPgqMode() {
        return duckPgqProperties.isEnabled()
            && !duckPgqProperties.isForceFallback()
            && duckPgqProperties.isFailOnUnavailable();
    }

    private List<EdgeRow> applyPerSeedLimit(List<EdgeRow> candidateEdges,
                                                            Set<String> seedNodeIds,
                                                            Direction direction,
                                                            int maxNeighborsPerSeed) {
        Map<String, Integer> neighborsPerSeed = new HashMap<>();
        Set<String> acceptedEdgeIds = new LinkedHashSet<>();
        List<EdgeRow> accepted = new ArrayList<>();

        for (EdgeRow edge : candidateEdges) {
            List<String> relatedSeeds = relatedSeeds(edge, seedNodeIds, direction);
            if (relatedSeeds.isEmpty()) {
                continue;
            }

            boolean canInclude = relatedSeeds.stream()
                .anyMatch(seed -> neighborsPerSeed.getOrDefault(seed, 0) < maxNeighborsPerSeed);

            if (!canInclude) {
                continue;
            }

            if (!acceptedEdgeIds.add(edge.edgeId())) {
                continue;
            }

            accepted.add(edge);
            for (String seed : relatedSeeds) {
                int current = neighborsPerSeed.getOrDefault(seed, 0);
                if (current < maxNeighborsPerSeed) {
                    neighborsPerSeed.put(seed, current + 1);
                }
            }
        }

        return accepted;
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

    private int orDefault(Integer value, int fallback) {
        return value == null ? fallback : value;
    }

    private record LimitSelection(
        LinkedHashSet<String> nodeIds,
        List<EdgeRow> edges,
        boolean truncated
    ) {
    }

    private record EdgeFetchResult(
        List<EdgeRow> edges,
        GraphSource source
    ) {
    }
}
