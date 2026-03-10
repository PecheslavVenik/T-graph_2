package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.DuckPgqProperties;
import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.dto.PathDto;
import com.pm.graph_api_v2.dto.ShortestPathRequest;
import com.pm.graph_api_v2.dto.ShortestPathResponse;
import com.pm.graph_api_v2.exception.ApiNotFoundException;
import com.pm.graph_api_v2.exception.ApiServiceUnavailableException;
import com.pm.graph_api_v2.metrics.GraphMetrics;
import com.pm.graph_api_v2.repository.DuckPgqGraphQueryRepository;
import com.pm.graph_api_v2.repository.GraphRepository;
import com.pm.graph_api_v2.repository.SqlGraphQueryRepository;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class GraphPathService {

    private static final Logger log = LoggerFactory.getLogger(GraphPathService.class);

    private final GraphRepository graphRepository;
    private final DuckPgqGraphQueryRepository duckPgqGraphQueryRepository;
    private final SqlGraphQueryRepository sqlGraphQueryRepository;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphProperties graphProperties;
    private final DuckPgqProperties duckPgqProperties;
    private final GraphMetrics graphMetrics;

    public GraphPathService(GraphRepository graphRepository,
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

    public ShortestPathResponse shortestPath(ShortestPathRequest request) {
        long startedAt = System.nanoTime();
        GraphSource source = GraphSource.SQL_FALLBACK;
        Timer.Sample sample = graphMetrics.startTimer();

        try {
            String sourceNodeId = graphRepository.resolveNodeId(request.source())
                .orElseThrow(() -> new ApiNotFoundException("Source node was not found"));
            String targetNodeId = graphRepository.resolveNodeId(request.target())
                .orElseThrow(() -> new ApiNotFoundException("Target node was not found"));

            int maxDepth = request.maxDepth() == null ? graphProperties.getDefaultMaxDepth() : request.maxDepth();

            PathSearchResult pathSearchResult;
            if (canTryDuckPgq(request.edgeTypes())) {
                try {
                    pathSearchResult = duckPgqGraphQueryRepository.findShortestPath(sourceNodeId, targetNodeId, request.direction(), maxDepth)
                        .map(this::toPathSearchResult)
                        .orElseGet(() -> new PathSearchResult(false, List.of(), List.of()));
                    source = GraphSource.DUCKPGQ;
                } catch (Exception ex) {
                    if (strictDuckPgqMode()) {
                        throw new ApiServiceUnavailableException("duckpgq query failed and SQL fallback is disabled");
                    }
                    log.warn("duckpgq shortest-path query failed, switching to SQL fallback: {}", ex.getMessage());
                    pathSearchResult = bfsViaSql(sourceNodeId, targetNodeId, request.direction(), request.edgeTypes(), maxDepth);
                    source = GraphSource.SQL_FALLBACK;
                }
            } else {
                if (strictDuckPgqMode()) {
                    throw new ApiServiceUnavailableException("duckpgq is required but unavailable");
                }
                pathSearchResult = bfsViaSql(sourceNodeId, targetNodeId, request.direction(), request.edgeTypes(), maxDepth);
                source = GraphSource.SQL_FALLBACK;
            }

            if (!pathSearchResult.found()) {
                throw new ApiNotFoundException("No path between source and target in current graph");
            }

            List<NodeRow> nodeRows = graphRepository.findNodesByIds(pathSearchResult.nodeIds());
            List<EdgeRow> edgeRows = graphRepository.findEdgesByIds(pathSearchResult.edgeIds());

            List<GraphNodeDto> nodes = nodeRows.stream()
                .map(row -> graphDtoMapper.toNodeDto(row, true))
                .toList();

            List<GraphEdgeDto> edges = edgeRows.stream()
                .map(row -> graphDtoMapper.toEdgeDto(row, true))
                .toList();

            graphMetrics.recordNodeCount(nodes.size());

            GraphMetaDto meta = new GraphMetaDto(
                false,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
                source
            );

            PathDto path = new PathDto(pathSearchResult.nodeIds(), pathSearchResult.edgeIds(), pathSearchResult.edgeIds().size());
            return new ShortestPathResponse(path, nodes, edges, meta);
        } finally {
            graphMetrics.stopTimer(sample, "shortest_path", source);
        }
    }

    private PathSearchResult toPathSearchResult(PathRow pathRow) {
        return new PathSearchResult(true, pathRow.nodeIds(), pathRow.edgeIds());
    }

    private boolean canTryDuckPgq(List<String> edgeTypes) {
        if (!duckPgqProperties.isEnabled() || duckPgqProperties.isForceFallback()) {
            return false;
        }

        if (!duckPgqGraphQueryRepository.isDuckPgqLoaded()) {
            return false;
        }

        // Edge-type filtering for path currently uses SQL fallback to keep behavior deterministic.
        return edgeTypes == null || edgeTypes.isEmpty();
    }

    private boolean strictDuckPgqMode() {
        return duckPgqProperties.isEnabled()
            && !duckPgqProperties.isForceFallback()
            && duckPgqProperties.isFailOnUnavailable();
    }

    private PathSearchResult bfsViaSql(String sourceNodeId,
                                       String targetNodeId,
                                       Direction direction,
                                       List<String> edgeTypes,
                                       int maxDepth) {
        if (sourceNodeId.equals(targetNodeId)) {
            return new PathSearchResult(true, List.of(sourceNodeId), List.of());
        }

        Set<String> visited = new HashSet<>();
        visited.add(sourceNodeId);

        Map<String, String> parentNode = new HashMap<>();
        Map<String, String> parentEdge = new HashMap<>();

        Set<String> frontier = new LinkedHashSet<>();
        frontier.add(sourceNodeId);

        // Bounded frontier-BFS: queries only edges touching current frontier, avoids full edge scan in memory.
        for (int depth = 0; depth < maxDepth && !frontier.isEmpty(); depth++) {
            List<EdgeRow> frontierEdges = sqlGraphQueryRepository.findNeighborEdges(frontier, direction, edgeTypes);
            Map<String, List<Neighbor>> adjacency = buildAdjacency(frontierEdges, direction);

            Set<String> nextFrontier = new LinkedHashSet<>();
            for (String current : frontier) {
                for (Neighbor neighbor : adjacency.getOrDefault(current, List.of())) {
                    if (!visited.add(neighbor.nodeId())) {
                        continue;
                    }

                    parentNode.put(neighbor.nodeId(), current);
                    parentEdge.put(neighbor.nodeId(), neighbor.edgeId());

                    if (neighbor.nodeId().equals(targetNodeId)) {
                        return rebuildPath(sourceNodeId, targetNodeId, parentNode, parentEdge);
                    }

                    nextFrontier.add(neighbor.nodeId());
                }
            }

            frontier = nextFrontier;
        }

        return new PathSearchResult(false, List.of(), List.of());
    }

    private Map<String, List<Neighbor>> buildAdjacency(List<EdgeRow> edges, Direction direction) {
        Map<String, List<Neighbor>> adjacency = new HashMap<>();

        for (EdgeRow edge : edges) {
            switch (direction) {
                case OUTBOUND -> {
                    addNeighbor(adjacency, edge.fromNodeId(), edge.toNodeId(), edge.edgeId());
                    if (!edge.directed()) {
                        addNeighbor(adjacency, edge.toNodeId(), edge.fromNodeId(), edge.edgeId());
                    }
                }
                case INBOUND -> {
                    addNeighbor(adjacency, edge.toNodeId(), edge.fromNodeId(), edge.edgeId());
                    if (!edge.directed()) {
                        addNeighbor(adjacency, edge.fromNodeId(), edge.toNodeId(), edge.edgeId());
                    }
                }
                case BOTH -> {
                    addNeighbor(adjacency, edge.fromNodeId(), edge.toNodeId(), edge.edgeId());
                    addNeighbor(adjacency, edge.toNodeId(), edge.fromNodeId(), edge.edgeId());
                }
            }
        }

        return adjacency;
    }

    private void addNeighbor(Map<String, List<Neighbor>> adjacency,
                             String fromNodeId,
                             String toNodeId,
                             String edgeId) {
        adjacency.computeIfAbsent(fromNodeId, ignored -> new ArrayList<>())
            .add(new Neighbor(toNodeId, edgeId));
    }

    private PathSearchResult rebuildPath(String sourceNodeId,
                                         String targetNodeId,
                                         Map<String, String> parentNode,
                                         Map<String, String> parentEdge) {
        List<String> nodeIds = new ArrayList<>();
        List<String> edgeIds = new ArrayList<>();

        String current = targetNodeId;
        nodeIds.add(current);

        while (!current.equals(sourceNodeId)) {
            String parent = parentNode.get(current);
            String edge = parentEdge.get(current);
            if (parent == null || edge == null) {
                return new PathSearchResult(false, List.of(), List.of());
            }
            edgeIds.add(edge);
            nodeIds.add(parent);
            current = parent;
        }

        Collections.reverse(nodeIds);
        Collections.reverse(edgeIds);

        return new PathSearchResult(true, nodeIds, edgeIds);
    }

    private record Neighbor(String nodeId, String edgeId) {
    }

    private record PathSearchResult(boolean found, List<String> nodeIds, List<String> edgeIds) {
    }
}
