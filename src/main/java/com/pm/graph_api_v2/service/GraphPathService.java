package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.dto.PathDto;
import com.pm.graph_api_v2.dto.ShortestPathRequest;
import com.pm.graph_api_v2.dto.ShortestPathResponse;
import com.pm.graph_api_v2.exception.ApiNotFoundException;
import com.pm.graph_api_v2.metrics.GraphMetrics;
import com.pm.graph_api_v2.repository.GraphEdgeRepository;
import com.pm.graph_api_v2.repository.GraphNodeRepository;
import com.pm.graph_api_v2.repository.GraphQueryBackend;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class GraphPathService {

    private final GraphNodeRepository nodeRepository;
    private final GraphEdgeRepository edgeRepository;
    private final GraphQueryBackend graphQueryBackend;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphRequestNormalizer requestNormalizer;
    private final GraphProperties graphProperties;
    private final GraphMetrics graphMetrics;

    public GraphPathService(GraphNodeRepository nodeRepository,
                            GraphEdgeRepository edgeRepository,
                            GraphQueryBackend graphQueryBackend,
                            GraphDtoMapper graphDtoMapper,
                            GraphRequestNormalizer requestNormalizer,
                            GraphProperties graphProperties,
                            GraphMetrics graphMetrics) {
        this.nodeRepository = nodeRepository;
        this.edgeRepository = edgeRepository;
        this.graphQueryBackend = graphQueryBackend;
        this.graphDtoMapper = graphDtoMapper;
        this.requestNormalizer = requestNormalizer;
        this.graphProperties = graphProperties;
        this.graphMetrics = graphMetrics;
    }

    public ShortestPathResponse shortestPath(ShortestPathRequest request) {
        long startedAt = System.nanoTime();
        var sample = graphMetrics.startTimer();

        try {
            String relationFamily = requestNormalizer.relationFamilyOrDefault(request.relationFamily());
            String sourceNodeId = nodeRepository.resolveNodeId(request.source())
                .orElseThrow(() -> new ApiNotFoundException("Source node was not found"));
            String targetNodeId = nodeRepository.resolveNodeId(request.target())
                .orElseThrow(() -> new ApiNotFoundException("Target node was not found"));
            int maxDepth = request.maxDepth() == null ? graphProperties.getDefaultMaxDepth() : request.maxDepth();

            PathRow pathRow = graphQueryBackend.findShortestPath(sourceNodeId, targetNodeId, relationFamily, request.direction(), maxDepth)
                .orElseThrow(() -> new ApiNotFoundException("No path between source and target in current graph"));

            List<NodeRow> nodeRows = nodeRepository.findNodesByIdsInOrder(pathRow.nodeIds());
            List<EdgeRow> edgeRows = edgeRepository.findEdgesByIdsInOrder(pathRow.edgeIds());

            List<GraphNodeDto> nodes = nodeRows.stream()
                .map(row -> graphDtoMapper.toNodeDto(row, true))
                .toList();
            List<GraphEdgeDto> edges = edgeRows.stream()
                .map(row -> graphDtoMapper.toEdgeDto(row, true))
                .toList();

            graphMetrics.recordNodeCount(nodes.size());
            graphMetrics.recordEdgeCount(edges.size());

            GraphMetaDto meta = new GraphMetaDto(
                false,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
                graphQueryBackend.source(),
                relationFamily,
                GraphExpandPlanner.RANKING_STRATEGY,
                pathRow.edgeIds().size(),
                nodes.size(),
                edges.size(),
                List.of()
            );

            return new ShortestPathResponse(
                new PathDto(pathRow.nodeIds(), pathRow.edgeIds(), pathRow.edgeIds().size()),
                nodes,
                edges,
                meta
            );
        } finally {
            graphMetrics.stopTimer(sample, "shortest_path");
        }
    }
}
