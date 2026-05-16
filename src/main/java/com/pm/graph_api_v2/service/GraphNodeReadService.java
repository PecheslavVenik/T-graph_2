package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphExpandPreviewDto;
import com.pm.graph_api_v2.dto.GraphFacetCountDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.dto.GraphNodeSearchMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeSearchResponse;
import com.pm.graph_api_v2.dto.GraphNodeSummaryDto;
import com.pm.graph_api_v2.dto.GraphNodeSummaryResponse;
import com.pm.graph_api_v2.exception.ApiBadRequestException;
import com.pm.graph_api_v2.exception.ApiNotFoundException;
import com.pm.graph_api_v2.metrics.GraphMetrics;
import com.pm.graph_api_v2.repository.GraphNeighborhoodSupport;
import com.pm.graph_api_v2.repository.GraphNodeRepository;
import com.pm.graph_api_v2.repository.model.FacetCountRow;
import com.pm.graph_api_v2.repository.model.NodeNeighborhoodSummaryRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;

@Service
public class GraphNodeReadService {

    private static final int DEFAULT_NODE_SEARCH_LIMIT = 20;
    private static final int MAX_NODE_SEARCH_LIMIT = 100;

    private final GraphNodeRepository nodeRepository;
    private final GraphNeighborhoodSupport graphNeighborhoodSupport;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphRequestNormalizer requestNormalizer;
    private final GraphProperties graphProperties;
    private final GraphMetrics graphMetrics;

    public GraphNodeReadService(GraphNodeRepository nodeRepository,
                                GraphNeighborhoodSupport graphNeighborhoodSupport,
                                GraphDtoMapper graphDtoMapper,
                                GraphRequestNormalizer requestNormalizer,
                                GraphProperties graphProperties,
                                GraphMetrics graphMetrics) {
        this.nodeRepository = nodeRepository;
        this.graphNeighborhoodSupport = graphNeighborhoodSupport;
        this.graphDtoMapper = graphDtoMapper;
        this.requestNormalizer = requestNormalizer;
        this.graphProperties = graphProperties;
        this.graphMetrics = graphMetrics;
    }

    public GraphNodeSummaryResponse nodeSummary(String nodeId,
                                                String relationFamily,
                                                Direction direction) {
        String resolvedRelationFamily = requestNormalizer.relationFamilyOrAll(relationFamily);

        NodeRow nodeRow = nodeRepository.findNodeById(nodeId)
            .orElseThrow(() -> new ApiNotFoundException("Node was not found"));
        NodeNeighborhoodSummaryRow summaryRow = graphNeighborhoodSupport.summarizeNeighborhood(nodeId, resolvedRelationFamily, direction);

        return new GraphNodeSummaryResponse(
            graphDtoMapper.toNodeDto(nodeRow, true),
            new GraphNodeSummaryDto(
                direction,
                resolvedRelationFamily,
                summaryRow.adjacentEdgeCount(),
                summaryRow.uniqueNeighborCount(),
                summaryRow.outboundEdgeCount(),
                summaryRow.inboundEdgeCount()
            ),
            toFacetDtos(graphNeighborhoodSupport.countRelationFamiliesAroundNode(nodeId, resolvedRelationFamily, direction)),
            toFacetDtos(graphNeighborhoodSupport.countEdgeTypesAroundNode(nodeId, resolvedRelationFamily, direction)),
            toFacetDtos(graphNeighborhoodSupport.countNeighborNodeTypesAroundNode(nodeId, resolvedRelationFamily, direction)),
            new GraphExpandPreviewDto(
                graphProperties.getDefaultMaxNeighborsPerSeed(),
                graphProperties.getDefaultMaxNodes(),
                graphProperties.getDefaultMaxEdges(),
                summaryRow.adjacentEdgeCount() > graphProperties.getDefaultMaxNeighborsPerSeed()
            )
        );
    }

    public GraphNodeSearchResponse searchNodes(String query,
                                               String nodeType,
                                               int limit,
                                               boolean includeAttributes) {
        String normalizedQuery = normalizeSearchQuery(query);
        String normalizedNodeType = normalizeNodeType(nodeType);
        int effectiveLimit = normalizeSearchLimit(limit);

        var sample = graphMetrics.startTimer();
        try {
            List<NodeRow> rows = nodeRepository.searchNodes(normalizedQuery, normalizedNodeType, effectiveLimit + 1);
            boolean truncated = rows.size() > effectiveLimit;
            List<GraphNodeDto> nodes = rows.stream()
                .limit(effectiveLimit)
                .map(row -> graphDtoMapper.toNodeDto(row, includeAttributes))
                .toList();

            graphMetrics.recordNodeCount(nodes.size());

            return new GraphNodeSearchResponse(
                nodes,
                new GraphNodeSearchMetaDto(
                    normalizedQuery,
                    normalizedNodeType,
                    effectiveLimit,
                    nodes.size(),
                    truncated
                )
            );
        } finally {
            graphMetrics.stopTimer(sample, "node_search");
        }
    }

    private List<GraphFacetCountDto> toFacetDtos(List<FacetCountRow> rows) {
        return rows.stream()
            .map(row -> new GraphFacetCountDto(row.key(), row.count()))
            .toList();
    }

    private String normalizeSearchQuery(String query) {
        if (query == null || query.trim().isBlank()) {
            throw new ApiBadRequestException("query must not be blank");
        }
        String normalized = query.trim();
        if (normalized.length() > 256) {
            throw new ApiBadRequestException("query must be at most 256 characters");
        }
        return normalized;
    }

    private String normalizeNodeType(String nodeType) {
        if (nodeType == null || nodeType.trim().isBlank()) {
            return null;
        }
        return nodeType.trim().toUpperCase(Locale.ROOT);
    }

    private int normalizeSearchLimit(Integer limit) {
        int effectiveLimit = limit == null ? DEFAULT_NODE_SEARCH_LIMIT : limit;
        if (effectiveLimit < 1 || effectiveLimit > MAX_NODE_SEARCH_LIMIT) {
            throw new ApiBadRequestException("limit must be between 1 and " + MAX_NODE_SEARCH_LIMIT);
        }
        return effectiveLimit;
    }
}
