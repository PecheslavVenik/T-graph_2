package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphDictionaryResponse;
import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphExpandRequest;
import com.pm.graph_api_v2.dto.GraphExpandResponse;
import com.pm.graph_api_v2.dto.GraphExpandPreviewDto;
import com.pm.graph_api_v2.dto.GraphFacetCountDto;
import com.pm.graph_api_v2.dto.GraphExportFormat;
import com.pm.graph_api_v2.dto.GraphExportRequest;
import com.pm.graph_api_v2.dto.GraphMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.dto.GraphNodeSummaryDto;
import com.pm.graph_api_v2.dto.GraphNodeSummaryResponse;
import com.pm.graph_api_v2.dto.PathDto;
import com.pm.graph_api_v2.dto.ShortestPathRequest;
import com.pm.graph_api_v2.dto.ShortestPathResponse;
import com.pm.graph_api_v2.exception.ApiBadRequestException;
import com.pm.graph_api_v2.exception.ApiNotFoundException;
import com.pm.graph_api_v2.metrics.GraphMetrics;
import com.pm.graph_api_v2.repository.GraphRepository;
import com.pm.graph_api_v2.repository.GraphQueryBackend;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.FacetCountRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.NodeNeighborhoodSummaryRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class InvestigationService {

    private final GraphRepository graphRepository;
    private final GraphQueryBackend graphQueryBackend;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphDictionaryFactory graphDictionaryFactory;
    private final GraphExpandPlanner graphExpandPlanner;
    private final GraphExportService graphExportService;
    private final GraphProperties graphProperties;
    private final GraphMetrics graphMetrics;

    public InvestigationService(GraphRepository graphRepository,
                                GraphQueryBackend graphQueryBackend,
                                GraphDtoMapper graphDtoMapper,
                                GraphDictionaryFactory graphDictionaryFactory,
                                GraphExpandPlanner graphExpandPlanner,
                                GraphExportService graphExportService,
                                GraphProperties graphProperties,
                                GraphMetrics graphMetrics) {
        this.graphRepository = graphRepository;
        this.graphQueryBackend = graphQueryBackend;
        this.graphDtoMapper = graphDtoMapper;
        this.graphDictionaryFactory = graphDictionaryFactory;
        this.graphExpandPlanner = graphExpandPlanner;
        this.graphExportService = graphExportService;
        this.graphProperties = graphProperties;
        this.graphMetrics = graphMetrics;
    }

    public GraphExpandResponse expand(GraphExpandRequest request) {
        long startedAt = System.nanoTime();
        Timer.Sample sample = graphMetrics.startTimer();

        try {
            String relationFamily = resolveRelationFamily(request.relationFamily());
            List<String> edgeTypes = normalizeEdgeTypes(request.edgeTypes());
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

            List<EdgeRow> candidateEdges = graphQueryBackend.findExpandEdges(
                seedNodeIds,
                relationFamily,
                edgeTypes,
                request.direction(),
                graphProperties.getMaxExpandCandidateEdges()
            );
            graphMetrics.recordCandidateEdgeCount(candidateEdges.size());

            GraphExpandPlanner.ExpandPlan expandPlan = graphExpandPlanner.plan(
                seedNodeIds,
                candidateEdges,
                request.direction(),
                maxNeighborsPerSeed,
                maxNodes,
                maxEdges
            );

            List<GraphNodeDto> nodes = expandPlan.nodeIds().stream()
                .map(expandPlan.nodesById()::get)
                .filter(row -> row != null)
                .map(row -> graphDtoMapper.toNodeDto(row, includeAttributes))
                .toList();

            List<GraphEdgeDto> edges = expandPlan.edges().stream()
                .map(row -> graphExpandPlanner.toEdgeDto(row, includeAttributes, seedNodeIds, request.direction(), expandPlan.nodesById()))
                .toList();

            List<String> warnings = buildWarnings(candidateEdges.size() >= graphProperties.getMaxExpandCandidateEdges(),
                expandPlan.perSeedTruncated(),
                expandPlan.globalTruncated());

            GraphMetaDto meta = new GraphMetaDto(
                expandPlan.perSeedTruncated() || expandPlan.globalTruncated(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
                graphQueryBackend.source(),
                relationFamily,
                GraphExpandPlanner.RANKING_STRATEGY,
                candidateEdges.size(),
                nodes.size(),
                edges.size(),
                warnings
            );

            graphMetrics.recordNodeCount(nodes.size());
            graphMetrics.recordEdgeCount(edges.size());
            if (meta.truncated()) {
                graphMetrics.recordTruncation("expand");
            }

            return new GraphExpandResponse(nodes, edges, meta);
        } finally {
            graphMetrics.stopTimer(sample, "expand");
        }
    }

    public ShortestPathResponse shortestPath(ShortestPathRequest request) {
        long startedAt = System.nanoTime();
        Timer.Sample sample = graphMetrics.startTimer();

        try {
            String relationFamily = resolveRelationFamily(request.relationFamily());
            String sourceNodeId = graphRepository.resolveNodeId(request.source())
                .orElseThrow(() -> new ApiNotFoundException("Source node was not found"));
            String targetNodeId = graphRepository.resolveNodeId(request.target())
                .orElseThrow(() -> new ApiNotFoundException("Target node was not found"));
            int maxDepth = request.maxDepth() == null ? graphProperties.getDefaultMaxDepth() : request.maxDepth();

            PathRow pathRow = graphQueryBackend.findShortestPath(sourceNodeId, targetNodeId, relationFamily, request.direction(), maxDepth)
                .orElseThrow(() -> new ApiNotFoundException("No path between source and target in current graph"));

            List<NodeRow> nodeRows = graphRepository.findNodesByIdsInOrder(pathRow.nodeIds());
            List<EdgeRow> edgeRows = graphRepository.findEdgesByIdsInOrder(pathRow.edgeIds());

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

    public GraphDictionaryResponse dictionary() {
        return graphDictionaryFactory.create(
            graphRepository.findDistinctEdgeTypes(),
            graphRepository.findDistinctRelationFamilies(),
            graphRepository.findDistinctNodeTypes(),
            graphRepository.findPresentNodeStatuses()
        );
    }

    public GraphNodeSummaryResponse nodeSummary(String nodeId,
                                                String relationFamily,
                                                Direction direction) {
        String normalizedRelationFamily = GraphRelationFamilies.normalize(relationFamily);
        String resolvedRelationFamily = normalizedRelationFamily == null
            ? GraphRelationFamilies.ALL_RELATIONS
            : normalizedRelationFamily;

        NodeRow nodeRow = graphRepository.findNodeById(nodeId)
            .orElseThrow(() -> new ApiNotFoundException("Node was not found"));
        NodeNeighborhoodSummaryRow summaryRow = graphRepository.summarizeNeighborhood(nodeId, resolvedRelationFamily, direction);

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
            toFacetDtos(graphRepository.countRelationFamiliesAroundNode(nodeId, resolvedRelationFamily, direction)),
            toFacetDtos(graphRepository.countEdgeTypesAroundNode(nodeId, resolvedRelationFamily, direction)),
            toFacetDtos(graphRepository.countNeighborNodeTypesAroundNode(nodeId, resolvedRelationFamily, direction)),
            new GraphExpandPreviewDto(
                graphProperties.getDefaultMaxNeighborsPerSeed(),
                graphProperties.getDefaultMaxNodes(),
                graphProperties.getDefaultMaxEdges(),
                summaryRow.adjacentEdgeCount() > graphProperties.getDefaultMaxNeighborsPerSeed()
            )
        );
    }

    public ExportedGraph export(GraphExportRequest request, GraphExportFormat format) {
        return graphExportService.export(request, format);
    }

    private List<String> buildWarnings(boolean candidateBudgetHit, boolean perSeedLimitApplied, boolean globalLimitApplied) {
        List<String> warnings = new ArrayList<>();
        if (candidateBudgetHit) {
            graphMetrics.recordGuardrailHit("expand", "candidate_edges");
            warnings.add("Candidate edge budget was applied before ranking");
        }
        if (perSeedLimitApplied) {
            graphMetrics.recordGuardrailHit("expand", "neighbors_per_seed");
            warnings.add("Per-seed neighbor budget filtered lower-ranked neighbors");
        }
        if (globalLimitApplied) {
            graphMetrics.recordGuardrailHit("expand", "graph_result_size");
            warnings.add("Global node/edge limits truncated the graph");
        }
        return warnings;
    }

    private String resolveRelationFamily(String relationFamily) {
        String normalized = GraphRelationFamilies.normalize(relationFamily);
        return normalized == null ? graphProperties.getDefaultRelationFamily() : normalized;
    }

    private List<String> normalizeEdgeTypes(List<String> edgeTypes) {
        if (edgeTypes == null || edgeTypes.isEmpty()) {
            return List.of();
        }

        return edgeTypes.stream()
            .filter(value -> value != null && !value.isBlank())
            .map(value -> value.trim().toUpperCase(Locale.ROOT))
            .distinct()
            .toList();
    }

    private List<GraphFacetCountDto> toFacetDtos(List<FacetCountRow> rows) {
        return rows.stream()
            .map(row -> new GraphFacetCountDto(row.key(), row.count()))
            .toList();
    }

    private int orDefault(Integer value, int fallback) {
        return value == null ? fallback : value;
    }
}
