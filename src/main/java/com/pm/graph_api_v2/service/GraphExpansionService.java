package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphExpandRequest;
import com.pm.graph_api_v2.dto.GraphExpandResponse;
import com.pm.graph_api_v2.dto.GraphMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.exception.ApiBadRequestException;
import com.pm.graph_api_v2.exception.ApiNotFoundException;
import com.pm.graph_api_v2.metrics.GraphMetrics;
import com.pm.graph_api_v2.repository.GraphNodeRepository;
import com.pm.graph_api_v2.repository.GraphQueryBackend;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class GraphExpansionService {

    private final GraphNodeRepository nodeRepository;
    private final GraphQueryBackend graphQueryBackend;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphExpandPlanner graphExpandPlanner;
    private final GraphRequestNormalizer requestNormalizer;
    private final GraphProperties graphProperties;
    private final GraphMetrics graphMetrics;

    public GraphExpansionService(GraphNodeRepository nodeRepository,
                                 GraphQueryBackend graphQueryBackend,
                                 GraphDtoMapper graphDtoMapper,
                                 GraphExpandPlanner graphExpandPlanner,
                                 GraphRequestNormalizer requestNormalizer,
                                 GraphProperties graphProperties,
                                 GraphMetrics graphMetrics) {
        this.nodeRepository = nodeRepository;
        this.graphQueryBackend = graphQueryBackend;
        this.graphDtoMapper = graphDtoMapper;
        this.graphExpandPlanner = graphExpandPlanner;
        this.requestNormalizer = requestNormalizer;
        this.graphProperties = graphProperties;
        this.graphMetrics = graphMetrics;
    }

    public GraphExpandResponse expand(GraphExpandRequest request) {
        long startedAt = System.nanoTime();
        var sample = graphMetrics.startTimer();

        try {
            String relationFamily = requestNormalizer.relationFamilyOrDefault(request.relationFamily());
            List<String> edgeTypes = requestNormalizer.edgeTypes(request.edgeTypes());
            int maxNeighborsPerSeed = requestNormalizer.orDefault(request.maxNeighborsPerSeed(), graphProperties.getDefaultMaxNeighborsPerSeed());
            int maxNodes = requestNormalizer.orDefault(request.maxNodes(), graphProperties.getDefaultMaxNodes());
            int maxEdges = requestNormalizer.orDefault(request.maxEdges(), graphProperties.getDefaultMaxEdges());
            boolean includeAttributes = requestNormalizer.includeAttributes(request.includeAttributes());

            LinkedHashSet<String> seedNodeIds = nodeRepository.resolveNodeIds(request.seeds());
            if (seedNodeIds.isEmpty()) {
                throw new ApiNotFoundException("No seed nodes were resolved from provided identifiers");
            }

            return expandResolvedNodes(
                seedNodeIds,
                relationFamily,
                edgeTypes,
                request.direction(),
                maxNeighborsPerSeed,
                maxNodes,
                maxEdges,
                includeAttributes,
                startedAt,
                GraphExpandPlanner.RANKING_STRATEGY,
                List.of()
            );
        } finally {
            graphMetrics.stopTimer(sample, "expand");
        }
    }

    GraphExpandResponse expandResolvedNodes(LinkedHashSet<String> seedNodeIds,
                                            String relationFamily,
                                            List<String> edgeTypes,
                                            Direction direction,
                                            int maxNeighborsPerSeed,
                                            int maxNodes,
                                            int maxEdges,
                                            boolean includeAttributes,
                                            long startedAt,
                                            String rankingStrategy,
                                            List<String> initialWarnings) {
        if (seedNodeIds.size() > maxNodes) {
            throw new ApiBadRequestException("maxNodes is lower than number of resolved seed nodes");
        }

        List<EdgeRow> candidateEdges = graphQueryBackend.findExpandEdges(
            seedNodeIds,
            relationFamily,
            edgeTypes,
            direction,
            graphProperties.getMaxExpandCandidateEdges()
        );
        graphMetrics.recordCandidateEdgeCount(candidateEdges.size());

        GraphExpandPlanner.ExpandPlan expandPlan = graphExpandPlanner.plan(
            seedNodeIds,
            candidateEdges,
            direction,
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
            .map(row -> graphExpandPlanner.toEdgeDto(row, includeAttributes, seedNodeIds, direction, expandPlan.nodesById()))
            .toList();

        List<String> warnings = new ArrayList<>(initialWarnings);
        warnings.addAll(buildWarnings(candidateEdges.size() >= graphProperties.getMaxExpandCandidateEdges(),
            expandPlan.perSeedTruncated(),
            expandPlan.globalTruncated()));

        GraphMetaDto meta = new GraphMetaDto(
            expandPlan.perSeedTruncated() || expandPlan.globalTruncated() || !initialWarnings.isEmpty(),
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
            graphQueryBackend.source(),
            relationFamily,
            rankingStrategy,
            candidateEdges.size(),
            nodes.size(),
            edges.size(),
            warnings
        );

        graphMetrics.recordNodeCount(nodes.size());
        graphMetrics.recordEdgeCount(edges.size());
        if (meta.truncated()) {
            graphMetrics.recordTruncation(rankingStrategy);
        }

        return new GraphExpandResponse(nodes, edges, meta);
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
}
