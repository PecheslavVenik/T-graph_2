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
import com.pm.graph_api_v2.dto.GraphRelationFamily;
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
import io.micrometer.core.instrument.Timer;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class InvestigationService {

    private static final String RANKING_STRATEGY = "GENERIC_ONE_HOP_RANKING";

    private final GraphRepository graphRepository;
    private final GraphQueryBackend graphQueryBackend;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphDictionaryFactory graphDictionaryFactory;
    private final GraphProperties graphProperties;
    private final GraphMetrics graphMetrics;
    private final ObjectMapper objectMapper;

    public InvestigationService(GraphRepository graphRepository,
                                GraphQueryBackend graphQueryBackend,
                                GraphDtoMapper graphDtoMapper,
                                GraphDictionaryFactory graphDictionaryFactory,
                                GraphProperties graphProperties,
                                GraphMetrics graphMetrics,
                                ObjectMapper objectMapper) {
        this.graphRepository = graphRepository;
        this.graphQueryBackend = graphQueryBackend;
        this.graphDtoMapper = graphDtoMapper;
        this.graphDictionaryFactory = graphDictionaryFactory;
        this.graphProperties = graphProperties;
        this.graphMetrics = graphMetrics;
        this.objectMapper = objectMapper;
    }

    public GraphExpandResponse expand(GraphExpandRequest request) {
        long startedAt = System.nanoTime();
        Timer.Sample sample = graphMetrics.startTimer();

        try {
            GraphRelationFamily relationFamily = resolveRelationFamily(request.relationFamily());
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
                request.direction(),
                graphProperties.getMaxExpandCandidateEdges()
            );
            graphMetrics.recordCandidateEdgeCount(candidateEdges.size());

            Map<String, NodeRow> rankingNodesById = loadRankingNodes(seedNodeIds, candidateEdges);
            List<EdgeRow> rankedEdges = rankCandidateEdges(candidateEdges, seedNodeIds, request.direction(), rankingNodesById);
            PerSeedLimitResult perSeedLimit = applyPerSeedLimit(rankedEdges, seedNodeIds, request.direction(), maxNeighborsPerSeed);
            LimitSelection selection = applyGlobalLimits(perSeedLimit.edges(), seedNodeIds, maxNodes, maxEdges);

            List<GraphNodeDto> nodes = selection.nodeIds().stream()
                .map(rankingNodesById::get)
                .filter(row -> row != null)
                .map(row -> graphDtoMapper.toNodeDto(row, includeAttributes))
                .toList();

            List<GraphEdgeDto> edges = selection.edges().stream()
                .map(row -> toEdgeDto(row, includeAttributes, seedNodeIds, request.direction(), rankingNodesById))
                .toList();

            List<String> warnings = buildWarnings(candidateEdges.size() >= graphProperties.getMaxExpandCandidateEdges(),
                perSeedLimit.truncated(),
                selection.truncated());

            GraphMetaDto meta = new GraphMetaDto(
                perSeedLimit.truncated() || selection.truncated(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
                graphQueryBackend.source(),
                relationFamily.name(),
                RANKING_STRATEGY,
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
            GraphRelationFamily relationFamily = resolveRelationFamily(request.relationFamily());
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
                relationFamily.name(),
                RANKING_STRATEGY,
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
                                                GraphRelationFamily relationFamily,
                                                Direction direction) {
        GraphRelationFamily resolvedRelationFamily = relationFamily == null
            ? GraphRelationFamily.ALL_RELATIONS
            : relationFamily;

        NodeRow nodeRow = graphRepository.findNodeById(nodeId)
            .orElseThrow(() -> new ApiNotFoundException("Node was not found"));
        NodeNeighborhoodSummaryRow summaryRow = graphRepository.summarizeNeighborhood(nodeId, resolvedRelationFamily, direction);

        return new GraphNodeSummaryResponse(
            graphDtoMapper.toNodeDto(nodeRow, true),
            new GraphNodeSummaryDto(
                direction,
                resolvedRelationFamily.name(),
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
        return switch (format) {
            case JSON -> exportJson(request);
            case CSV -> exportCsv(request);
            case NDJSON -> exportNdjson(request);
        };
    }

    private ExportedGraph exportJson(GraphExportRequest request) {
        try {
            byte[] payload = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(request);
            return new ExportedGraph("graph-export.json", MediaType.APPLICATION_JSON_VALUE, payload);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize graph export as JSON", ex);
        }
    }

    private ExportedGraph exportCsv(GraphExportRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("section,node_id,node_type,display_name,identifiers,statuses,attributes\n");
        for (GraphNodeDto node : request.nodes()) {
            sb.append("node")
                .append(',').append(csv(node.nodeId()))
                .append(',').append(csv(node.nodeType()))
                .append(',').append(csv(node.displayName()))
                .append(',').append(csv(writeJson(node.identifiers())))
                .append(',').append(csv(join(node.statuses() == null ? List.of() : node.statuses().stream().toList())))
                .append(',').append(csv(writeJson(node.attributes())))
                .append('\n');
        }

        sb.append("section,edge_id,from_node_id,to_node_id,type,relation_family,directed,weight,source_system,first_seen_at,last_seen_at,attributes\n");
        for (GraphEdgeDto edge : request.edges()) {
            sb.append("edge")
                .append(',').append(csv(edge.edgeId()))
                .append(',').append(csv(edge.fromNodeId()))
                .append(',').append(csv(edge.toNodeId()))
                .append(',').append(csv(edge.type()))
                .append(',').append(csv(edge.relationFamily()))
                .append(',').append(csv(Boolean.toString(edge.directed())))
                .append(',').append(csv(edge.weight() == null ? null : edge.weight().toString()))
                .append(',').append(csv(edge.sourceSystem()))
                .append(',').append(csv(edge.firstSeenAt() == null ? null : edge.firstSeenAt().toString()))
                .append(',').append(csv(edge.lastSeenAt() == null ? null : edge.lastSeenAt().toString()))
                .append(',').append(csv(writeJson(edge.attributes())))
                .append('\n');
        }

        return new ExportedGraph("graph-export.csv", "text/csv", sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private ExportedGraph exportNdjson(GraphExportRequest request) {
        StringBuilder sb = new StringBuilder();
        for (GraphNodeDto node : request.nodes()) {
            sb.append(writeJson(Map.of("kind", "node", "payload", node))).append('\n');
        }
        for (GraphEdgeDto edge : request.edges()) {
            sb.append(writeJson(Map.of("kind", "edge", "payload", edge))).append('\n');
        }
        if (request.meta() != null) {
            sb.append(writeJson(Map.of("kind", "meta", "payload", request.meta()))).append('\n');
        }

        return new ExportedGraph("graph-export.ndjson", "application/x-ndjson", sb.toString().getBytes(StandardCharsets.UTF_8));
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
        Map<String, Integer> neighborsPerSeed = new LinkedHashMap<>();
        Set<String> acceptedEdgeIds = new LinkedHashSet<>();
        List<EdgeRow> accepted = new ArrayList<>();
        boolean truncated = false;

        for (EdgeRow edge : candidateEdges) {
            List<String> relatedSeeds = relatedSeeds(edge, seedNodeIds, direction);
            if (relatedSeeds.isEmpty()) {
                continue;
            }

            boolean canInclude = relatedSeeds.stream()
                .anyMatch(seed -> neighborsPerSeed.getOrDefault(seed, 0) < maxNeighborsPerSeed);

            if (!canInclude) {
                truncated = true;
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

        return new PerSeedLimitResult(accepted, truncated);
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

    private GraphEdgeDto toEdgeDto(EdgeRow row,
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

    private GraphRelationFamily resolveRelationFamily(GraphRelationFamily relationFamily) {
        return relationFamily == null ? graphProperties.getDefaultRelationFamily() : relationFamily;
    }

    private List<GraphFacetCountDto> toFacetDtos(List<FacetCountRow> rows) {
        return rows.stream()
            .map(row -> new GraphFacetCountDto(row.key(), row.count()))
            .toList();
    }

    private int orDefault(Integer value, int fallback) {
        return value == null ? fallback : value;
    }

    private String writeJson(Object value) {
        if (value == null) {
            return "";
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception ex) {
            return "{}";
        }
    }

    private String join(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        return String.join("|", values);
    }

    private String csv(String raw) {
        if (raw == null) {
            return "";
        }
        return '"' + raw.replace("\"", "\"\"") + '"';
    }

    public record ExportedGraph(String fileName, String contentType, byte[] payload) {
    }

    private record PerSeedLimitResult(List<EdgeRow> edges, boolean truncated) {
    }

    private record LimitSelection(LinkedHashSet<String> nodeIds, List<EdgeRow> edges, boolean truncated) {
    }
}
