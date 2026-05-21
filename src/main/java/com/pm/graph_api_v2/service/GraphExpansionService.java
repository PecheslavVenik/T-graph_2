package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphAttributeFilterDto;
import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphExpandExcludeDto;
import com.pm.graph_api_v2.dto.GraphExpandFacetsDto;
import com.pm.graph_api_v2.dto.GraphExpandFiltersDto;
import com.pm.graph_api_v2.dto.GraphExpandPreviewDto;
import com.pm.graph_api_v2.dto.GraphExpandPreviewResponse;
import com.pm.graph_api_v2.dto.GraphExpandPreviewSummaryDto;
import com.pm.graph_api_v2.dto.GraphExpandRequest;
import com.pm.graph_api_v2.dto.GraphExpandResponse;
import com.pm.graph_api_v2.dto.GraphFacetCountDto;
import com.pm.graph_api_v2.dto.GraphMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.exception.ApiBadRequestException;
import com.pm.graph_api_v2.exception.ApiNotFoundException;
import com.pm.graph_api_v2.metrics.GraphMetrics;
import com.pm.graph_api_v2.repository.GraphEdgeRepository;
import com.pm.graph_api_v2.repository.GraphNodeRepository;
import com.pm.graph_api_v2.repository.GraphQueryBackend;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class GraphExpansionService {

    private static final String UNKNOWN_NODE_TYPE = "UNKNOWN";
    private static final String FULL_DATABASE_STRATEGY = "FULL_DATABASE_GRAPH";

    private final GraphNodeRepository nodeRepository;
    private final GraphEdgeRepository edgeRepository;
    private final GraphQueryBackend graphQueryBackend;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphExpandPlanner graphExpandPlanner;
    private final GraphRequestNormalizer requestNormalizer;
    private final GraphProperties graphProperties;
    private final GraphMetrics graphMetrics;

    public GraphExpansionService(GraphNodeRepository nodeRepository,
                                 GraphEdgeRepository edgeRepository,
                                 GraphQueryBackend graphQueryBackend,
                                 GraphDtoMapper graphDtoMapper,
                                 GraphExpandPlanner graphExpandPlanner,
                                 GraphRequestNormalizer requestNormalizer,
                                 GraphProperties graphProperties,
                                 GraphMetrics graphMetrics) {
        this.nodeRepository = nodeRepository;
        this.edgeRepository = edgeRepository;
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
            ExpandContext context = buildContext(request, startedAt, GraphExpandPlanner.RANKING_STRATEGY, List.of());
            return expandPrepared(context, prepareCandidates(context));
        } finally {
            graphMetrics.stopTimer(sample, "expand");
        }
    }

    public GraphExpandResponse fullGraph(boolean includeAttributes) {
        long startedAt = System.nanoTime();
        var sample = graphMetrics.startTimer();

        try {
            List<GraphNodeDto> nodes = nodeRepository.findAllNodes().stream()
                .map(row -> graphDtoMapper.toNodeDto(row, includeAttributes))
                .toList();
            List<GraphEdgeDto> edges = edgeRepository.findAllEdges().stream()
                .map(row -> graphDtoMapper.toEdgeDto(row, includeAttributes))
                .toList();

            GraphMetaDto meta = new GraphMetaDto(
                false,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
                graphQueryBackend.source(),
                GraphRelationFamilies.ALL_RELATIONS,
                FULL_DATABASE_STRATEGY,
                edges.size(),
                nodes.size(),
                edges.size(),
                List.of()
            );

            graphMetrics.recordCandidateEdgeCount(edges.size());
            graphMetrics.recordNodeCount(nodes.size());
            graphMetrics.recordEdgeCount(edges.size());

            return new GraphExpandResponse(nodes, edges, meta);
        } finally {
            graphMetrics.stopTimer(sample, "full_graph");
        }
    }

    public GraphExpandPreviewResponse preview(GraphExpandRequest request) {
        long startedAt = System.nanoTime();
        var sample = graphMetrics.startTimer();

        try {
            ExpandContext context = buildContext(request, startedAt, GraphExpandPlanner.RANKING_STRATEGY, List.of());
            PreparedCandidates prepared = prepareCandidates(context);
            return previewPrepared(context, prepared);
        } finally {
            graphMetrics.stopTimer(sample, "expand_preview");
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
        ExpandContext context = buildContext(
            seedNodeIds,
            relationFamily,
            relationFamiliesFromLegacy(relationFamily),
            requestNormalizer.edgeTypes(edgeTypes),
            List.of(),
            Map.of(),
            Map.of(),
            Set.of(),
            Set.of(),
            direction,
            maxNeighborsPerSeed,
            maxNodes,
            maxEdges,
            includeAttributes,
            startedAt,
            rankingStrategy,
            initialWarnings
        );
        return expandPrepared(context, prepareCandidates(context));
    }

    private GraphExpandResponse expandPrepared(ExpandContext context, PreparedCandidates prepared) {
        if (context.seedNodeIds().size() > context.maxNodes() + context.excludeNodeIds().size()) {
            throw new ApiBadRequestException("maxNodes is lower than number of resolved seed nodes");
        }

        GraphExpandPlanner.ExpandPlan expandPlan = graphExpandPlanner.plan(
            context.seedNodeIds(),
            prepared.candidateEdges(),
            context.direction(),
            context.maxNeighborsPerSeed(),
            plannerNodeBudget(context),
            context.maxEdges()
        );

        List<GraphNodeDto> nodes = expandPlan.nodeIds().stream()
            .filter(nodeId -> !context.excludeNodeIds().contains(nodeId))
            .map(expandPlan.nodesById()::get)
            .filter(row -> row != null)
            .map(row -> graphDtoMapper.toNodeDto(row, context.includeAttributes()))
            .toList();

        List<GraphEdgeDto> edges = expandPlan.edges().stream()
            .map(row -> graphExpandPlanner.toEdgeDto(row, context.includeAttributes(), context.seedNodeIds(), context.direction(), expandPlan.nodesById()))
            .toList();

        List<String> warnings = new ArrayList<>(context.initialWarnings());
        warnings.addAll(buildWarnings(prepared.candidateBudgetHit(), expandPlan.perSeedTruncated(), expandPlan.globalTruncated()));

        GraphMetaDto meta = new GraphMetaDto(
            prepared.candidateBudgetHit() || expandPlan.perSeedTruncated() || expandPlan.globalTruncated() || !context.initialWarnings().isEmpty(),
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - context.startedAt()),
            graphQueryBackend.source(),
            context.relationLabel(),
            context.rankingStrategy(),
            prepared.candidateEdges().size(),
            nodes.size(),
            edges.size(),
            warnings
        );

        graphMetrics.recordNodeCount(nodes.size());
        graphMetrics.recordEdgeCount(edges.size());
        if (meta.truncated()) {
            graphMetrics.recordTruncation(context.rankingStrategy());
        }

        return new GraphExpandResponse(nodes, edges, meta);
    }

    private GraphExpandPreviewResponse previewPrepared(ExpandContext context, PreparedCandidates prepared) {
        PreviewCounts previewCounts = countPreview(context, prepared);
        boolean wouldTruncateByNeighborBudget = previewCounts.maxNeighborsForAnySeed() > context.maxNeighborsPerSeed();

        return new GraphExpandPreviewResponse(
            new GraphExpandPreviewSummaryDto(
                prepared.candidateEdges().size(),
                previewCounts.uniqueNeighborNodeIds().size(),
                previewCounts.newNodeIds().size(),
                prepared.candidateEdges().size()
            ),
            new GraphExpandFacetsDto(
                toFacetCounts(prepared.candidateEdges(), EdgeRow::relationFamily),
                toFacetCounts(prepared.candidateEdges(), EdgeRow::edgeType),
                toNeighborTypeFacetCounts(context, prepared),
                toNodeAttributeFacetCounts(context, prepared),
                toEdgeAttributeFacetCounts(context, prepared)
            ),
            new GraphExpandPreviewDto(
                context.maxNeighborsPerSeed(),
                context.maxNodes(),
                context.maxEdges(),
                wouldTruncateByNeighborBudget
            )
        );
    }

    private ExpandContext buildContext(GraphExpandRequest request,
                                       long startedAt,
                                       String rankingStrategy,
                                       List<String> initialWarnings) {
        LinkedHashSet<String> seedNodeIds = nodeRepository.resolveNodeIds(request.seeds());
        if (seedNodeIds.isEmpty()) {
            throw new ApiNotFoundException("No seed nodes were resolved from provided identifiers");
        }

        GraphExpandFiltersDto filters = request.filters();
        List<String> relationFamilies = relationFamilies(filters, request.relationFamily());
        List<String> edgeTypes = edgeTypes(filters, request.edgeTypes());
        String relationLabel = relationLabel(relationFamilies, request.relationFamily());

        return buildContext(
            seedNodeIds,
            backendRelationFamily(relationFamilies, request.relationFamily()),
            relationFamilies,
            edgeTypes,
            nodeTypes(filters),
            attributeFilters(filters == null ? null : filters.nodeAttributes()),
            attributeFilters(filters == null ? null : filters.edgeAttributes()),
            nodeIdSet(request.exclude()),
            edgeIdSet(request.exclude()),
            request.direction(),
            requestNormalizer.orDefault(request.maxNeighborsPerSeed(), graphProperties.getDefaultMaxNeighborsPerSeed()),
            requestNormalizer.orDefault(request.maxNodes(), graphProperties.getDefaultMaxNodes()),
            requestNormalizer.orDefault(request.maxEdges(), graphProperties.getDefaultMaxEdges()),
            requestNormalizer.includeAttributes(request.includeAttributes()),
            startedAt,
            rankingStrategy,
            initialWarnings,
            relationLabel
        );
    }

    private ExpandContext buildContext(LinkedHashSet<String> seedNodeIds,
                                       String backendRelationFamily,
                                       List<String> relationFamilies,
                                       List<String> edgeTypes,
                                       List<String> nodeTypes,
                                       Map<String, GraphAttributeFilterDto> nodeAttributeFilters,
                                       Map<String, GraphAttributeFilterDto> edgeAttributeFilters,
                                       Set<String> excludeNodeIds,
                                       Set<String> excludeEdgeIds,
                                       Direction direction,
                                       int maxNeighborsPerSeed,
                                       int maxNodes,
                                       int maxEdges,
                                       boolean includeAttributes,
                                       long startedAt,
                                       String rankingStrategy,
                                       List<String> initialWarnings) {
        return buildContext(
            seedNodeIds,
            backendRelationFamily,
            relationFamilies,
            edgeTypes,
            nodeTypes,
            nodeAttributeFilters,
            edgeAttributeFilters,
            excludeNodeIds,
            excludeEdgeIds,
            direction,
            maxNeighborsPerSeed,
            maxNodes,
            maxEdges,
            includeAttributes,
            startedAt,
            rankingStrategy,
            initialWarnings,
            relationLabel(relationFamilies, backendRelationFamily)
        );
    }

    private ExpandContext buildContext(LinkedHashSet<String> seedNodeIds,
                                       String backendRelationFamily,
                                       List<String> relationFamilies,
                                       List<String> edgeTypes,
                                       List<String> nodeTypes,
                                       Map<String, GraphAttributeFilterDto> nodeAttributeFilters,
                                       Map<String, GraphAttributeFilterDto> edgeAttributeFilters,
                                       Set<String> excludeNodeIds,
                                       Set<String> excludeEdgeIds,
                                       Direction direction,
                                       int maxNeighborsPerSeed,
                                       int maxNodes,
                                       int maxEdges,
                                       boolean includeAttributes,
                                       long startedAt,
                                       String rankingStrategy,
                                       List<String> initialWarnings,
                                       String relationLabel) {
        return new ExpandContext(
            seedNodeIds,
            backendRelationFamily,
            relationLabel,
            relationFamilies,
            edgeTypes,
            nodeTypes,
            nodeAttributeFilters,
            edgeAttributeFilters,
            excludeNodeIds,
            excludeEdgeIds,
            direction,
            maxNeighborsPerSeed,
            maxNodes,
            maxEdges,
            includeAttributes,
            startedAt,
            rankingStrategy,
            initialWarnings
        );
    }

    private PreparedCandidates prepareCandidates(ExpandContext context) {
        List<EdgeRow> rawCandidateEdges = graphQueryBackend.findExpandEdges(
            context.seedNodeIds(),
            context.backendRelationFamily(),
            context.edgeTypes(),
            context.direction(),
            graphProperties.getMaxExpandCandidateEdges()
        );
        boolean candidateBudgetHit = rawCandidateEdges.size() >= graphProperties.getMaxExpandCandidateEdges();

        List<EdgeRow> edgeFiltered = rawCandidateEdges.stream()
            .filter(edge -> !context.excludeEdgeIds().contains(edge.edgeId()))
            .filter(edge -> matchesRelationFamilies(edge, context.relationFamilies()))
            .filter(edge -> matchesEdgeTypes(edge, context.edgeTypes()))
            .filter(edge -> matchesAttributeFilters(edgeAttributes(edge), context.edgeAttributeFilters()))
            .toList();

        Map<String, NodeRow> nodesById = loadNodesById(context.seedNodeIds(), edgeFiltered);
        List<EdgeRow> candidateEdges = edgeFiltered.stream()
            .filter(edge -> matchesNeighborFilters(edge, context, nodesById))
            .toList();

        graphMetrics.recordCandidateEdgeCount(candidateEdges.size());
        return new PreparedCandidates(candidateEdges, nodesById, candidateBudgetHit);
    }

    private boolean matchesRelationFamilies(EdgeRow edge, List<String> relationFamilies) {
        return relationFamilies.isEmpty() || relationFamilies.contains(normalizeUpper(edge.relationFamily()));
    }

    private boolean matchesEdgeTypes(EdgeRow edge, List<String> edgeTypes) {
        return edgeTypes.isEmpty() || edgeTypes.contains(normalizeUpper(edge.edgeType()));
    }

    private boolean matchesNeighborFilters(EdgeRow edge, ExpandContext context, Map<String, NodeRow> nodesById) {
        if (context.nodeTypes().isEmpty() && context.nodeAttributeFilters().isEmpty()) {
            return true;
        }

        for (String seedNodeId : relatedSeeds(edge, context.seedNodeIds(), context.direction())) {
            NodeRow neighbor = nodesById.get(neighborForSeed(edge, seedNodeId));
            if (neighbor == null) {
                continue;
            }
            boolean nodeTypeMatches = context.nodeTypes().isEmpty() || context.nodeTypes().contains(normalizeUpper(neighbor.nodeType()));
            if (nodeTypeMatches && matchesAttributeFilters(nodeAttributes(neighbor), context.nodeAttributeFilters())) {
                return true;
            }
        }
        return false;
    }

    private Map<String, NodeRow> loadNodesById(Collection<String> seedNodeIds, List<EdgeRow> edges) {
        LinkedHashSet<String> nodeIds = new LinkedHashSet<>(seedNodeIds);
        for (EdgeRow edge : edges) {
            nodeIds.add(edge.fromNodeId());
            nodeIds.add(edge.toNodeId());
        }

        return nodeRepository.findNodesByIds(nodeIds).stream()
            .collect(Collectors.toMap(NodeRow::nodeId, Function.identity(), (left, ignored) -> left, LinkedHashMap::new));
    }

    private PreviewCounts countPreview(ExpandContext context, PreparedCandidates prepared) {
        LinkedHashSet<String> uniqueNeighborNodeIds = new LinkedHashSet<>();
        LinkedHashSet<String> newNodeIds = new LinkedHashSet<>();
        Map<String, LinkedHashSet<String>> neighborsBySeed = new LinkedHashMap<>();

        for (EdgeRow edge : prepared.candidateEdges()) {
            for (String seedNodeId : relatedSeeds(edge, context.seedNodeIds(), context.direction())) {
                String neighborNodeId = neighborForSeed(edge, seedNodeId);
                uniqueNeighborNodeIds.add(neighborNodeId);
                neighborsBySeed
                    .computeIfAbsent(seedNodeId, ignored -> new LinkedHashSet<>())
                    .add(neighborNodeId);
            }

            addNewNode(edge.fromNodeId(), context, newNodeIds);
            addNewNode(edge.toNodeId(), context, newNodeIds);
        }

        int maxNeighborsForAnySeed = neighborsBySeed.values().stream()
            .mapToInt(Set::size)
            .max()
            .orElse(0);

        return new PreviewCounts(uniqueNeighborNodeIds, newNodeIds, maxNeighborsForAnySeed);
    }

    private void addNewNode(String nodeId, ExpandContext context, Set<String> newNodeIds) {
        if (!context.seedNodeIds().contains(nodeId) && !context.excludeNodeIds().contains(nodeId)) {
            newNodeIds.add(nodeId);
        }
    }

    private List<GraphFacetCountDto> toNeighborTypeFacetCounts(ExpandContext context, PreparedCandidates prepared) {
        Map<String, Set<String>> neighborIdsByType = new LinkedHashMap<>();
        for (EdgeRow edge : prepared.candidateEdges()) {
            for (String seedNodeId : relatedSeeds(edge, context.seedNodeIds(), context.direction())) {
                String neighborNodeId = neighborForSeed(edge, seedNodeId);
                String nodeType = nodeType(prepared.nodesById().get(neighborNodeId));
                neighborIdsByType
                    .computeIfAbsent(nodeType, ignored -> new LinkedHashSet<>())
                    .add(neighborNodeId);
            }
        }

        return neighborIdsByType.entrySet().stream()
            .map(entry -> new GraphFacetCountDto(entry.getKey(), entry.getValue().size()))
            .sorted(Comparator.comparingInt(GraphFacetCountDto::count).reversed().thenComparing(GraphFacetCountDto::key))
            .toList();
    }

    private Map<String, List<GraphFacetCountDto>> toNodeAttributeFacetCounts(ExpandContext context, PreparedCandidates prepared) {
        if (context.nodeAttributeFilters().isEmpty()) {
            return Map.of();
        }

        Map<String, Map<String, Set<String>>> nodeIdsByAttributeValue = new LinkedHashMap<>();
        for (String attributeName : context.nodeAttributeFilters().keySet()) {
            nodeIdsByAttributeValue.put(attributeName, new LinkedHashMap<>());
        }

        for (EdgeRow edge : prepared.candidateEdges()) {
            for (String seedNodeId : relatedSeeds(edge, context.seedNodeIds(), context.direction())) {
                String neighborNodeId = neighborForSeed(edge, seedNodeId);
                NodeRow neighbor = prepared.nodesById().get(neighborNodeId);
                if (neighbor == null) {
                    continue;
                }
                Map<String, Object> attributes = nodeAttributes(neighbor);
                for (String attributeName : context.nodeAttributeFilters().keySet()) {
                    String value = facetValue(attributes.get(attributeName));
                    if (value == null) {
                        continue;
                    }
                    nodeIdsByAttributeValue
                        .get(attributeName)
                        .computeIfAbsent(value, ignored -> new LinkedHashSet<>())
                        .add(neighborNodeId);
                }
            }
        }

        Map<String, List<GraphFacetCountDto>> facets = new LinkedHashMap<>();
        nodeIdsByAttributeValue.forEach((attributeName, countsByValue) -> facets.put(attributeName, toFacetCountsFromSets(countsByValue)));
        return facets;
    }

    private Map<String, List<GraphFacetCountDto>> toEdgeAttributeFacetCounts(ExpandContext context, PreparedCandidates prepared) {
        if (context.edgeAttributeFilters().isEmpty()) {
            return Map.of();
        }

        Map<String, Map<String, Integer>> countsByAttributeValue = new LinkedHashMap<>();
        for (String attributeName : context.edgeAttributeFilters().keySet()) {
            countsByAttributeValue.put(attributeName, new LinkedHashMap<>());
        }

        for (EdgeRow edge : prepared.candidateEdges()) {
            Map<String, Object> attributes = edgeAttributes(edge);
            for (String attributeName : context.edgeAttributeFilters().keySet()) {
                String value = facetValue(attributes.get(attributeName));
                if (value == null) {
                    continue;
                }
                countsByAttributeValue
                    .get(attributeName)
                    .merge(value, 1, Integer::sum);
            }
        }

        Map<String, List<GraphFacetCountDto>> facets = new LinkedHashMap<>();
        countsByAttributeValue.forEach((attributeName, countsByValue) -> facets.put(attributeName, toFacetCountsFromIntegers(countsByValue)));
        return facets;
    }

    private List<GraphFacetCountDto> toFacetCounts(List<EdgeRow> edges, Function<EdgeRow, String> classifier) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (EdgeRow edge : edges) {
            String key = classifier.apply(edge);
            if (key == null || key.isBlank()) {
                continue;
            }
            counts.merge(key, 1, Integer::sum);
        }

        return counts.entrySet().stream()
            .map(entry -> new GraphFacetCountDto(entry.getKey(), entry.getValue()))
            .sorted(Comparator.comparingInt(GraphFacetCountDto::count).reversed().thenComparing(GraphFacetCountDto::key))
            .toList();
    }

    private List<GraphFacetCountDto> toFacetCountsFromSets(Map<String, Set<String>> countsByValue) {
        return countsByValue.entrySet().stream()
            .map(entry -> new GraphFacetCountDto(entry.getKey(), entry.getValue().size()))
            .sorted(Comparator.comparingInt(GraphFacetCountDto::count).reversed().thenComparing(GraphFacetCountDto::key))
            .toList();
    }

    private List<GraphFacetCountDto> toFacetCountsFromIntegers(Map<String, Integer> countsByValue) {
        return countsByValue.entrySet().stream()
            .map(entry -> new GraphFacetCountDto(entry.getKey(), entry.getValue()))
            .sorted(Comparator.comparingInt(GraphFacetCountDto::count).reversed().thenComparing(GraphFacetCountDto::key))
            .toList();
    }

    private String facetValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Collection<?> collection) {
            return collection.stream()
                .map(String::valueOf)
                .sorted()
                .collect(Collectors.joining(","));
        }
        return String.valueOf(value);
    }

    private int plannerNodeBudget(ExpandContext context) {
        long budget = (long) context.maxNodes() + context.seedNodeIds().size() + context.excludeNodeIds().size();
        return budget > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) budget;
    }

    private List<String> relationFamilies(GraphExpandFiltersDto filters, String legacyRelationFamily) {
        List<String> filterFamilies = filters == null ? List.of() : normalizeUpperList(filters.relationFamilies());
        if (!filterFamilies.isEmpty()) {
            return filterFamilies.contains(GraphRelationFamilies.ALL_RELATIONS) ? List.of() : filterFamilies;
        }
        return relationFamiliesFromLegacy(requestNormalizer.relationFamilyOrDefault(legacyRelationFamily));
    }

    private List<String> relationFamiliesFromLegacy(String relationFamily) {
        String normalized = GraphRelationFamilies.normalize(relationFamily);
        if (normalized == null || GraphRelationFamilies.isAllRelations(normalized)) {
            return List.of();
        }
        return List.of(normalized);
    }

    private String backendRelationFamily(List<String> relationFamilies, String legacyRelationFamily) {
        if (relationFamilies.size() == 1) {
            return relationFamilies.get(0);
        }
        String normalizedLegacy = requestNormalizer.relationFamilyOrDefault(legacyRelationFamily);
        return relationFamilies.isEmpty() ? normalizedLegacy : GraphRelationFamilies.ALL_RELATIONS;
    }

    private String relationLabel(List<String> relationFamilies, String fallback) {
        if (relationFamilies.isEmpty()) {
            return GraphRelationFamilies.ALL_RELATIONS;
        }
        if (relationFamilies.size() == 1) {
            return relationFamilies.get(0);
        }
        return String.join(",", relationFamilies);
    }

    private List<String> edgeTypes(GraphExpandFiltersDto filters, List<String> legacyEdgeTypes) {
        List<String> filterEdgeTypes = filters == null ? List.of() : normalizeUpperList(filters.edgeTypes());
        return filterEdgeTypes.isEmpty() ? requestNormalizer.edgeTypes(legacyEdgeTypes) : filterEdgeTypes;
    }

    private List<String> nodeTypes(GraphExpandFiltersDto filters) {
        return filters == null ? List.of() : normalizeUpperList(filters.nodeTypes());
    }

    private List<String> normalizeUpperList(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        return values.stream()
            .filter(value -> value != null && !value.isBlank())
            .map(this::normalizeUpper)
            .distinct()
            .toList();
    }

    private String normalizeUpper(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private Map<String, GraphAttributeFilterDto> attributeFilters(Map<String, GraphAttributeFilterDto> filters) {
        if (filters == null || filters.isEmpty()) {
            return Map.of();
        }
        Map<String, GraphAttributeFilterDto> normalized = new LinkedHashMap<>();
        filters.forEach((key, value) -> {
            if (key != null && !key.isBlank() && value != null) {
                normalized.put(key.trim(), value);
            }
        });
        return normalized;
    }

    private Set<String> nodeIdSet(GraphExpandExcludeDto exclude) {
        return normalizeIdSet(exclude == null ? null : exclude.nodeIds());
    }

    private Set<String> edgeIdSet(GraphExpandExcludeDto exclude) {
        return normalizeIdSet(exclude == null ? null : exclude.edgeIds());
    }

    private Set<String> normalizeIdSet(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Set.of();
        }
        return values.stream()
            .filter(value -> value != null && !value.isBlank())
            .map(String::trim)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Map<String, Object> nodeAttributes(NodeRow row) {
        Map<String, Object> attributes = new LinkedHashMap<>(row.attrs());
        attributes.put("nodeType", row.nodeType());
        attributes.put("displayName", row.displayName());
        attributes.put("employer", row.employer());
        attributes.put("city", row.city());
        attributes.put("sourceSystem", row.sourceSystem());
        attributes.put("pagerankScore", row.pagerankScore());
        attributes.put("hubScore", row.hubScore());
        attributes.put("blacklist", row.blacklist());
        attributes.put("isBlocked", row.blacklist());
        attributes.put("vip", row.vip());
        attributes.put("isVip", row.vip());
        return attributes;
    }

    private Map<String, Object> edgeAttributes(EdgeRow row) {
        Map<String, Object> attributes = new LinkedHashMap<>(row.attrs());
        attributes.put("edgeType", row.edgeType());
        attributes.put("type", row.edgeType());
        attributes.put("relationFamily", row.relationFamily());
        attributes.put("directed", row.directed());
        attributes.put("txCount", row.txCount());
        attributes.put("txSum", row.txSum());
        attributes.put("amount", row.txSum());
        attributes.put("strengthScore", row.strengthScore());
        attributes.put("evidenceCount", row.evidenceCount());
        attributes.put("sourceSystem", row.sourceSystem());
        return attributes;
    }

    private boolean matchesAttributeFilters(Map<String, Object> attributes, Map<String, GraphAttributeFilterDto> filters) {
        for (Map.Entry<String, GraphAttributeFilterDto> entry : filters.entrySet()) {
            if (!matchesAttributeFilter(attributes.get(entry.getKey()), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesAttributeFilter(Object actualValue, GraphAttributeFilterDto filter) {
        if (actualValue == null) {
            return false;
        }
        if (filter.eq() != null && !equalsFilterValue(actualValue, filter.eq())) {
            return false;
        }
        if (filter.in() != null && !filter.in().isEmpty()
            && filter.in().stream().noneMatch(expected -> equalsFilterValue(actualValue, expected))) {
            return false;
        }
        if (filter.gte() != null && !matchesLowerBound(actualValue, filter.gte())) {
            return false;
        }
        return filter.lte() == null || matchesUpperBound(actualValue, filter.lte());
    }

    private boolean equalsFilterValue(Object actualValue, Object expectedValue) {
        Double actualNumber = toDouble(actualValue);
        Double expectedNumber = toDouble(expectedValue);
        if (actualNumber != null && expectedNumber != null) {
            return Double.compare(actualNumber, expectedNumber) == 0;
        }
        return String.valueOf(actualValue).equalsIgnoreCase(String.valueOf(expectedValue));
    }

    private boolean matchesLowerBound(Object actualValue, double lowerBound) {
        Double actualNumber = toDouble(actualValue);
        return actualNumber != null && actualNumber >= lowerBound;
    }

    private boolean matchesUpperBound(Object actualValue, double upperBound) {
        Double actualNumber = toDouble(actualValue);
        return actualNumber != null && actualNumber <= upperBound;
    }

    private Double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return value == null ? null : Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String nodeType(NodeRow row) {
        if (row == null || row.nodeType() == null || row.nodeType().isBlank()) {
            return UNKNOWN_NODE_TYPE;
        }
        return row.nodeType();
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

    private String neighborForSeed(EdgeRow edge, String seedNodeId) {
        return seedNodeId.equals(edge.fromNodeId()) ? edge.toNodeId() : edge.fromNodeId();
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

    private record ExpandContext(LinkedHashSet<String> seedNodeIds,
                                 String backendRelationFamily,
                                 String relationLabel,
                                 List<String> relationFamilies,
                                 List<String> edgeTypes,
                                 List<String> nodeTypes,
                                 Map<String, GraphAttributeFilterDto> nodeAttributeFilters,
                                 Map<String, GraphAttributeFilterDto> edgeAttributeFilters,
                                 Set<String> excludeNodeIds,
                                 Set<String> excludeEdgeIds,
                                 Direction direction,
                                 int maxNeighborsPerSeed,
                                 int maxNodes,
                                 int maxEdges,
                                 boolean includeAttributes,
                                 long startedAt,
                                 String rankingStrategy,
                                 List<String> initialWarnings) {
    }

    private record PreparedCandidates(List<EdgeRow> candidateEdges,
                                      Map<String, NodeRow> nodesById,
                                      boolean candidateBudgetHit) {
    }

    private record PreviewCounts(Set<String> uniqueNeighborNodeIds,
                                 Set<String> newNodeIds,
                                 int maxNeighborsForAnySeed) {
    }
}
