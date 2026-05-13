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
import com.pm.graph_api_v2.dto.GraphImportResponse;
import com.pm.graph_api_v2.dto.GraphMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.dto.GraphNodeSearchMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeSearchResponse;
import com.pm.graph_api_v2.dto.GraphNodeSummaryDto;
import com.pm.graph_api_v2.dto.GraphNodeSummaryResponse;
import com.pm.graph_api_v2.dto.GraphQueryRequest;
import com.pm.graph_api_v2.dto.GraphQueryResultMode;
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
import com.pm.graph_api_v2.repository.model.SqlGraphQueryResult;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Service
public class InvestigationService {

    private static final int DEFAULT_NODE_SEARCH_LIMIT = 20;
    private static final int MAX_NODE_SEARCH_LIMIT = 100;
    private static final int MAX_SQL_SEED_ROWS = 100;
    private static final int MAX_SQL_LENGTH = 10000;
    private static final String SQL_SEED_RANKING_STRATEGY = "SQL_SEED_QUERY";
    private static final String SQL_GRAPH_RANKING_STRATEGY = "SQL_GRAPH_QUERY";
    private static final Pattern SQL_DANGEROUS_KEYWORDS = Pattern.compile(
        "\\b(insert|update|delete|merge|drop|alter|create|truncate|copy|attach|detach|install|load|pragma|set|call|vacuum|checkpoint|export|import)\\b",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern SQL_EXTERNAL_READS = Pattern.compile(
        "\\b(read_csv|read_json|read_parquet|read_text|read_blob|glob|parquet_scan|csv_auto|sqlite_scan|postgres_scan)\\s*\\(",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern SQL_INTERNAL_SCHEMAS = Pattern.compile(
        "\\b(information_schema|duckdb_[a-z_]+|pg_[a-z_]+|sqlite_[a-z_]+|flyway_schema_history)\\b",
        Pattern.CASE_INSENSITIVE
    );

    private final GraphRepository graphRepository;
    private final GraphQueryBackend graphQueryBackend;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphDictionaryFactory graphDictionaryFactory;
    private final GraphExpandPlanner graphExpandPlanner;
    private final GraphExportService graphExportService;
    private final GraphImportService graphImportService;
    private final GraphProperties graphProperties;
    private final GraphMetrics graphMetrics;

    public InvestigationService(GraphRepository graphRepository,
                                GraphQueryBackend graphQueryBackend,
                                GraphDtoMapper graphDtoMapper,
                                GraphDictionaryFactory graphDictionaryFactory,
                                GraphExpandPlanner graphExpandPlanner,
                                GraphExportService graphExportService,
                                GraphImportService graphImportService,
                                GraphProperties graphProperties,
                                GraphMetrics graphMetrics) {
        this.graphRepository = graphRepository;
        this.graphQueryBackend = graphQueryBackend;
        this.graphDtoMapper = graphDtoMapper;
        this.graphDictionaryFactory = graphDictionaryFactory;
        this.graphExpandPlanner = graphExpandPlanner;
        this.graphExportService = graphExportService;
        this.graphImportService = graphImportService;
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

    public GraphExpandResponse query(GraphQueryRequest request) {
        long startedAt = System.nanoTime();
        Timer.Sample sample = graphMetrics.startTimer();

        try {
            String sql = normalizeReadOnlySql(request.sql());
            GraphQueryResultMode resultMode = request.resultMode() == null ? GraphQueryResultMode.SEEDS : request.resultMode();
            String relationFamily = resolveRelationFamily(request.relationFamily());
            List<String> edgeTypes = normalizeEdgeTypes(request.edgeTypes());
            Direction direction = request.direction() == null ? Direction.BOTH : request.direction();
            int maxNeighborsPerSeed = orDefault(request.maxNeighborsPerSeed(), graphProperties.getDefaultMaxNeighborsPerSeed());
            int maxNodes = orDefault(request.maxNodes(), graphProperties.getDefaultMaxNodes());
            int maxEdges = orDefault(request.maxEdges(), graphProperties.getDefaultMaxEdges());
            boolean includeAttributes = request.includeAttributes() == null || request.includeAttributes();

            return switch (resultMode) {
                case SEEDS -> querySeedExpand(
                    sql,
                    relationFamily,
                    edgeTypes,
                    direction,
                    maxNeighborsPerSeed,
                    maxNodes,
                    maxEdges,
                    includeAttributes,
                    startedAt
                );
                case GRAPH -> queryGraph(
                    sql,
                    relationFamily,
                    maxNodes,
                    maxEdges,
                    includeAttributes,
                    startedAt
                );
            };
        } finally {
            graphMetrics.stopTimer(sample, "sql_query");
        }
    }

    public GraphImportResponse importPreview(MultipartFile file) {
        return graphImportService.preview(file);
    }

    public GraphImportResponse importCommit(MultipartFile file) {
        return graphImportService.commit(file);
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

    public GraphNodeSearchResponse searchNodes(String query,
                                               String nodeType,
                                               int limit,
                                               boolean includeAttributes) {
        String normalizedQuery = normalizeSearchQuery(query);
        String normalizedNodeType = normalizeNodeType(nodeType);
        int effectiveLimit = normalizeSearchLimit(limit);

        Timer.Sample sample = graphMetrics.startTimer();
        try {
            List<NodeRow> rows = graphRepository.searchNodes(normalizedQuery, normalizedNodeType, effectiveLimit + 1);
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

    public ExportedGraph export(GraphExportRequest request, GraphExportFormat format) {
        return graphExportService.export(request, format);
    }

    private GraphExpandResponse querySeedExpand(String sql,
                                                String relationFamily,
                                                List<String> edgeTypes,
                                                Direction direction,
                                                int maxNeighborsPerSeed,
                                                int maxNodes,
                                                int maxEdges,
                                                boolean includeAttributes,
                                                long startedAt) {
        int seedLimit = Math.min(maxNodes, MAX_SQL_SEED_ROWS);
        List<String> queriedNodeIds = graphRepository.executeSqlNodeIdQuery(sql, seedLimit + 1);
        boolean seedTruncated = queriedNodeIds.size() > seedLimit;

        LinkedHashSet<String> seedNodeIds = new LinkedHashSet<>();
        queriedNodeIds.stream()
            .limit(seedLimit)
            .forEach(seedNodeIds::add);

        if (seedNodeIds.isEmpty()) {
            throw new ApiNotFoundException("SQL query did not return seed node ids");
        }

        List<String> warnings = new ArrayList<>();
        if (seedTruncated) {
            graphMetrics.recordGuardrailHit("sql_query", "seed_rows");
            warnings.add("SQL seed query returned more rows than the seed limit");
        }

        return expandResolvedNodes(
            seedNodeIds,
            relationFamily,
            edgeTypes,
            direction,
            maxNeighborsPerSeed,
            maxNodes,
            maxEdges,
            includeAttributes,
            startedAt,
            SQL_SEED_RANKING_STRATEGY,
            warnings
        );
    }

    private GraphExpandResponse queryGraph(String sql,
                                           String relationFamily,
                                           int maxNodes,
                                           int maxEdges,
                                           boolean includeAttributes,
                                           long startedAt) {
        int rowLimit = Math.max(maxNodes, maxEdges) + 1;
        SqlGraphQueryResult queryResult = graphRepository.executeSqlGraphQuery(sql, rowLimit);

        Map<String, EdgeRow> edgesById = new LinkedHashMap<>();
        for (EdgeRow row : graphRepository.findEdgesByIdsInOrder(queryResult.edgeIds())) {
            edgesById.putIfAbsent(row.edgeId(), row);
        }

        int remainingEdgeSlots = Math.max(0, maxEdges + 1 - edgesById.size());
        if (!queryResult.nodePairs().isEmpty() && remainingEdgeSlots > 0) {
            for (EdgeRow row : graphRepository.findEdgesByEndpointPairs(queryResult.nodePairs(), remainingEdgeSlots)) {
                edgesById.putIfAbsent(row.edgeId(), row);
            }
        }

        boolean edgeTruncated = edgesById.size() > maxEdges;
        List<EdgeRow> edgeRows = edgesById.values()
            .stream()
            .limit(maxEdges)
            .toList();

        LinkedHashSet<String> nodeIds = new LinkedHashSet<>(queryResult.nodeIds());
        for (EdgeRow edgeRow : edgeRows) {
            nodeIds.add(edgeRow.fromNodeId());
            nodeIds.add(edgeRow.toNodeId());
        }
        boolean nodeTruncated = nodeIds.size() > maxNodes;

        List<String> returnedNodeIds = nodeIds.stream()
            .limit(maxNodes)
            .toList();
        List<NodeRow> nodeRows = graphRepository.findNodesByIdsInOrder(returnedNodeIds);
        Set<String> returnedNodeIdSet = new LinkedHashSet<>();
        for (NodeRow nodeRow : nodeRows) {
            returnedNodeIdSet.add(nodeRow.nodeId());
        }

        List<EdgeRow> returnedEdgeRows = edgeRows.stream()
            .filter(edge -> returnedNodeIdSet.contains(edge.fromNodeId()) && returnedNodeIdSet.contains(edge.toNodeId()))
            .toList();

        if (nodeRows.isEmpty() && returnedEdgeRows.isEmpty()) {
            throw new ApiNotFoundException("SQL query did not return graph node or edge references");
        }

        List<GraphNodeDto> nodes = nodeRows.stream()
            .map(row -> graphDtoMapper.toNodeDto(row, includeAttributes))
            .toList();
        List<GraphEdgeDto> edges = returnedEdgeRows.stream()
            .map(row -> graphDtoMapper.toEdgeDto(row, includeAttributes))
            .toList();

        List<String> warnings = new ArrayList<>();
        boolean rowTruncated = queryResult.rowCount() > rowLimit - 1;
        if (rowTruncated || edgeTruncated || nodeTruncated) {
            graphMetrics.recordGuardrailHit("sql_query", "graph_result_size");
            warnings.add("SQL graph query result was truncated by graph result limits");
        }

        GraphMetaDto meta = new GraphMetaDto(
            rowTruncated || edgeTruncated || nodeTruncated,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
            graphQueryBackend.source(),
            relationFamily,
            SQL_GRAPH_RANKING_STRATEGY,
            edgesById.size(),
            nodes.size(),
            edges.size(),
            warnings
        );

        graphMetrics.recordNodeCount(nodes.size());
        graphMetrics.recordEdgeCount(edges.size());
        if (meta.truncated()) {
            graphMetrics.recordTruncation("sql_query");
        }

        return new GraphExpandResponse(nodes, edges, meta);
    }

    private GraphExpandResponse expandResolvedNodes(LinkedHashSet<String> seedNodeIds,
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

    private String normalizeReadOnlySql(String sql) {
        if (sql == null || sql.trim().isBlank()) {
            throw new ApiBadRequestException("sql must not be blank");
        }

        String normalized = sql.trim();
        if (normalized.length() > MAX_SQL_LENGTH) {
            throw new ApiBadRequestException("sql must be at most " + MAX_SQL_LENGTH + " characters");
        }
        while (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }

        String lowerSql = normalized.toLowerCase(Locale.ROOT);
        if (normalized.contains(";") || lowerSql.contains("--") || lowerSql.contains("/*") || lowerSql.contains("*/")) {
            throw new ApiBadRequestException("Only a single read-only SELECT statement is allowed");
        }
        if (!lowerSql.startsWith("select ") && !lowerSql.startsWith("with ")) {
            throw new ApiBadRequestException("SQL query must start with SELECT or WITH");
        }
        if (SQL_DANGEROUS_KEYWORDS.matcher(lowerSql).find()
            || SQL_EXTERNAL_READS.matcher(lowerSql).find()
            || SQL_INTERNAL_SCHEMAS.matcher(lowerSql).find()) {
            throw new ApiBadRequestException("SQL query contains a disallowed statement or table reference");
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

    private int orDefault(Integer value, int fallback) {
        return value == null ? fallback : value;
    }
}
