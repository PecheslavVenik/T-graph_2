package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.config.GraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphExpandResponse;
import com.pm.graph_api_v2.dto.GraphMetaDto;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import com.pm.graph_api_v2.dto.GraphQueryRequest;
import com.pm.graph_api_v2.dto.GraphQueryResultMode;
import com.pm.graph_api_v2.exception.ApiBadRequestException;
import com.pm.graph_api_v2.exception.ApiNotFoundException;
import com.pm.graph_api_v2.metrics.GraphMetrics;
import com.pm.graph_api_v2.repository.GraphEdgeRepository;
import com.pm.graph_api_v2.repository.GraphNodeRepository;
import com.pm.graph_api_v2.repository.GraphQueryBackend;
import com.pm.graph_api_v2.repository.GraphSqlRepository;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.SqlGraphQueryResult;
import org.springframework.stereotype.Service;

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
public class GraphSqlQueryService {

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

    private final GraphSqlRepository sqlRepository;
    private final GraphNodeRepository nodeRepository;
    private final GraphEdgeRepository edgeRepository;
    private final GraphQueryBackend graphQueryBackend;
    private final GraphExpansionService graphExpansionService;
    private final GraphDtoMapper graphDtoMapper;
    private final GraphRequestNormalizer requestNormalizer;
    private final GraphProperties graphProperties;
    private final GraphMetrics graphMetrics;

    public GraphSqlQueryService(GraphSqlRepository sqlRepository,
                                GraphNodeRepository nodeRepository,
                                GraphEdgeRepository edgeRepository,
                                GraphQueryBackend graphQueryBackend,
                                GraphExpansionService graphExpansionService,
                                GraphDtoMapper graphDtoMapper,
                                GraphRequestNormalizer requestNormalizer,
                                GraphProperties graphProperties,
                                GraphMetrics graphMetrics) {
        this.sqlRepository = sqlRepository;
        this.nodeRepository = nodeRepository;
        this.edgeRepository = edgeRepository;
        this.graphQueryBackend = graphQueryBackend;
        this.graphExpansionService = graphExpansionService;
        this.graphDtoMapper = graphDtoMapper;
        this.requestNormalizer = requestNormalizer;
        this.graphProperties = graphProperties;
        this.graphMetrics = graphMetrics;
    }

    public GraphExpandResponse query(GraphQueryRequest request) {
        long startedAt = System.nanoTime();
        var sample = graphMetrics.startTimer();

        try {
            String sql = normalizeReadOnlySql(request.sql());
            GraphQueryResultMode resultMode = request.resultMode() == null ? GraphQueryResultMode.SEEDS : request.resultMode();
            String relationFamily = requestNormalizer.relationFamilyOrDefault(request.relationFamily());
            List<String> edgeTypes = requestNormalizer.edgeTypes(request.edgeTypes());
            Direction direction = requestNormalizer.directionOrBoth(request.direction());
            int maxNeighborsPerSeed = requestNormalizer.orDefault(request.maxNeighborsPerSeed(), graphProperties.getDefaultMaxNeighborsPerSeed());
            int maxNodes = requestNormalizer.orDefault(request.maxNodes(), graphProperties.getDefaultMaxNodes());
            int maxEdges = requestNormalizer.orDefault(request.maxEdges(), graphProperties.getDefaultMaxEdges());
            boolean includeAttributes = requestNormalizer.includeAttributes(request.includeAttributes());

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
        List<String> queriedNodeIds = sqlRepository.executeSqlNodeIdQuery(sql, seedLimit + 1);
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

        return graphExpansionService.expandResolvedNodes(
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
        SqlGraphQueryResult queryResult = sqlRepository.executeSqlGraphQuery(sql, rowLimit);

        Map<String, EdgeRow> edgesById = new LinkedHashMap<>();
        for (EdgeRow row : edgeRepository.findEdgesByIdsInOrder(queryResult.edgeIds())) {
            edgesById.putIfAbsent(row.edgeId(), row);
        }

        int remainingEdgeSlots = Math.max(0, maxEdges + 1 - edgesById.size());
        if (!queryResult.nodePairs().isEmpty() && remainingEdgeSlots > 0) {
            for (EdgeRow row : edgeRepository.findEdgesByEndpointPairs(queryResult.nodePairs(), remainingEdgeSlots)) {
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
        List<NodeRow> nodeRows = nodeRepository.findNodesByIdsInOrder(returnedNodeIds);
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
}
