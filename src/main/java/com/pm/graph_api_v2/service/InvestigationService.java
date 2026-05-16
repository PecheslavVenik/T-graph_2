package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphDictionaryResponse;
import com.pm.graph_api_v2.dto.GraphExpandRequest;
import com.pm.graph_api_v2.dto.GraphExpandResponse;
import com.pm.graph_api_v2.dto.GraphExportFormat;
import com.pm.graph_api_v2.dto.GraphExportRequest;
import com.pm.graph_api_v2.dto.GraphImportResponse;
import com.pm.graph_api_v2.dto.GraphNodeSearchResponse;
import com.pm.graph_api_v2.dto.GraphNodeSummaryResponse;
import com.pm.graph_api_v2.dto.GraphQueryRequest;
import com.pm.graph_api_v2.dto.ShortestPathRequest;
import com.pm.graph_api_v2.dto.ShortestPathResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class InvestigationService {

    private final GraphExpansionService graphExpansionService;
    private final GraphPathService graphPathService;
    private final GraphSqlQueryService graphSqlQueryService;
    private final GraphImportService graphImportService;
    private final GraphDictionaryService graphDictionaryService;
    private final GraphNodeReadService graphNodeReadService;
    private final GraphExportService graphExportService;

    public InvestigationService(GraphExpansionService graphExpansionService,
                                GraphPathService graphPathService,
                                GraphSqlQueryService graphSqlQueryService,
                                GraphImportService graphImportService,
                                GraphDictionaryService graphDictionaryService,
                                GraphNodeReadService graphNodeReadService,
                                GraphExportService graphExportService) {
        this.graphExpansionService = graphExpansionService;
        this.graphPathService = graphPathService;
        this.graphSqlQueryService = graphSqlQueryService;
        this.graphImportService = graphImportService;
        this.graphDictionaryService = graphDictionaryService;
        this.graphNodeReadService = graphNodeReadService;
        this.graphExportService = graphExportService;
    }

    public GraphExpandResponse expand(GraphExpandRequest request) {
        return graphExpansionService.expand(request);
    }

    public ShortestPathResponse shortestPath(ShortestPathRequest request) {
        return graphPathService.shortestPath(request);
    }

    public GraphExpandResponse query(GraphQueryRequest request) {
        return graphSqlQueryService.query(request);
    }

    public GraphImportResponse importPreview(MultipartFile file) {
        return graphImportService.preview(file);
    }

    public GraphImportResponse importCommit(MultipartFile file) {
        return graphImportService.commit(file);
    }

    public GraphDictionaryResponse dictionary() {
        return graphDictionaryService.dictionary();
    }

    public GraphNodeSummaryResponse nodeSummary(String nodeId,
                                                String relationFamily,
                                                Direction direction) {
        return graphNodeReadService.nodeSummary(nodeId, relationFamily, direction);
    }

    public GraphNodeSearchResponse searchNodes(String query,
                                               String nodeType,
                                               int limit,
                                               boolean includeAttributes) {
        return graphNodeReadService.searchNodes(query, nodeType, limit, includeAttributes);
    }

    public ExportedGraph export(GraphExportRequest request, GraphExportFormat format) {
        return graphExportService.export(request, format);
    }
}
