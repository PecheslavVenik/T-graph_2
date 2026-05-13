package com.pm.graph_api_v2.controller;

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
import com.pm.graph_api_v2.service.ExportedGraph;
import com.pm.graph_api_v2.service.InvestigationService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@Validated
@RequestMapping("/api/v1/graph")
public class GraphController {

    private final InvestigationService investigationService;

    public GraphController(InvestigationService investigationService) {
        this.investigationService = investigationService;
    }

    @PostMapping("/expand")
    public GraphExpandResponse expand(@Valid @RequestBody GraphExpandRequest request) {
        return investigationService.expand(request);
    }

    @PostMapping("/shortest-path")
    public ShortestPathResponse shortestPath(@Valid @RequestBody ShortestPathRequest request) {
        return investigationService.shortestPath(request);
    }

    @PostMapping("/query")
    public GraphExpandResponse query(@Valid @RequestBody GraphQueryRequest request) {
        return investigationService.query(request);
    }

    @PostMapping(value = "/import/preview", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public GraphImportResponse importPreview(@RequestParam("file") MultipartFile file) {
        return investigationService.importPreview(file);
    }

    @PostMapping(value = "/import/commit", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public GraphImportResponse importCommit(@RequestParam("file") MultipartFile file) {
        return investigationService.importCommit(file);
    }

    @GetMapping("/dictionary")
    public GraphDictionaryResponse dictionary() {
        return investigationService.dictionary();
    }

    @GetMapping("/node-summary")
    public GraphNodeSummaryResponse nodeSummary(@RequestParam @NotBlank String nodeId,
                                                @RequestParam(required = false) String relationFamily,
                                                @RequestParam(defaultValue = "BOTH") Direction direction) {
        return investigationService.nodeSummary(nodeId, relationFamily, direction);
    }

    @GetMapping("/nodes/search")
    public GraphNodeSearchResponse searchNodes(@RequestParam String query,
                                               @RequestParam(required = false) String nodeType,
                                               @RequestParam(defaultValue = "20") int limit,
                                               @RequestParam(defaultValue = "true") boolean includeAttributes) {
        return investigationService.searchNodes(query, nodeType, limit, includeAttributes);
    }

    @PostMapping("/export")
    public ResponseEntity<byte[]> export(@Valid @RequestBody GraphExportRequest request,
                                         @RequestParam(defaultValue = "JSON") GraphExportFormat format) {
        ExportedGraph exported = investigationService.export(request, format);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(org.springframework.http.MediaType.parseMediaType(exported.contentType()));
        headers.setContentDisposition(ContentDisposition.attachment().filename(exported.fileName()).build());

        return ResponseEntity.ok()
            .headers(headers)
            .body(exported.payload());
    }
}
