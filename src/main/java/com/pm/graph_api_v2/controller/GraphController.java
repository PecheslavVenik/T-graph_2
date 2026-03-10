package com.pm.graph_api_v2.controller;

import com.pm.graph_api_v2.dto.GraphDictionaryResponse;
import com.pm.graph_api_v2.dto.GraphExpandRequest;
import com.pm.graph_api_v2.dto.GraphExpandResponse;
import com.pm.graph_api_v2.dto.GraphExportFormat;
import com.pm.graph_api_v2.dto.GraphExportRequest;
import com.pm.graph_api_v2.dto.ShortestPathRequest;
import com.pm.graph_api_v2.dto.ShortestPathResponse;
import com.pm.graph_api_v2.service.GraphDictionaryService;
import com.pm.graph_api_v2.service.GraphExpandService;
import com.pm.graph_api_v2.service.GraphExportService;
import com.pm.graph_api_v2.service.GraphPathService;
import jakarta.validation.Valid;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Validated
@RequestMapping("/api/v1/graph")
public class GraphController {

    private final GraphExpandService graphExpandService;
    private final GraphPathService graphPathService;
    private final GraphDictionaryService graphDictionaryService;
    private final GraphExportService graphExportService;

    public GraphController(GraphExpandService graphExpandService,
                           GraphPathService graphPathService,
                           GraphDictionaryService graphDictionaryService,
                           GraphExportService graphExportService) {
        this.graphExpandService = graphExpandService;
        this.graphPathService = graphPathService;
        this.graphDictionaryService = graphDictionaryService;
        this.graphExportService = graphExportService;
    }

    @PostMapping("/expand")
    public GraphExpandResponse expand(@Valid @RequestBody GraphExpandRequest request) {
        return graphExpandService.expand(request);
    }

    @PostMapping("/shortest-path")
    public ShortestPathResponse shortestPath(@Valid @RequestBody ShortestPathRequest request) {
        return graphPathService.shortestPath(request);
    }

    @GetMapping("/dictionary")
    public GraphDictionaryResponse dictionary() {
        return graphDictionaryService.dictionary();
    }

    @PostMapping("/export")
    public ResponseEntity<byte[]> export(@Valid @RequestBody GraphExportRequest request,
                                         @RequestParam(defaultValue = "JSON") GraphExportFormat format) {
        GraphExportService.ExportedGraph exported = graphExportService.export(request, format);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(org.springframework.http.MediaType.parseMediaType(exported.contentType()));
        headers.setContentDisposition(ContentDisposition.attachment().filename(exported.fileName()).build());

        return ResponseEntity.ok()
            .headers(headers)
            .body(exported.payload());
    }
}
