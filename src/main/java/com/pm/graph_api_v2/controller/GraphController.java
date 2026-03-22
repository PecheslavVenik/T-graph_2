package com.pm.graph_api_v2.controller;

import com.pm.graph_api_v2.dto.GraphDictionaryResponse;
import com.pm.graph_api_v2.dto.GraphExpandRequest;
import com.pm.graph_api_v2.dto.GraphExpandResponse;
import com.pm.graph_api_v2.dto.GraphExportFormat;
import com.pm.graph_api_v2.dto.GraphExportRequest;
import com.pm.graph_api_v2.dto.ShortestPathRequest;
import com.pm.graph_api_v2.dto.ShortestPathResponse;
import com.pm.graph_api_v2.service.InvestigationService;
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

    @GetMapping("/dictionary")
    public GraphDictionaryResponse dictionary() {
        return investigationService.dictionary();
    }

    @PostMapping("/export")
    public ResponseEntity<byte[]> export(@Valid @RequestBody GraphExportRequest request,
                                         @RequestParam(defaultValue = "JSON") GraphExportFormat format) {
        InvestigationService.ExportedGraph exported = investigationService.export(request, format);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(org.springframework.http.MediaType.parseMediaType(exported.contentType()));
        headers.setContentDisposition(ContentDisposition.attachment().filename(exported.fileName()).build());

        return ResponseEntity.ok()
            .headers(headers)
            .body(exported.payload());
    }
}
