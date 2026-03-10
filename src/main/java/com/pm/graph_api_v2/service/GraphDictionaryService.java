package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.dto.GraphDictionaryResponse;
import com.pm.graph_api_v2.repository.GraphRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class GraphDictionaryService {

    private final GraphRepository graphRepository;

    public GraphDictionaryService(GraphRepository graphRepository) {
        this.graphRepository = graphRepository;
    }

    public GraphDictionaryResponse dictionary() {
        List<String> edgeTypes = graphRepository.findDistinctEdgeTypes();
        List<String> statuses = List.of("BLACKLIST", "VIP", "EMPLOYED");

        Map<String, String> styleHints = Map.of(
            "BLACKLIST", "node-color:#111111",
            "VIP", "node-color:#f39c12",
            "EMPLOYED", "node-border:#1f7a8c",
            "TRANSFER", "edge-style:bold-dashed",
            "TK_LINK", "edge-style:thin-dashed"
        );

        return new GraphDictionaryResponse(edgeTypes, statuses, styleHints);
    }
}
