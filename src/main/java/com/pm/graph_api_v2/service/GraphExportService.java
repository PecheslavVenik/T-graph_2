package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.dto.GraphEdgeDto;
import com.pm.graph_api_v2.dto.GraphExportFormat;
import com.pm.graph_api_v2.dto.GraphExportRequest;
import com.pm.graph_api_v2.dto.GraphNodeDto;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Component
public class GraphExportService {

    private final ObjectMapper objectMapper;

    public GraphExportService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
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
}
