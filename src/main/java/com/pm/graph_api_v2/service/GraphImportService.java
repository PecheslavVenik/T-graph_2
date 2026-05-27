package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.dto.GraphImportErrorDto;
import com.pm.graph_api_v2.dto.GraphImportResponse;
import com.pm.graph_api_v2.exception.ApiBadRequestException;
import com.pm.graph_api_v2.repository.DuckPgqRuntimeManager;
import com.pm.graph_api_v2.repository.GraphImportWriter;
import com.pm.graph_api_v2.repository.GraphNodeRepository;
import com.pm.graph_api_v2.repository.model.ImportEdgeRow;
import com.pm.graph_api_v2.repository.model.ImportGraphData;
import com.pm.graph_api_v2.repository.model.ImportNodeRow;
import com.pm.graph_api_v2.repository.model.ImportWriteResult;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.util.GraphRelationFamilies;
import com.pm.graph_api_v2.util.StableIdUtil;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
public class GraphImportService {

    private static final int MAX_FILE_BYTES = 10 * 1024 * 1024;
    private static final int MAX_ROWS = 50_000;
    private static final int MAX_ERRORS = 100;
    private static final int SAMPLE_ROWS = 5;
    private static final Set<String> NODE_COLUMNS = Set.of(
        "record_type", "node_id", "id", "node_type", "entity_type", "display_name", "name",
        "party_rk", "person_id", "phone_no", "phone", "full_name", "is_blacklist", "blacklist",
        "is_vip", "vip", "employer", "city", "source_system", "pagerank_score", "hub_score", "attrs_json"
    );
    private static final Set<String> EDGE_COLUMNS = Set.of(
        "record_type", "edge_id", "from_node_id", "source", "from", "to_node_id", "target", "to",
        "edge_type", "type", "relation", "relation_family", "directed", "tx_count", "tx_sum",
        "strength_score", "evidence_count", "source_system", "first_seen_at", "last_seen_at", "attrs_json"
    );
    private static final Map<String, String> IDENTIFIER_COLUMNS = Map.ofEntries(
        Map.entry("party_rk", "PARTY_RK"),
        Map.entry("person_id", "PERSON_ID"),
        Map.entry("phone_no", "PHONE_NO"),
        Map.entry("phone", "PHONE_NO"),
        Map.entry("account_no", "ACCOUNT_NO"),
        Map.entry("card_mask", "CARD_MASK"),
        Map.entry("tax_id", "TAX_ID"),
        Map.entry("device_id", "DEVICE_ID"),
        Map.entry("ip", "IP")
    );

    private final GraphNodeRepository nodeRepository;
    private final GraphImportWriter importWriter;
    private final ObjectMapper objectMapper;
    private final ObjectProvider<DuckPgqRuntimeManager> duckPgqRuntimeManager;

    public GraphImportService(GraphNodeRepository nodeRepository,
                              GraphImportWriter importWriter,
                              ObjectMapper objectMapper,
                              ObjectProvider<DuckPgqRuntimeManager> duckPgqRuntimeManager) {
        this.nodeRepository = nodeRepository;
        this.importWriter = importWriter;
        this.objectMapper = objectMapper;
        this.duckPgqRuntimeManager = duckPgqRuntimeManager;
    }

    public GraphImportResponse preview(MultipartFile file) {
        ParsedImport parsed = parse(file);
        return response(fileName(file), "PREVIEW", parsed, new ImportWriteResult(0, 0, 0, 0), parsed.warnings());
    }

    public GraphImportResponse commit(MultipartFile file) {
        ParsedImport parsed = parse(file);
        if (!parsed.errors().isEmpty()) {
            throw new ApiBadRequestException("Import file has validation errors; call preview first", parsed.errors());
        }

        ImportWriteResult writeResult = importWriter.importGraph(new ImportGraphData(parsed.nodes(), parsed.edges()));
        duckPgqRuntimeManager.ifAvailable(DuckPgqRuntimeManager::syncGraphState);

        return response(fileName(file), "COMMITTED", parsed, writeResult, parsed.warnings());
    }

    private ParsedImport parse(MultipartFile file) {
        validateFile(file);
        String content = readContent(file);
        List<List<String>> records = parseCsv(content);
        if (records.isEmpty()) {
            throw new ApiBadRequestException("CSV file must contain a header row");
        }
        if (records.size() - 1 > MAX_ROWS) {
            throw new ApiBadRequestException("CSV file has too many rows; max is " + MAX_ROWS);
        }

        List<String> headers = normalizeHeaders(records.get(0));
        Map<String, ImportNodeRow> nodesById = new LinkedHashMap<>();
        Map<String, ImportEdgeRow> edgesById = new LinkedHashMap<>();
        List<GraphImportErrorDto> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        List<Map<String, String>> sampleRows = new ArrayList<>();

        for (int index = 1; index < records.size(); index++) {
            List<String> record = records.get(index);
            int rowNumber = index + 1;
            if (record.size() > headers.size()) {
                addError(errors, rowNumber, "ROW", "CSV row has more values than header columns");
                continue;
            }

            Map<String, String> row = toRow(headers, record);
            if (isEmptyRow(row)) {
                continue;
            }
            if (sampleRows.size() < SAMPLE_ROWS) {
                sampleRows.add(row);
            }

            String recordType = value(row, "record_type").toUpperCase(Locale.ROOT);
            if (!recordType.isBlank() && !"NODE".equals(recordType) && !"EDGE".equals(recordType)) {
                addError(errors, rowNumber, "ROW", "record_type must be NODE or EDGE");
                continue;
            }

            boolean edgeRow = "EDGE".equals(recordType)
                || (recordType.isBlank() && hasAny(row, "from_node_id", "source", "from", "to_node_id", "target", "to"));

            if (edgeRow) {
                parseEdgeRow(row, rowNumber, edgesById, errors);
            } else {
                parseNodeRow(row, rowNumber, nodesById, errors);
            }
        }

        int inferredNodeCount = inferMissingEndpointNodes(nodesById, edgesById.values());
        if (inferredNodeCount > 0) {
            warnings.add("Created " + inferredNodeCount + " inferred endpoint node(s) from imported edges");
        }

        return new ParsedImport(
            new ArrayList<>(nodesById.values()),
            new ArrayList<>(edgesById.values()),
            inferredNodeCount,
            errors,
            warnings,
            sampleRows
        );
    }

    private void parseNodeRow(Map<String, String> row,
                              int rowNumber,
                              Map<String, ImportNodeRow> nodesById,
                              List<GraphImportErrorDto> errors) {
        String nodeId = first(row, "node_id", "id");
        if (nodeId == null) {
            addError(errors, rowNumber, "NODE", "node_id is required");
            return;
        }

        Map<String, String> identifiers = identifiers(row);
        String nodeType = firstOrDefault(row, "PERSON", "node_type", "entity_type");
        String displayName = firstOrDefault(row, nodeId, "display_name", "name", "full_name", "party_rk", "person_id", "phone_no", "phone");
        String phoneNo = first(row, "phone_no", "phone");
        ParsedValue<Double> pagerankScore = parseDouble(row, "pagerank_score", 0, rowNumber, "NODE", errors);
        ParsedValue<Double> hubScore = parseDouble(row, "hub_score", 0, rowNumber, "NODE", errors);
        ParsedValue<Boolean> blacklist = parseBoolean(row, false, rowNumber, "NODE", errors, "is_blacklist", "blacklist");
        ParsedValue<Boolean> vip = parseBoolean(row, false, rowNumber, "NODE", errors, "is_vip", "vip");
        if (!pagerankScore.valid() || !hubScore.valid() || !blacklist.valid() || !vip.valid()) {
            return;
        }

        nodesById.put(nodeId, new ImportNodeRow(
            nodeId,
            normalizeUpper(nodeType, "PERSON"),
            displayName,
            first(row, "party_rk"),
            first(row, "person_id"),
            phoneNo,
            first(row, "full_name"),
            blacklist.value(),
            vip.value(),
            first(row, "employer"),
            first(row, "city"),
            firstOrDefault(row, "file_import", "source_system"),
            pagerankScore.value(),
            hubScore.value(),
            identifiers,
            attributes(row, NODE_COLUMNS)
        ));
    }

    private void parseEdgeRow(Map<String, String> row,
                              int rowNumber,
                              Map<String, ImportEdgeRow> edgesById,
                              List<GraphImportErrorDto> errors) {
        String fromNodeId = first(row, "from_node_id", "source", "from");
        String toNodeId = first(row, "to_node_id", "target", "to");
        String edgeType = first(row, "edge_type", "type", "relation");
        if (fromNodeId == null) {
            addError(errors, rowNumber, "EDGE", "from_node_id/source is required");
            return;
        }
        if (toNodeId == null) {
            addError(errors, rowNumber, "EDGE", "to_node_id/target is required");
            return;
        }
        if (edgeType == null) {
            addError(errors, rowNumber, "EDGE", "edge_type/type/relation is required");
            return;
        }

        String normalizedEdgeType = normalizeUpper(edgeType, "RELATED");
        String relationFamily = GraphRelationFamilies.normalize(first(row, "relation_family"));
        if (relationFamily == null) {
            relationFamily = GraphRelationFamilies.ALL_RELATIONS;
        }
        ParsedValue<Boolean> parsedDirected = parseBoolean(row, true, rowNumber, "EDGE", errors, "directed");
        String edgeId = first(row, "edge_id");
        if (edgeId == null) {
            edgeId = StableIdUtil.stableEdgeId(null, fromNodeId, toNodeId, normalizedEdgeType, parsedDirected.value(), relationFamily);
        }

        ParsedValue<Long> txCount = parseLong(row, "tx_count", 0, rowNumber, "EDGE", errors);
        ParsedValue<Double> txSum = parseDouble(row, "tx_sum", 0, rowNumber, "EDGE", errors);
        ParsedValue<Double> strengthScore = parseDouble(row, "strength_score", 0, rowNumber, "EDGE", errors);
        ParsedValue<Long> evidenceCount = parseLong(row, "evidence_count", 0, rowNumber, "EDGE", errors);
        ParsedValue<Instant> firstSeenAt = parseInstant(row, "first_seen_at", rowNumber, "EDGE", errors);
        ParsedValue<Instant> lastSeenAt = parseInstant(row, "last_seen_at", rowNumber, "EDGE", errors);
        if (!parsedDirected.valid()
            || !txCount.valid()
            || !txSum.valid()
            || !strengthScore.valid()
            || !evidenceCount.valid()
            || !firstSeenAt.valid()
            || !lastSeenAt.valid()) {
            return;
        }

        edgesById.put(edgeId, new ImportEdgeRow(
            edgeId,
            fromNodeId,
            toNodeId,
            normalizedEdgeType,
            parsedDirected.value(),
            txCount.value(),
            txSum.value(),
            relationFamily,
            strengthScore.value(),
            evidenceCount.value(),
            firstOrDefault(row, "file_import", "source_system"),
            firstSeenAt.value(),
            lastSeenAt.value(),
            attributes(row, EDGE_COLUMNS)
        ));
    }

    private int inferMissingEndpointNodes(Map<String, ImportNodeRow> nodesById, Collection<ImportEdgeRow> edges) {
        Set<String> endpointIds = new LinkedHashSet<>();
        for (ImportEdgeRow edge : edges) {
            endpointIds.add(edge.fromNodeId());
            endpointIds.add(edge.toNodeId());
        }
        endpointIds.removeAll(nodesById.keySet());
        if (endpointIds.isEmpty()) {
            return 0;
        }

        Set<String> existingIds = new HashSet<>();
        for (NodeRow existingNode : nodeRepository.findNodesByIds(endpointIds)) {
            existingIds.add(existingNode.nodeId());
        }

        int inferred = 0;
        for (String endpointId : endpointIds) {
            if (existingIds.contains(endpointId)) {
                continue;
            }
            nodesById.put(endpointId, new ImportNodeRow(
                endpointId,
                "UNKNOWN",
                endpointId,
                null,
                null,
                null,
                null,
                false,
                false,
                null,
                null,
                "file_import",
                0,
                0,
                Map.of(),
                Map.of("inferred", true)
            ));
            inferred++;
        }
        return inferred;
    }

    private GraphImportResponse response(String fileName,
                                         String status,
                                         ParsedImport parsed,
                                         ImportWriteResult writeResult,
                                         List<String> warnings) {
        return new GraphImportResponse(
            fileName,
            status,
            parsed.nodes().size(),
            parsed.edges().size(),
            parsed.inferredNodeCount(),
            invalidRowCount(parsed.errors()),
            writeResult.insertedNodeCount(),
            writeResult.updatedNodeCount(),
            writeResult.insertedEdgeCount(),
            writeResult.updatedEdgeCount(),
            parsed.errors(),
            warnings,
            parsed.sampleRows()
        );
    }

    private void validateFile(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new ApiBadRequestException("CSV file is required");
        }
        if (file.getSize() > MAX_FILE_BYTES) {
            throw new ApiBadRequestException("CSV file is too large; max is " + MAX_FILE_BYTES + " bytes");
        }
        String fileName = fileName(file).toLowerCase(Locale.ROOT);
        if (!fileName.endsWith(".csv")) {
            throw new ApiBadRequestException("Only CSV import is supported");
        }
    }

    private String readContent(MultipartFile file) {
        try {
            String content = new String(file.getBytes(), StandardCharsets.UTF_8);
            if (content.startsWith("\uFEFF")) {
                return content.substring(1);
            }
            return content;
        } catch (Exception ex) {
            throw new ApiBadRequestException("Failed to read CSV file");
        }
    }

    private List<List<String>> parseCsv(String content) {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;
        boolean quoteClosed = false;

        for (int index = 0; index < content.length(); index++) {
            char ch = content.charAt(index);
            if (quoted) {
                if (ch == '"') {
                    if (index + 1 < content.length() && content.charAt(index + 1) == '"') {
                        cell.append('"');
                        index++;
                    } else {
                        quoted = false;
                        quoteClosed = true;
                    }
                } else {
                    cell.append(ch);
                }
            } else if (quoteClosed) {
                if (ch == ',') {
                    row.add(cell.toString());
                    cell.setLength(0);
                    quoteClosed = false;
                } else if (ch == '\n') {
                    row.add(cell.toString());
                    rows.add(row);
                    row = new ArrayList<>();
                    cell.setLength(0);
                    quoteClosed = false;
                } else if (ch == '\r') {
                    row.add(cell.toString());
                    rows.add(row);
                    row = new ArrayList<>();
                    cell.setLength(0);
                    quoteClosed = false;
                    if (index + 1 < content.length() && content.charAt(index + 1) == '\n') {
                        index++;
                    }
                } else if (!Character.isWhitespace(ch)) {
                    throw new ApiBadRequestException("Malformed CSV: unexpected character after closing quote");
                }
            } else if (ch == '"') {
                if (cell.length() > 0) {
                    throw new ApiBadRequestException("Malformed CSV: quote must start a quoted value");
                }
                quoted = true;
            } else if (ch == ',') {
                row.add(cell.toString());
                cell.setLength(0);
            } else if (ch == '\n') {
                row.add(cell.toString());
                rows.add(row);
                row = new ArrayList<>();
                cell.setLength(0);
            } else if (ch == '\r') {
                row.add(cell.toString());
                rows.add(row);
                row = new ArrayList<>();
                cell.setLength(0);
                if (index + 1 < content.length() && content.charAt(index + 1) == '\n') {
                    index++;
                }
            } else {
                cell.append(ch);
            }
        }
        if (quoted) {
            throw new ApiBadRequestException("Malformed CSV: unclosed quoted value");
        }
        row.add(cell.toString());
        if (!isCsvRowEmpty(row)) {
            rows.add(row);
        }
        return rows;
    }

    private List<String> normalizeHeaders(List<String> rawHeaders) {
        List<String> headers = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String rawHeader : rawHeaders) {
            String normalized = normalizeColumn(rawHeader);
            if (normalized.isBlank()) {
                throw new ApiBadRequestException("CSV header contains a blank column name");
            }
            if (!seen.add(normalized)) {
                throw new ApiBadRequestException("CSV header contains duplicate column: " + normalized);
            }
            headers.add(normalized);
        }
        return headers;
    }

    private Map<String, String> toRow(List<String> headers, List<String> record) {
        Map<String, String> row = new LinkedHashMap<>();
        for (int index = 0; index < headers.size(); index++) {
            String value = index < record.size() ? record.get(index).trim() : "";
            row.put(headers.get(index), value);
        }
        return row;
    }

    private Map<String, String> identifiers(Map<String, String> row) {
        Map<String, String> identifiers = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : row.entrySet()) {
            String column = entry.getKey();
            String value = trimToNull(entry.getValue());
            if (value == null) {
                continue;
            }
            if (IDENTIFIER_COLUMNS.containsKey(column)) {
                identifiers.put(IDENTIFIER_COLUMNS.get(column), value);
            } else if (column.startsWith("identifier_")) {
                identifiers.put(column.substring("identifier_".length()).toUpperCase(Locale.ROOT), value);
            }
        }
        return identifiers;
    }

    private Map<String, Object> attributes(Map<String, String> row, Set<String> canonicalColumns) {
        Map<String, Object> attrs = new LinkedHashMap<>();
        String rawAttrs = trimToNull(row.get("attrs_json"));
        if (rawAttrs != null) {
            attrs.putAll(readAttrsJson(rawAttrs));
        }
        for (Map.Entry<String, String> entry : row.entrySet()) {
            String column = entry.getKey();
            String value = trimToNull(entry.getValue());
            if (value == null || canonicalColumns.contains(column) || IDENTIFIER_COLUMNS.containsKey(column) || column.startsWith("identifier_")) {
                continue;
            }
            attrs.put(column, value);
        }
        return attrs;
    }

    private Map<String, Object> readAttrsJson(String rawJson) {
        try {
            return objectMapper.readValue(rawJson, new TypeReference<>() {
            });
        } catch (Exception ignored) {
            return Map.of("attrs_json", rawJson);
        }
    }

    private boolean hasAny(Map<String, String> row, String... columns) {
        for (String column : columns) {
            if (trimToNull(row.get(column)) != null) {
                return true;
            }
        }
        return false;
    }

    private String first(Map<String, String> row, String... columns) {
        for (String column : columns) {
            String value = trimToNull(row.get(column));
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String firstOrDefault(Map<String, String> row, String fallback, String... columns) {
        String value = first(row, columns);
        return value == null ? fallback : value;
    }

    private String normalizeUpper(String value, String fallback) {
        String normalized = trimToNull(value);
        if (normalized == null) {
            return fallback;
        }
        return normalized.toUpperCase(Locale.ROOT);
    }

    private String value(Map<String, String> row, String column) {
        return row.getOrDefault(column, "").trim();
    }

    private boolean isEmptyRow(Map<String, String> row) {
        return row.values().stream().allMatch(value -> value == null || value.isBlank());
    }

    private boolean isCsvRowEmpty(List<String> row) {
        return row.stream().allMatch(value -> value == null || value.isBlank());
    }

    private void addError(List<GraphImportErrorDto> errors, int rowNumber, String section, String message) {
        if (errors.size() < MAX_ERRORS) {
            errors.add(new GraphImportErrorDto(rowNumber, section, message));
        }
    }

    private void addFieldError(List<GraphImportErrorDto> errors,
                               int rowNumber,
                               String section,
                               String field,
                               String value,
                               String message) {
        if (errors.size() < MAX_ERRORS) {
            errors.add(new GraphImportErrorDto(rowNumber, section, field, truncateValue(value), message));
        }
    }

    private int invalidRowCount(List<GraphImportErrorDto> errors) {
        return (int) errors.stream()
            .map(GraphImportErrorDto::rowNumber)
            .distinct()
            .count();
    }

    private String normalizeColumn(String raw) {
        return raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
    }

    private String trimToNull(String value) {
        if (value == null || value.trim().isBlank()) {
            return null;
        }
        return value.trim();
    }

    private ParsedValue<Boolean> parseBoolean(Map<String, String> row,
                                              boolean fallback,
                                              int rowNumber,
                                              String section,
                                              List<GraphImportErrorDto> errors,
                                              String... fields) {
        String field = firstPresentField(row, fields);
        if (field == null) {
            return ParsedValue.valid(fallback);
        }

        String value = row.get(field);
        String normalized = trimToNull(value);
        if (normalized == null) {
            return ParsedValue.valid(fallback);
        }

        String lower = normalized.toLowerCase(Locale.ROOT);
        if ("true".equals(lower) || "1".equals(lower) || "yes".equals(lower)) {
            return ParsedValue.valid(true);
        }
        if ("false".equals(lower) || "0".equals(lower) || "no".equals(lower)) {
            return ParsedValue.valid(false);
        }

        addFieldError(errors, rowNumber, section, field, value, field + " must be a boolean");
        return ParsedValue.invalid(fallback);
    }

    private String firstPresentField(Map<String, String> row, String... fields) {
        for (String field : fields) {
            if (trimToNull(row.get(field)) != null) {
                return field;
            }
        }
        return null;
    }

    private ParsedValue<Long> parseLong(Map<String, String> row,
                                        String field,
                                        long fallback,
                                        int rowNumber,
                                        String section,
                                        List<GraphImportErrorDto> errors) {
        String value = first(row, field);
        if (value == null) {
            return ParsedValue.valid(fallback);
        }
        try {
            return ParsedValue.valid(Long.parseLong(value.trim()));
        } catch (NumberFormatException ex) {
            addFieldError(errors, rowNumber, section, field, value, field + " must be a whole number");
            return ParsedValue.invalid(fallback);
        }
    }

    private ParsedValue<Double> parseDouble(Map<String, String> row,
                                            String field,
                                            double fallback,
                                            int rowNumber,
                                            String section,
                                            List<GraphImportErrorDto> errors) {
        String value = first(row, field);
        if (value == null) {
            return ParsedValue.valid(fallback);
        }
        try {
            double parsed = Double.parseDouble(value.trim());
            if (!Double.isFinite(parsed)) {
                addFieldError(errors, rowNumber, section, field, value, field + " must be a finite number");
                return ParsedValue.invalid(fallback);
            }
            return ParsedValue.valid(parsed);
        } catch (NumberFormatException ex) {
            addFieldError(errors, rowNumber, section, field, value, field + " must be a finite number");
            return ParsedValue.invalid(fallback);
        }
    }

    private ParsedValue<Instant> parseInstant(Map<String, String> row,
                                              String field,
                                              int rowNumber,
                                              String section,
                                              List<GraphImportErrorDto> errors) {
        String value = first(row, field);
        if (value == null) {
            return ParsedValue.valid(null);
        }
        try {
            return ParsedValue.valid(Instant.parse(value.trim()));
        } catch (Exception ex) {
            addFieldError(errors, rowNumber, section, field, value, field + " must be an ISO-8601 instant");
            return ParsedValue.invalid(null);
        }
    }

    private String truncateValue(String value) {
        if (value == null || value.length() <= 128) {
            return value;
        }
        return value.substring(0, 128) + "...";
    }

    private String fileName(MultipartFile file) {
        String originalName = file.getOriginalFilename();
        return originalName == null || originalName.isBlank() ? "graph-import.csv" : originalName;
    }

    private record ParsedImport(
        List<ImportNodeRow> nodes,
        List<ImportEdgeRow> edges,
        int inferredNodeCount,
        List<GraphImportErrorDto> errors,
        List<String> warnings,
        List<Map<String, String>> sampleRows
    ) {
    }

    private record ParsedValue<T>(T value, boolean valid) {
        private static <T> ParsedValue<T> valid(T value) {
            return new ParsedValue<>(value, true);
        }

        private static <T> ParsedValue<T> invalid(T fallback) {
            return new ParsedValue<>(fallback, false);
        }
    }
}
