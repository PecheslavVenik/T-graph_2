package com.pm.graph_api_v2.repository;

import com.kuzudb.Connection;
import com.kuzudb.QueryResult;
import com.pm.graph_api_v2.config.KuzuProperties;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "KUZU")
public class KuzuRuntimeManager {

    private static final Logger log = LoggerFactory.getLogger(KuzuRuntimeManager.class);
    private static final int SYNC_BATCH_SIZE = 10_000;

    private final Connection connection;
    private final KuzuProperties properties;
    private final GraphRepository graphRepository;

    public KuzuRuntimeManager(Connection connection,
                              KuzuProperties properties,
                              GraphRepository graphRepository) {
        this.connection = connection;
        this.properties = properties;
        this.graphRepository = graphRepository;
    }

    public void initialize() {
        if (properties.isClearProjectionOnStartup()) {
            dropSchema();
        }
        ensureSchema();
        if (!properties.isSyncGraphStateOnStartup()) {
            log.info("kuzu graph-state synchronization is skipped at startup");
            return;
        }
        bulkSync();
    }

    public boolean isAvailable() {
        try {
            execute("RETURN 1 AS ok");
            return true;
        } catch (Exception ex) {
            log.debug("Kuzu connectivity check failed: {}", ex.getMessage());
            return false;
        }
    }

    private void ensureSchema() {
        executeIgnoringAlreadyExists("""
            CREATE NODE TABLE GraphNode(
                node_id STRING,
                projection_owner STRING,
                node_type STRING,
                display_name STRING,
                source_system STRING,
                pagerank_score DOUBLE,
                hub_score DOUBLE,
                PRIMARY KEY(node_id)
            )
            """);
        executeIgnoringAlreadyExists("""
            CREATE REL TABLE CONNECTED(
                FROM GraphNode TO GraphNode,
                graph_rel_id STRING,
                projection_owner STRING,
                edge_id STRING,
                from_node_id STRING,
                to_node_id STRING,
                traversal_from_node_id STRING,
                traversal_to_node_id STRING,
                edge_type STRING,
                directed BOOL,
                tx_count INT64,
                tx_sum DOUBLE,
                relation_family STRING,
                strength_score DOUBLE,
                evidence_count INT64,
                source_system STRING,
                first_seen_at STRING,
                last_seen_at STRING,
                attrs_json STRING
            )
            """);
    }

    private void dropSchema() {
        executeIgnoringFailure("DROP TABLE CONNECTED");
        executeIgnoringFailure("DROP TABLE GraphNode");
    }

    private void bulkSync() {
        Path importDirectory;
        try {
            importDirectory = Files.createTempDirectory(importBasePath(), "kuzu-import-");
            Path nodesCsv = importDirectory.resolve("graph_nodes.csv");
            Path edgesCsv = importDirectory.resolve("graph_edges.csv");
            writeNodesCsv(nodesCsv);
            writeEdgesCsv(edgesCsv);
            copyTable("GraphNode", nodesCsv);
            copyTable("CONNECTED", edgesCsv);
        } catch (IOException ex) {
            throw new IllegalStateException("Kuzu projection CSV generation failed", ex);
        }
    }

    private Path importBasePath() throws IOException {
        Path databasePath = Path.of(properties.getPath()).toAbsolutePath();
        Path parent = databasePath.getParent();
        if (parent == null) {
            parent = Path.of(".").toAbsolutePath();
        }
        Files.createDirectories(parent);
        return parent;
    }

    private void writeNodesCsv(Path path) throws IOException {
        AtomicLong synced = new AtomicLong();
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writeCsvRow(
                writer,
                "node_id",
                "projection_owner",
                "node_type",
                "display_name",
                "source_system",
                "pagerank_score",
                "hub_score"
            );
            graphRepository.forEachNodeBatch(SYNC_BATCH_SIZE, nodes -> {
                try {
                    for (NodeRow node : nodes) {
                        writeCsvRow(
                            writer,
                            node.nodeId(),
                            Neo4jProjectionSupport.PROJECTION_OWNER,
                            node.nodeType(),
                            node.displayName(),
                            node.sourceSystem(),
                            Double.toString(node.pagerankScore()),
                            Double.toString(node.hubScore())
                        );
                    }
                } catch (IOException ex) {
                    throw new IllegalStateException("Kuzu node CSV write failed", ex);
                }
                long total = synced.addAndGet(nodes.size());
                if (total % 50_000 == 0) {
                    log.info("kuzu projection nodes exported: {}", total);
                }
            });
        }
        log.info("kuzu projection nodes exported: {}", synced.get());
    }

    private void writeEdgesCsv(Path path) throws IOException {
        AtomicLong sourceEdges = new AtomicLong();
        AtomicLong projected = new AtomicLong();
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writeCsvRow(
                writer,
                "from",
                "to",
                "graph_rel_id",
                "projection_owner",
                "edge_id",
                "from_node_id",
                "to_node_id",
                "traversal_from_node_id",
                "traversal_to_node_id",
                "edge_type",
                "directed",
                "tx_count",
                "tx_sum",
                "relation_family",
                "strength_score",
                "evidence_count",
                "source_system",
                "first_seen_at",
                "last_seen_at",
                "attrs_json"
            );
            graphRepository.forEachEdgeBatch(SYNC_BATCH_SIZE, edges -> {
                try {
                    for (EdgeRow edge : edges) {
                        writeEdgeCsvRow(writer, edge, false);
                        projected.incrementAndGet();
                        if (!edge.directed()) {
                            writeEdgeCsvRow(writer, edge, true);
                            projected.incrementAndGet();
                        }
                    }
                } catch (IOException ex) {
                    throw new IllegalStateException("Kuzu edge CSV write failed", ex);
                }
                long total = sourceEdges.addAndGet(edges.size());
                if (total % 50_000 == 0) {
                    log.info("kuzu projection source edges exported: {}, relationships exported: {}", total, projected.get());
                }
            });
        }
        log.info("kuzu projection source edges exported: {}, relationships exported: {}", sourceEdges.get(), projected.get());
    }

    private void writeEdgeCsvRow(BufferedWriter writer, EdgeRow edge, boolean reverseTraversal) throws IOException {
        String graphRelId = edge.edgeId() + (reverseTraversal ? ":rev" : ":fwd");
        String traversalFrom = reverseTraversal ? edge.toNodeId() : edge.fromNodeId();
        String traversalTo = reverseTraversal ? edge.fromNodeId() : edge.toNodeId();
        writeCsvRow(
            writer,
            traversalFrom,
            traversalTo,
            graphRelId,
            Neo4jProjectionSupport.PROJECTION_OWNER,
            edge.edgeId(),
            edge.fromNodeId(),
            edge.toNodeId(),
            traversalFrom,
            traversalTo,
            edge.edgeType(),
            Boolean.toString(edge.directed()),
            Long.toString(edge.txCount()),
            Double.toString(edge.txSum()),
            edge.relationFamily(),
            Double.toString(edge.strengthScore()),
            Long.toString(edge.evidenceCount()),
            edge.sourceSystem(),
            edge.firstSeenAt() == null ? null : edge.firstSeenAt().toString(),
            edge.lastSeenAt() == null ? null : edge.lastSeenAt().toString(),
            null
        );
    }

    private void copyTable(String table, Path csvPath) {
        execute("COPY " + table + " FROM " + literal(csvPath.toAbsolutePath().toString()) + " (header=true)");
        log.info("kuzu table {} imported from {}", table, csvPath);
    }

    private void writeCsvRow(BufferedWriter writer, String... values) throws IOException {
        for (int index = 0; index < values.length; index++) {
            if (index > 0) {
                writer.write(',');
            }
            writeCsvValue(writer, values[index]);
        }
        writer.newLine();
    }

    private void writeCsvValue(BufferedWriter writer, String value) throws IOException {
        if (value == null) {
            return;
        }
        writer.write('"');
        for (int index = 0; index < value.length(); index++) {
            char ch = value.charAt(index);
            if (ch == '"') {
                writer.write("\"\"");
            } else {
                writer.write(ch);
            }
        }
        writer.write('"');
    }

    synchronized QueryResult query(String sql) {
        QueryResult result = connection.query(sql);
        if (!result.isSuccess()) {
            String error = result.getErrorMessage();
            result.close();
            throw new IllegalStateException("Kuzu query failed: " + error + "\n" + sql);
        }
        return result;
    }

    synchronized void execute(String sql) {
        try (QueryResult result = query(sql)) {
            while (result.hasNext()) {
                result.getNext();
            }
        }
    }

    private void executeIgnoringAlreadyExists(String sql) {
        try {
            execute(sql);
        } catch (Exception ex) {
            if (!ex.getMessage().toLowerCase().contains("already exists")) {
                throw ex;
            }
        }
    }

    private void executeIgnoringFailure(String sql) {
        try {
            execute(sql);
        } catch (Exception ex) {
            log.debug("Ignoring Kuzu cleanup failure for '{}': {}", sql, ex.getMessage());
        }
    }

    static String literal(String value) {
        if (value == null) {
            return "NULL";
        }
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}
