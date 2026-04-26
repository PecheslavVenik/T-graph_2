package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.MemgraphProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "MEMGRAPH")
public class MemgraphGraphQueryBackend implements GraphQueryBackend {

    private final Driver driver;
    private final Neo4jProjectionSupport projectionSupport;
    private final MemgraphProperties memgraphProperties;

    public MemgraphGraphQueryBackend(Driver driver,
                                     Neo4jProjectionSupport projectionSupport,
                                     MemgraphProperties memgraphProperties) {
        this.driver = driver;
        this.projectionSupport = projectionSupport;
        this.memgraphProperties = memgraphProperties;
    }

    @Override
    public GraphSource source() {
        return GraphSource.MEMGRAPH;
    }

    @Override
    public List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                         String relationFamily,
                                         List<String> edgeTypes,
                                         Direction direction,
                                         int candidateLimit) {
        if (seedNodeIds.isEmpty()) {
            return List.of();
        }

        String query = Neo4jQuerySupport.buildExpandQuery(direction, relationFamily, edgeTypes, candidateLimit);
        try (Session session = newSession()) {
            return session.run(query, Neo4jQuerySupport.expandParameters(seedNodeIds, relationFamily, edgeTypes))
                .list(projectionSupport::mapEdgeRow);
        }
    }

    @Override
    public Optional<PathRow> findShortestPath(String sourceNodeId,
                                              String targetNodeId,
                                              String relationFamily,
                                              Direction direction,
                                              int maxDepth) {
        String query = Neo4jQuerySupport.buildShortestPathQuery(direction, maxDepth);
        try (Session session = newSession()) {
            var result = session.run(query, Neo4jQuerySupport.pathParameters(sourceNodeId, targetNodeId, relationFamily));
            if (!result.hasNext()) {
                return Optional.empty();
            }
            var record = result.next();
            List<String> nodeIds = record.get("node_ids").asList(Value::asString);
            List<String> edgeIds = record.get("edge_ids").asList(Value::asString);
            int hopCount = record.get("hop_count").asInt();
            return Optional.of(new PathRow(nodeIds, edgeIds, hopCount));
        }
    }

    private Session newSession() {
        String database = memgraphProperties.getDatabase();
        if (database == null || database.isBlank()) {
            return driver.session();
        }
        return driver.session(SessionConfig.forDatabase(database));
    }
}
