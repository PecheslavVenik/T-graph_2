package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.Neo4jGraphQueryBackend;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

@Component
@DependsOn("flyway")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "NEO4J")
public class Neo4jProjectionLoader {

    private static final Logger log = LoggerFactory.getLogger(Neo4jProjectionLoader.class);

    private final Neo4jGraphQueryBackend neo4jGraphQueryBackend;

    public Neo4jProjectionLoader(Neo4jGraphQueryBackend neo4jGraphQueryBackend) {
        this.neo4jGraphQueryBackend = neo4jGraphQueryBackend;
    }

    @PostConstruct
    public void initialize() {
        try {
            neo4jGraphQueryBackend.initialize();
        } catch (Exception ex) {
            log.error("neo4j initialization failed", ex);
            throw new IllegalStateException("neo4j initialization failed", ex);
        }
    }
}
