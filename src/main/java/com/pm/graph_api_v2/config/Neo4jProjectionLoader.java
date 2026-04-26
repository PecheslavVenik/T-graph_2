package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.Neo4jRuntimeManager;
import jakarta.annotation.PostConstruct;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "NEO4J")
public class Neo4jProjectionLoader {

    private static final Logger log = LoggerFactory.getLogger(Neo4jProjectionLoader.class);

    private final Neo4jRuntimeManager neo4jRuntimeManager;

    public Neo4jProjectionLoader(Neo4jRuntimeManager neo4jRuntimeManager,
                                 Optional<Flyway> flyway) {
        this.neo4jRuntimeManager = neo4jRuntimeManager;
    }

    @PostConstruct
    public void initialize() {
        try {
            neo4jRuntimeManager.initialize();
        } catch (Exception ex) {
            log.error("neo4j initialization failed", ex);
            throw new IllegalStateException("neo4j initialization failed", ex);
        }
    }
}
