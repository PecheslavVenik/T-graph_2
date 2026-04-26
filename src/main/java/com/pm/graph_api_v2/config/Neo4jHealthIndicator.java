package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.Neo4jRuntimeManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("neo4jBackend")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "NEO4J")
public class Neo4jHealthIndicator implements HealthIndicator {

    private final Neo4jRuntimeManager neo4jRuntimeManager;

    public Neo4jHealthIndicator(Neo4jRuntimeManager neo4jRuntimeManager) {
        this.neo4jRuntimeManager = neo4jRuntimeManager;
    }

    @Override
    public Health health() {
        boolean available = neo4jRuntimeManager.isAvailable();
        if (available) {
            return Health.up().withDetail("neo4j.available", true).build();
        }

        return Health.down()
            .withDetail("neo4j.available", false)
            .build();
    }
}
