package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.Neo4jGraphQueryBackend;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("neo4jBackend")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "NEO4J")
public class Neo4jHealthIndicator implements HealthIndicator {

    private final Neo4jGraphQueryBackend neo4jGraphQueryBackend;

    public Neo4jHealthIndicator(Neo4jGraphQueryBackend neo4jGraphQueryBackend) {
        this.neo4jGraphQueryBackend = neo4jGraphQueryBackend;
    }

    @Override
    public Health health() {
        boolean available = neo4jGraphQueryBackend.isAvailable();
        if (available) {
            return Health.up().withDetail("neo4j.available", true).build();
        }

        return Health.down()
            .withDetail("neo4j.available", false)
            .build();
    }
}
