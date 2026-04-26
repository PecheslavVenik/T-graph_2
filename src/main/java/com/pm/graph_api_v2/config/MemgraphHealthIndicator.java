package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.MemgraphRuntimeManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("memgraphBackend")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "MEMGRAPH")
public class MemgraphHealthIndicator implements HealthIndicator {

    private final MemgraphRuntimeManager memgraphRuntimeManager;

    public MemgraphHealthIndicator(MemgraphRuntimeManager memgraphRuntimeManager) {
        this.memgraphRuntimeManager = memgraphRuntimeManager;
    }

    @Override
    public Health health() {
        if (memgraphRuntimeManager.isAvailable()) {
            return Health.up().withDetail("memgraph.available", true).build();
        }
        return Health.down().withDetail("memgraph.available", false).build();
    }
}
