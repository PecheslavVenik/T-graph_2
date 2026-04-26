package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.JanusGraphRuntimeManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("janusGraphBackend")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "JANUSGRAPH")
public class JanusGraphHealthIndicator implements HealthIndicator {

    private final JanusGraphRuntimeManager runtimeManager;

    public JanusGraphHealthIndicator(JanusGraphRuntimeManager runtimeManager) {
        this.runtimeManager = runtimeManager;
    }

    @Override
    public Health health() {
        if (runtimeManager.isAvailable()) {
            return Health.up().withDetail("janusgraph.available", true).build();
        }
        return Health.down().withDetail("janusgraph.available", false).build();
    }
}
