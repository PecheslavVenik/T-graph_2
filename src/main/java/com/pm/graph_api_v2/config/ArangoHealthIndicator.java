package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.ArangoRuntimeManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("arangoBackend")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "ARANGODB")
public class ArangoHealthIndicator implements HealthIndicator {

    private final ArangoRuntimeManager runtimeManager;

    public ArangoHealthIndicator(ArangoRuntimeManager runtimeManager) {
        this.runtimeManager = runtimeManager;
    }

    @Override
    public Health health() {
        if (runtimeManager.isAvailable()) {
            return Health.up().withDetail("arangodb.available", true).build();
        }
        return Health.down().withDetail("arangodb.available", false).build();
    }
}
