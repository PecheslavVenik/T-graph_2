package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.KuzuRuntimeManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("kuzuBackend")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "KUZU")
public class KuzuHealthIndicator implements HealthIndicator {

    private final KuzuRuntimeManager runtimeManager;

    public KuzuHealthIndicator(KuzuRuntimeManager runtimeManager) {
        this.runtimeManager = runtimeManager;
    }

    @Override
    public Health health() {
        if (runtimeManager.isAvailable()) {
            return Health.up().withDetail("kuzu.available", true).build();
        }
        return Health.down().withDetail("kuzu.available", false).build();
    }
}
