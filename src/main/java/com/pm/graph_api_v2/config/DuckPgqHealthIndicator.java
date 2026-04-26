package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.DuckPgqRuntimeManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("duckpgqLoaded")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "DUCKPGQ", matchIfMissing = true)
public class DuckPgqHealthIndicator implements HealthIndicator {

    private final DuckPgqRuntimeManager duckPgqRuntimeManager;

    public DuckPgqHealthIndicator(DuckPgqRuntimeManager duckPgqRuntimeManager) {
        this.duckPgqRuntimeManager = duckPgqRuntimeManager;
    }

    @Override
    public Health health() {
        boolean loaded = duckPgqRuntimeManager.isDuckPgqLoaded();
        if (loaded) {
            return Health.up().withDetail("duckpgq.loaded", true).build();
        }

        return Health.down()
            .withDetail("duckpgq.loaded", false)
            .build();
    }
}
