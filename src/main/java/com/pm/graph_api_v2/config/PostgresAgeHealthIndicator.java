package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.PostgresAgeRuntimeManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("postgresAgeBackend")
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "POSTGRES_AGE")
public class PostgresAgeHealthIndicator implements HealthIndicator {

    private final PostgresAgeRuntimeManager runtimeManager;

    public PostgresAgeHealthIndicator(PostgresAgeRuntimeManager runtimeManager) {
        this.runtimeManager = runtimeManager;
    }

    @Override
    public Health health() {
        if (runtimeManager.isAvailable()) {
            return Health.up().withDetail("postgres-age.available", true).build();
        }
        return Health.down().withDetail("postgres-age.available", false).build();
    }
}
