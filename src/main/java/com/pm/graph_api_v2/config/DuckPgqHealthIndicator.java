package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.DuckPgqGraphQueryRepository;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("duckpgqLoaded")
public class DuckPgqHealthIndicator implements HealthIndicator {

    private final DuckPgqGraphQueryRepository duckPgqGraphQueryRepository;

    public DuckPgqHealthIndicator(DuckPgqGraphQueryRepository duckPgqGraphQueryRepository) {
        this.duckPgqGraphQueryRepository = duckPgqGraphQueryRepository;
    }

    @Override
    public Health health() {
        boolean loaded = duckPgqGraphQueryRepository.isDuckPgqLoaded();
        if (loaded) {
            return Health.up().withDetail("duckpgq.loaded", true).build();
        }

        return Health.status("DEGRADED")
            .withDetail("duckpgq.loaded", false)
            .withDetail("mode", "sql_fallback")
            .build();
    }
}
