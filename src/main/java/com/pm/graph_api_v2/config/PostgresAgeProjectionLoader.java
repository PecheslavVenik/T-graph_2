package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.PostgresAgeRuntimeManager;
import jakarta.annotation.PostConstruct;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "POSTGRES_AGE")
public class PostgresAgeProjectionLoader {

    private static final Logger log = LoggerFactory.getLogger(PostgresAgeProjectionLoader.class);

    private final PostgresAgeRuntimeManager runtimeManager;

    public PostgresAgeProjectionLoader(PostgresAgeRuntimeManager runtimeManager,
                                       Optional<Flyway> flyway) {
        this.runtimeManager = runtimeManager;
    }

    @PostConstruct
    public void initialize() {
        try {
            runtimeManager.initialize();
        } catch (Exception ex) {
            log.error("postgres-age initialization failed", ex);
            throw new IllegalStateException("postgres-age initialization failed", ex);
        }
    }
}
