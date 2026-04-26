package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.DuckPgqRuntimeManager;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

import java.util.Optional;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "DUCKPGQ", matchIfMissing = true)
public class DuckPgqExtensionLoader {

    private static final Logger log = LoggerFactory.getLogger(DuckPgqExtensionLoader.class);

    private final DuckPgqRuntimeManager duckPgqRuntimeManager;
    private final DuckPgqProperties properties;

    public DuckPgqExtensionLoader(DuckPgqRuntimeManager duckPgqRuntimeManager,
                                  DuckPgqProperties properties,
                                  Optional<Flyway> flyway) {
        this.duckPgqRuntimeManager = duckPgqRuntimeManager;
        this.properties = properties;
    }

    @PostConstruct
    public void initialize() {
        if (!properties.isEnabled() || !properties.isAutoLoad()) {
            log.info("duckpgq initialization is skipped (enabled={}, autoLoad={})",
                properties.isEnabled(), properties.isAutoLoad());
            return;
        }

        try {
            duckPgqRuntimeManager.initialize();
        } catch (Exception ex) {
            log.error("duckpgq initialization failed", ex);
            throw new IllegalStateException("duckpgq initialization failed", ex);
        }
    }
}
