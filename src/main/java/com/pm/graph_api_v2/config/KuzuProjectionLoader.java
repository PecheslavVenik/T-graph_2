package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.KuzuRuntimeManager;
import jakarta.annotation.PostConstruct;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "KUZU")
public class KuzuProjectionLoader {

    private static final Logger log = LoggerFactory.getLogger(KuzuProjectionLoader.class);

    private final KuzuRuntimeManager runtimeManager;

    public KuzuProjectionLoader(KuzuRuntimeManager runtimeManager,
                                Optional<Flyway> flyway) {
        this.runtimeManager = runtimeManager;
    }

    @PostConstruct
    public void initialize() {
        try {
            runtimeManager.initialize();
        } catch (Exception ex) {
            log.error("kuzu initialization failed", ex);
            throw new IllegalStateException("kuzu initialization failed", ex);
        }
    }
}
