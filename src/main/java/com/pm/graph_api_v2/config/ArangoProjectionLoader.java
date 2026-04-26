package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.ArangoRuntimeManager;
import jakarta.annotation.PostConstruct;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "ARANGODB")
public class ArangoProjectionLoader {

    private static final Logger log = LoggerFactory.getLogger(ArangoProjectionLoader.class);

    private final ArangoRuntimeManager runtimeManager;

    public ArangoProjectionLoader(ArangoRuntimeManager runtimeManager,
                                  Optional<Flyway> flyway) {
        this.runtimeManager = runtimeManager;
    }

    @PostConstruct
    public void initialize() {
        try {
            runtimeManager.initialize();
        } catch (Exception ex) {
            log.error("arangodb initialization failed", ex);
            throw new IllegalStateException("arangodb initialization failed", ex);
        }
    }
}
