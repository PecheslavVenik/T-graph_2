package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.repository.MemgraphRuntimeManager;
import jakarta.annotation.PostConstruct;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "MEMGRAPH")
public class MemgraphProjectionLoader {

    private static final Logger log = LoggerFactory.getLogger(MemgraphProjectionLoader.class);

    private final MemgraphRuntimeManager memgraphRuntimeManager;

    public MemgraphProjectionLoader(MemgraphRuntimeManager memgraphRuntimeManager,
                                    Optional<Flyway> flyway) {
        this.memgraphRuntimeManager = memgraphRuntimeManager;
    }

    @PostConstruct
    public void initialize() {
        try {
            memgraphRuntimeManager.initialize();
        } catch (Exception ex) {
            log.error("memgraph initialization failed", ex);
            throw new IllegalStateException("memgraph initialization failed", ex);
        }
    }
}
