package com.pm.graph_api_v2.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import com.pm.graph_api_v2.repository.DuckPgqGraphQueryRepository;
import jakarta.annotation.PostConstruct;

@Component
@DependsOn("flyway")
public class DuckPgqExtensionLoader {

    private static final Logger log = LoggerFactory.getLogger(DuckPgqExtensionLoader.class);

    private final DuckPgqGraphQueryRepository duckPgqGraphQueryRepository;
    private final DuckPgqProperties properties;

    public DuckPgqExtensionLoader(DuckPgqGraphQueryRepository duckPgqGraphQueryRepository,
                                  DuckPgqProperties properties) {
        this.duckPgqGraphQueryRepository = duckPgqGraphQueryRepository;
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
            duckPgqGraphQueryRepository.initialize();
        } catch (Exception ex) {
            log.error("duckpgq initialization failed", ex);
            throw new IllegalStateException("duckpgq initialization failed", ex);
        }
    }
}
