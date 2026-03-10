package com.pm.graph_api_v2.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@DependsOn("flyway")
public class DuckPgqExtensionLoader implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DuckPgqExtensionLoader.class);

    private final JdbcTemplate jdbcTemplate;
    private final DuckPgqProperties properties;

    public DuckPgqExtensionLoader(JdbcTemplate jdbcTemplate, DuckPgqProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isEnabled() || properties.isForceFallback() || !properties.isAutoLoad()) {
            log.info("duckpgq autoload is skipped (enabled={}, forceFallback={}, autoLoad={})",
                properties.isEnabled(), properties.isForceFallback(), properties.isAutoLoad());
            return;
        }

        boolean loaded = tryLoad();
        if (!loaded) {
            loaded = tryInstallAndLoad();
        }

        updateEngineState(loaded);
        if (loaded) {
            log.info("duckpgq extension loaded successfully");
        } else {
            log.warn("duckpgq extension is unavailable, SQL fallback mode will be used");
        }
    }

    private boolean tryLoad() {
        try {
            jdbcTemplate.execute("LOAD duckpgq");
            return true;
        } catch (Exception ex) {
            log.info("LOAD duckpgq failed, will try INSTALL + LOAD: {}", ex.getMessage());
            return false;
        }
    }

    private boolean tryInstallAndLoad() {
        try {
            jdbcTemplate.execute("INSTALL duckpgq");
            jdbcTemplate.execute("LOAD duckpgq");
            return true;
        } catch (Exception ex) {
            log.warn("INSTALL/LOAD duckpgq failed: {}", ex.getMessage());
            return false;
        }
    }

    private void updateEngineState(boolean loaded) {
        try {
            jdbcTemplate.update("DELETE FROM g_engine_state WHERE state_key = 'duckpgq.loaded'");
            jdbcTemplate.update(
                "INSERT INTO g_engine_state (state_key, state_value, updated_at) VALUES ('duckpgq.loaded', ?, CURRENT_TIMESTAMP)",
                Boolean.toString(loaded)
            );
        } catch (Exception ex) {
            log.debug("Could not update g_engine_state for duckpgq.loaded: {}", ex.getMessage());
        }
    }
}
