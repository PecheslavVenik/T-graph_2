package com.pm.graph_api_v2.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "graph.postgres-age")
public class PostgresAgeProperties {

    private String jdbcUrl = "jdbc:postgresql://localhost:5433/graph_bench";
    private String username = "graph";
    private String password = "graph-api-password";
    private String schema = "public";
    private boolean syncGraphStateOnStartup = true;
    private boolean clearProjectionOnStartup = false;

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public boolean isSyncGraphStateOnStartup() {
        return syncGraphStateOnStartup;
    }

    public void setSyncGraphStateOnStartup(boolean syncGraphStateOnStartup) {
        this.syncGraphStateOnStartup = syncGraphStateOnStartup;
    }

    public boolean isClearProjectionOnStartup() {
        return clearProjectionOnStartup;
    }

    public void setClearProjectionOnStartup(boolean clearProjectionOnStartup) {
        this.clearProjectionOnStartup = clearProjectionOnStartup;
    }
}
