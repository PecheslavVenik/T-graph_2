package com.pm.graph_api_v2.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "graph.arango")
public class ArangoProperties {

    private String endpoint = "http://localhost:8529";
    private String database = "graph_bench";
    private String username = "root";
    private String rootPassword = "graph-api-password";
    private boolean syncGraphStateOnStartup = true;
    private boolean clearProjectionOnStartup = false;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getRootPassword() {
        return rootPassword;
    }

    public void setRootPassword(String rootPassword) {
        this.rootPassword = rootPassword;
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
