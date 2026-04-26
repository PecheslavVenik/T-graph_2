package com.pm.graph_api_v2.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "graph.memgraph")
public class MemgraphProperties {

    private String uri = "bolt://localhost:7688";
    private String username = "";
    private String password = "";
    private String database = "";
    private boolean syncGraphStateOnStartup = true;
    private boolean clearProjectionOnStartup = false;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
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

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
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
