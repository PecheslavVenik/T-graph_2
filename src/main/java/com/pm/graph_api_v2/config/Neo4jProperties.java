package com.pm.graph_api_v2.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "graph.neo4j")
public class Neo4jProperties {

    private String uri = "bolt://localhost:7687";
    private String username = "neo4j";
    private String password = "graph-api-password";
    private String database = "neo4j";
    private boolean syncGraphStateOnStartup = true;

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
}
