package com.pm.graph_api_v2.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "graph.kuzu")
public class KuzuProperties {

    private String path = "data/graph_bench.kuzu";
    private boolean syncGraphStateOnStartup = true;
    private boolean clearProjectionOnStartup = false;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
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
