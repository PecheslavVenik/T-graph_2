package com.pm.graph_api_v2.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "graph.duckpgq")
public class DuckPgqProperties {

    private boolean enabled = true;
    private boolean forceFallback = false;
    private boolean autoLoad = true;
    private boolean failOnUnavailable = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isForceFallback() {
        return forceFallback;
    }

    public void setForceFallback(boolean forceFallback) {
        this.forceFallback = forceFallback;
    }

    public boolean isAutoLoad() {
        return autoLoad;
    }

    public void setAutoLoad(boolean autoLoad) {
        this.autoLoad = autoLoad;
    }

    public boolean isFailOnUnavailable() {
        return failOnUnavailable;
    }

    public void setFailOnUnavailable(boolean failOnUnavailable) {
        this.failOnUnavailable = failOnUnavailable;
    }
}
