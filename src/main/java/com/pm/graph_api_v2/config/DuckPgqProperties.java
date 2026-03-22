package com.pm.graph_api_v2.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "graph.duckpgq")
public class DuckPgqProperties {

    private boolean enabled = true;
    private boolean autoLoad = true;
    private boolean preferLatest = false;
    private boolean forceInstall = false;
    private String repositoryUrl = "http://duckpgq.s3.eu-north-1.amazonaws.com";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isAutoLoad() {
        return autoLoad;
    }

    public void setAutoLoad(boolean autoLoad) {
        this.autoLoad = autoLoad;
    }

    public boolean isPreferLatest() {
        return preferLatest;
    }

    public void setPreferLatest(boolean preferLatest) {
        this.preferLatest = preferLatest;
    }

    public String getRepositoryUrl() {
        return repositoryUrl;
    }

    public void setRepositoryUrl(String repositoryUrl) {
        this.repositoryUrl = repositoryUrl;
    }

    public boolean isForceInstall() {
        return forceInstall;
    }

    public void setForceInstall(boolean forceInstall) {
        this.forceInstall = forceInstall;
    }
}
