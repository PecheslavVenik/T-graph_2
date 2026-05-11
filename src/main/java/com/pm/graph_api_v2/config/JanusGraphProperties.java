package com.pm.graph_api_v2.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "graph.janusgraph")
public class JanusGraphProperties {

    private String host = "localhost";
    private int port = 8182;
    private boolean syncGraphStateOnStartup = true;
    private boolean clearProjectionOnStartup = false;
    private int minConnectionPoolSize = 1;
    private int maxConnectionPoolSize = 4;
    private int maxInProcessPerConnection = 4;
    private int maxWaitForConnectionMillis = 15_000;
    private int connectionSetupTimeoutMillis = 30_000;
    private int requestTimeoutMillis = 25_000;
    private int resultBatchSize = 64;
    private int maxConcurrentQueries = 2;
    private int queryPermitTimeoutMillis = 30_000;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public int getMinConnectionPoolSize() {
        return minConnectionPoolSize;
    }

    public void setMinConnectionPoolSize(int minConnectionPoolSize) {
        this.minConnectionPoolSize = minConnectionPoolSize;
    }

    public int getMaxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    public void setMaxConnectionPoolSize(int maxConnectionPoolSize) {
        this.maxConnectionPoolSize = maxConnectionPoolSize;
    }

    public int getMaxInProcessPerConnection() {
        return maxInProcessPerConnection;
    }

    public void setMaxInProcessPerConnection(int maxInProcessPerConnection) {
        this.maxInProcessPerConnection = maxInProcessPerConnection;
    }

    public int getMaxWaitForConnectionMillis() {
        return maxWaitForConnectionMillis;
    }

    public void setMaxWaitForConnectionMillis(int maxWaitForConnectionMillis) {
        this.maxWaitForConnectionMillis = maxWaitForConnectionMillis;
    }

    public int getConnectionSetupTimeoutMillis() {
        return connectionSetupTimeoutMillis;
    }

    public void setConnectionSetupTimeoutMillis(int connectionSetupTimeoutMillis) {
        this.connectionSetupTimeoutMillis = connectionSetupTimeoutMillis;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public void setRequestTimeoutMillis(int requestTimeoutMillis) {
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    public int getResultBatchSize() {
        return resultBatchSize;
    }

    public void setResultBatchSize(int resultBatchSize) {
        this.resultBatchSize = resultBatchSize;
    }

    public int getMaxConcurrentQueries() {
        return maxConcurrentQueries;
    }

    public void setMaxConcurrentQueries(int maxConcurrentQueries) {
        this.maxConcurrentQueries = maxConcurrentQueries;
    }

    public int getQueryPermitTimeoutMillis() {
        return queryPermitTimeoutMillis;
    }

    public void setQueryPermitTimeoutMillis(int queryPermitTimeoutMillis) {
        this.queryPermitTimeoutMillis = queryPermitTimeoutMillis;
    }
}
