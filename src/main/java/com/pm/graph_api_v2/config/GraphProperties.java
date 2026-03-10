package com.pm.graph_api_v2.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "graph")
public class GraphProperties {

    @Min(1)
    @Max(1000)
    private int defaultMaxNeighborsPerSeed = 200;

    @Min(1)
    @Max(1000)
    private int defaultMaxNodes = 1000;

    @Min(1)
    @Max(5000)
    private int defaultMaxEdges = 2000;

    @Min(1)
    @Max(10)
    private int defaultMaxDepth = 6;

    public int getDefaultMaxNeighborsPerSeed() {
        return defaultMaxNeighborsPerSeed;
    }

    public void setDefaultMaxNeighborsPerSeed(int defaultMaxNeighborsPerSeed) {
        this.defaultMaxNeighborsPerSeed = defaultMaxNeighborsPerSeed;
    }

    public int getDefaultMaxNodes() {
        return defaultMaxNodes;
    }

    public void setDefaultMaxNodes(int defaultMaxNodes) {
        this.defaultMaxNodes = defaultMaxNodes;
    }

    public int getDefaultMaxEdges() {
        return defaultMaxEdges;
    }

    public void setDefaultMaxEdges(int defaultMaxEdges) {
        this.defaultMaxEdges = defaultMaxEdges;
    }

    public int getDefaultMaxDepth() {
        return defaultMaxDepth;
    }

    public void setDefaultMaxDepth(int defaultMaxDepth) {
        this.defaultMaxDepth = defaultMaxDepth;
    }
}
