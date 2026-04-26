package com.pm.graph_api_v2.config;

import com.pm.graph_api_v2.util.GraphRelationFamilies;
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

    @Min(100)
    @Max(200000)
    private int maxExpandCandidateEdges = 10000;

    @Min(0)
    @Max(100)
    private int hubPenaltyPercent = 35;

    private String defaultRelationFamily = GraphRelationFamilies.ALL_RELATIONS;

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

    public int getMaxExpandCandidateEdges() {
        return maxExpandCandidateEdges;
    }

    public void setMaxExpandCandidateEdges(int maxExpandCandidateEdges) {
        this.maxExpandCandidateEdges = maxExpandCandidateEdges;
    }

    public int getHubPenaltyPercent() {
        return hubPenaltyPercent;
    }

    public void setHubPenaltyPercent(int hubPenaltyPercent) {
        this.hubPenaltyPercent = hubPenaltyPercent;
    }

    public String getDefaultRelationFamily() {
        return defaultRelationFamily;
    }

    public void setDefaultRelationFamily(String defaultRelationFamily) {
        String normalized = GraphRelationFamilies.normalize(defaultRelationFamily);
        this.defaultRelationFamily = normalized == null ? GraphRelationFamilies.ALL_RELATIONS : normalized;
    }
}
