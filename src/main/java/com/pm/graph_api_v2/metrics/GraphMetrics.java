package com.pm.graph_api_v2.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class GraphMetrics {

    private final MeterRegistry meterRegistry;
    private final DistributionSummary resultNodesSummary;
    private final DistributionSummary resultEdgesSummary;
    private final DistributionSummary candidateEdgesSummary;

    public GraphMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.resultNodesSummary = DistributionSummary.builder("graph.result.nodes.count")
            .description("Node count returned by graph operations")
            .register(meterRegistry);
        this.resultEdgesSummary = DistributionSummary.builder("graph.result.edges.count")
            .description("Edge count returned by graph operations")
            .register(meterRegistry);
        this.candidateEdgesSummary = DistributionSummary.builder("graph.candidate.edges.count")
            .description("Candidate edges examined before result limits")
            .register(meterRegistry);
    }

    public Timer.Sample startTimer() {
        return Timer.start(meterRegistry);
    }

    public void stopTimer(Timer.Sample sample, String operation) {
        sample.stop(Timer.builder("graph.query.duration")
            .description("Graph query duration")
            .tag("operation", operation)
            .register(meterRegistry));
    }

    public void recordNodeCount(int count) {
        resultNodesSummary.record(count);
    }

    public void recordEdgeCount(int count) {
        resultEdgesSummary.record(count);
    }

    public void recordCandidateEdgeCount(int count) {
        candidateEdgesSummary.record(count);
    }

    public void recordTruncation(String operation) {
        Counter.builder("graph.result.truncated.count")
            .description("Number of truncated graph responses")
            .tag("operation", operation)
            .register(meterRegistry)
            .increment();
    }

    public void recordGuardrailHit(String operation, String guardrail) {
        Counter.builder("graph.guardrail.hit.count")
            .description("Number of requests constrained by backend guardrails")
            .tag("operation", operation)
            .tag("guardrail", guardrail)
            .register(meterRegistry)
            .increment();
    }
}
