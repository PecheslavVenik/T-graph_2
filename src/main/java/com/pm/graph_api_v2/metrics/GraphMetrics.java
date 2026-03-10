package com.pm.graph_api_v2.metrics;

import com.pm.graph_api_v2.dto.GraphSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class GraphMetrics {

    private final MeterRegistry meterRegistry;
    private final Counter fallbackUsageCounter;
    private final DistributionSummary resultNodesSummary;

    public GraphMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.fallbackUsageCounter = Counter.builder("graph.fallback.usage.count")
            .description("Number of requests served by SQL fallback")
            .register(meterRegistry);
        this.resultNodesSummary = DistributionSummary.builder("graph.result.nodes.count")
            .description("Node count returned by graph operations")
            .register(meterRegistry);
    }

    public Timer.Sample startTimer() {
        return Timer.start(meterRegistry);
    }

    public void stopTimer(Timer.Sample sample, String operation, GraphSource source) {
        sample.stop(Timer.builder("graph.query.duration")
            .description("Graph query duration")
            .tag("operation", operation)
            .tag("source", source.name())
            .register(meterRegistry));

        if (source == GraphSource.SQL_FALLBACK) {
            fallbackUsageCounter.increment();
        }
    }

    public void recordNodeCount(int count) {
        resultNodesSummary.record(count);
    }
}
