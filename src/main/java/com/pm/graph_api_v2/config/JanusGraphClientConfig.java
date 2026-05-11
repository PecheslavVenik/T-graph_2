package com.pm.graph_api_v2.config;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "JANUSGRAPH")
public class JanusGraphClientConfig {

    @Bean(destroyMethod = "close")
    public Cluster janusGraphCluster(JanusGraphProperties properties) {
        return Cluster.build()
            .addContactPoint(properties.getHost())
            .port(properties.getPort())
            .minConnectionPoolSize(properties.getMinConnectionPoolSize())
            .maxConnectionPoolSize(properties.getMaxConnectionPoolSize())
            .maxInProcessPerConnection(properties.getMaxInProcessPerConnection())
            .maxWaitForConnection(properties.getMaxWaitForConnectionMillis())
            .connectionSetupTimeoutMillis(properties.getConnectionSetupTimeoutMillis())
            .resultIterationBatchSize(properties.getResultBatchSize())
            .create();
    }

    @Bean(destroyMethod = "close")
    public Client janusGraphClient(Cluster janusGraphCluster) {
        return janusGraphCluster.connect();
    }
}
