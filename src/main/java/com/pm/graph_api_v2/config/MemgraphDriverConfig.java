package com.pm.graph_api_v2.config;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "MEMGRAPH")
public class MemgraphDriverConfig {

    @Bean(destroyMethod = "close")
    public Driver memgraphDriver(MemgraphProperties memgraphProperties) {
        if (isBlank(memgraphProperties.getUsername())) {
            return GraphDatabase.driver(memgraphProperties.getUri(), AuthTokens.none());
        }
        return GraphDatabase.driver(
            memgraphProperties.getUri(),
            AuthTokens.basic(memgraphProperties.getUsername(), blankToEmpty(memgraphProperties.getPassword()))
        );
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private String blankToEmpty(String value) {
        return value == null ? "" : value;
    }
}
