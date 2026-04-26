package com.pm.graph_api_v2.config;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "NEO4J")
public class Neo4jDriverConfig {

    @Bean(destroyMethod = "close")
    public Driver neo4jDriver(Neo4jProperties neo4jProperties) {
        if (isBlank(neo4jProperties.getUsername()) || isBlank(neo4jProperties.getPassword())) {
            throw new IllegalStateException("Neo4j username and password must be configured when graph.query-backend=NEO4J");
        }

        return GraphDatabase.driver(
            neo4jProperties.getUri(),
            AuthTokens.basic(neo4jProperties.getUsername(), neo4jProperties.getPassword())
        );
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
