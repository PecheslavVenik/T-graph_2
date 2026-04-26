package com.pm.graph_api_v2.config;

import com.kuzudb.Connection;
import com.kuzudb.Database;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

@Configuration
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "KUZU")
public class KuzuDatabaseConfig {

    @Bean(destroyMethod = "close")
    public Database kuzuDatabase(KuzuProperties properties) throws java.io.IOException {
        Path path = Path.of(properties.getPath()).toAbsolutePath();
        Path parent = path.getParent();
        if (parent != null) {
            java.nio.file.Files.createDirectories(parent);
        }
        return new Database(path.toString());
    }

    @Bean(destroyMethod = "close")
    public Connection kuzuConnection(Database kuzuDatabase) {
        return new Connection(kuzuDatabase);
    }
}
