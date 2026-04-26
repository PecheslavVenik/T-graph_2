package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.PostgresAgeProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Component
@ConditionalOnProperty(name = "graph.query-backend", havingValue = "POSTGRES_AGE")
public class PostgresAgeConnectionFactory {

    private final PostgresAgeProperties properties;

    public PostgresAgeConnectionFactory(PostgresAgeProperties properties) {
        this.properties = properties;
    }

    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(properties.getJdbcUrl(), properties.getUsername(), properties.getPassword());
    }

    public String table(String tableName) {
        return quoteIdentifier(properties.getSchema()) + "." + quoteIdentifier(tableName);
    }

    public String schema() {
        return quoteIdentifier(properties.getSchema());
    }

    private String quoteIdentifier(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("PostgreSQL identifier must not be blank");
        }
        return "\"" + raw.replace("\"", "\"\"") + "\"";
    }
}
