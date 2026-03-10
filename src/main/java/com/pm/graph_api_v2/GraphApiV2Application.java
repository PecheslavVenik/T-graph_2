package com.pm.graph_api_v2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class GraphApiV2Application {

    public static void main(String[] args) {
        SpringApplication.run(GraphApiV2Application.class, args);
    }
}
