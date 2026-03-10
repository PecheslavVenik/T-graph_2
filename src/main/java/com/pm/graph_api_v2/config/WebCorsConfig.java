package com.pm.graph_api_v2.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebCorsConfig implements WebMvcConfigurer {

    private final GraphCorsProperties graphCorsProperties;

    public WebCorsConfig(GraphCorsProperties graphCorsProperties) {
        this.graphCorsProperties = graphCorsProperties;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        if (graphCorsProperties.getAllowedOrigins() == null || graphCorsProperties.getAllowedOrigins().isEmpty()) {
            return;
        }

        registry.addMapping("/api/v1/**")
            .allowedOrigins(graphCorsProperties.getAllowedOrigins().toArray(String[]::new))
            .allowedMethods(graphCorsProperties.getAllowedMethods().toArray(String[]::new))
            .allowedHeaders(graphCorsProperties.getAllowedHeaders().toArray(String[]::new))
            .maxAge(graphCorsProperties.getMaxAgeSeconds());
    }
}
