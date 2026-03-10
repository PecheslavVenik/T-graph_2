package com.pm.graph_api_v2.integration;

public interface ExternalGraphSourceAdapter {

    String sourceName();

    void ingest(ExternalGraphIngestRequest request);
}
