package com.pm.graph_api_v2.service;

public record ExportedGraph(String fileName, String contentType, byte[] payload) {
}
