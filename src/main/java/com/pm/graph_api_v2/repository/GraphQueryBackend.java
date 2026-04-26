package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.PathRow;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface GraphQueryBackend {

    GraphSource source();

    List<EdgeRow> findExpandEdges(Collection<String> seedNodeIds,
                                 String relationFamily,
                                 List<String> edgeTypes,
                                 Direction direction,
                                 int candidateLimit);

    Optional<PathRow> findShortestPath(String sourceNodeId,
                                       String targetNodeId,
                                       String relationFamily,
                                       Direction direction,
                                       int maxDepth);
}
