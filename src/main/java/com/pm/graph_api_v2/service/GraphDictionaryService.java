package com.pm.graph_api_v2.service;

import com.pm.graph_api_v2.dto.GraphDictionaryResponse;
import com.pm.graph_api_v2.repository.GraphDictionaryRepository;
import org.springframework.stereotype.Service;

@Service
public class GraphDictionaryService {

    private final GraphDictionaryRepository dictionaryRepository;
    private final GraphDictionaryFactory graphDictionaryFactory;

    public GraphDictionaryService(GraphDictionaryRepository dictionaryRepository,
                                  GraphDictionaryFactory graphDictionaryFactory) {
        this.dictionaryRepository = dictionaryRepository;
        this.graphDictionaryFactory = graphDictionaryFactory;
    }

    public GraphDictionaryResponse dictionary() {
        return graphDictionaryFactory.create(
            dictionaryRepository.findDistinctEdgeTypes(),
            dictionaryRepository.findDistinctRelationFamilies(),
            dictionaryRepository.findDistinctNodeTypes(),
            dictionaryRepository.findPresentNodeStatuses()
        );
    }
}
