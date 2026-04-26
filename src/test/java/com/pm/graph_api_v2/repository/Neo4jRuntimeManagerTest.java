package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.Neo4jProperties;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class Neo4jRuntimeManagerTest {

    private Driver driver;
    private Session session;
    private GraphRepository graphRepository;
    private Neo4jProperties neo4jProperties;
    private Neo4jRuntimeManager runtimeManager;

    @BeforeEach
    void setUp() {
        driver = mock(Driver.class);
        session = mock(Session.class);
        graphRepository = mock(GraphRepository.class);
        neo4jProperties = new Neo4jProperties();
        neo4jProperties.setDatabase("");
        when(driver.session()).thenReturn(session);
        runtimeManager = new Neo4jRuntimeManager(driver, graphRepository, new Neo4jProjectionSupport(new ObjectMapper()), neo4jProperties);
    }

    @Test
    void initialize_shouldProjectReverseTraversalForUndirectedEdges() {
        Result result = mock(Result.class);
        ResultSummary summary = mock(ResultSummary.class);

        when(session.run(anyString())).thenReturn(result);
        when(session.run(anyString(), any(Value.class))).thenReturn(result);
        when(result.consume()).thenReturn(summary);
        List<NodeRow> nodes = List.of(
            new NodeRow(
                "N_PARTY_1001",
                "PERSON",
                "Ivan Ivanov",
                "PARTY_1001",
                "P1001",
                "+79990000001",
                "Ivan Ivanov",
                false,
                false,
                "ACME",
                "Vladivostok",
                "crm",
                0.2,
                0.1,
                Map.of("party_rk", "PARTY_1001"),
                Map.of("risk", "low")
            ),
            new NodeRow(
                "N_PARTY_1002",
                "PERSON",
                "Petr Petrov",
                "PARTY_1002",
                "P1002",
                "+79990000002",
                "Petr Petrov",
                false,
                false,
                "ACME",
                "Vladivostok",
                "crm",
                0.3,
                0.2,
                Map.of("party_rk", "PARTY_1002"),
                Map.of()
            )
        );
        List<EdgeRow> edges = List.of(
            new EdgeRow(
                "E_REL_1001_1002",
                "N_PARTY_1001",
                "N_PARTY_1002",
                "RELATIVE",
                false,
                0,
                0D,
                "PERSON_RELATIVE_PERSON",
                0.7,
                1,
                "crm",
                Instant.parse("2026-04-01T00:00:00Z"),
                Instant.parse("2026-04-02T00:00:00Z"),
                Map.of("kind", "family")
            )
        );
        stubNodeBatches(nodes);
        stubEdgeBatches(edges);

        runtimeManager.initialize();

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Value> parametersCaptor = ArgumentCaptor.forClass(Value.class);
        verify(session, atLeast(2)).run(queryCaptor.capture(), parametersCaptor.capture());

        assertThat(queryCaptor.getAllValues())
            .noneMatch(query -> query.contains("DETACH DELETE n"))
            .anyMatch(query -> query.contains("MERGE (from)-[r:" + Neo4jQuerySupport.REL_TYPE + " {graph_rel_id: row.graph_rel_id}]->(to)"));

        List<Value> allParams = parametersCaptor.getAllValues();
        Value relationshipBatch = allParams.get(allParams.size() - 1).get("batch");
        List<Map<String, Object>> relationships = relationshipBatch.asList(Value::asMap);

        assertThat(relationships).hasSize(2);
        assertThat(relationships)
            .extracting(item -> item.get("graph_rel_id"))
            .containsExactlyInAnyOrder("E_REL_1001_1002:fwd", "E_REL_1001_1002:rev");
        assertThat(relationships)
            .extracting(item -> item.get("projection_owner"))
            .containsOnly(Neo4jProjectionSupport.PROJECTION_OWNER);
    }

    @Test
    void initialize_shouldOnlyClearOwnedProjectionWhenEnabled() {
        Result result = mock(Result.class);
        ResultSummary summary = mock(ResultSummary.class);
        Record clearRecord = mock(Record.class);
        Value deletedValue = mock(Value.class);

        neo4jProperties.setClearProjectionOnStartup(true);
        when(session.run(anyString())).thenReturn(result);
        when(session.run(anyString(), any(Value.class))).thenReturn(result);
        when(result.consume()).thenReturn(summary);
        when(result.single()).thenReturn(clearRecord);
        when(clearRecord.get("deleted")).thenReturn(deletedValue);
        when(deletedValue.asLong()).thenReturn(0L);

        runtimeManager.initialize();

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Value> parametersCaptor = ArgumentCaptor.forClass(Value.class);
        verify(session, atLeast(1)).run(queryCaptor.capture(), parametersCaptor.capture());

        assertThat(queryCaptor.getAllValues())
            .anyMatch(query -> query.contains("projection_owner: $projectionOwner") && query.contains("DETACH DELETE n"));
        assertThat(parametersCaptor.getAllValues())
            .map(value -> value.get("projectionOwner"))
            .filteredOn(value -> !value.isNull())
            .extracting(Value::asString)
            .contains(Neo4jProjectionSupport.PROJECTION_OWNER);
    }

    private void stubNodeBatches(List<NodeRow> nodes) {
        doAnswer(invocation -> {
            Consumer<List<NodeRow>> consumer = invocation.getArgument(1);
            consumer.accept(nodes);
            return null;
        }).when(graphRepository).forEachNodeBatch(anyInt(), any());
    }

    private void stubEdgeBatches(List<EdgeRow> edges) {
        doAnswer(invocation -> {
            Consumer<List<EdgeRow>> consumer = invocation.getArgument(1);
            consumer.accept(edges);
            return null;
        }).when(graphRepository).forEachEdgeBatch(anyInt(), any());
    }
}
