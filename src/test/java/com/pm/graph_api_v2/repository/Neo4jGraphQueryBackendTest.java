package com.pm.graph_api_v2.repository;

import com.pm.graph_api_v2.config.Neo4jProperties;
import com.pm.graph_api_v2.dto.Direction;
import com.pm.graph_api_v2.dto.GraphSource;
import com.pm.graph_api_v2.repository.model.EdgeRow;
import com.pm.graph_api_v2.repository.model.NodeRow;
import com.pm.graph_api_v2.repository.model.PathRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.summary.ResultSummary;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Neo4jGraphQueryBackendTest {

    private Driver driver;
    private Session session;
    private Neo4jProperties neo4jProperties;
    private Neo4jProjectionSupport projectionSupport;
    private Neo4jGraphQueryBackend backend;

    @BeforeEach
    void setUp() {
        driver = mock(Driver.class);
        session = mock(Session.class);
        neo4jProperties = new Neo4jProperties();
        neo4jProperties.setDatabase("");
        when(driver.session()).thenReturn(session);
        projectionSupport = new Neo4jProjectionSupport(new ObjectMapper());
        backend = new Neo4jGraphQueryBackend(driver, projectionSupport, neo4jProperties);
    }

    @Test
    void source_shouldBeNeo4j() {
        assertThat(backend.source()).isEqualTo(GraphSource.NEO4J);
    }

    @Test
    @SuppressWarnings("unchecked")
    void findExpandEdges_shouldMapEdgeProperties() {
        Result result = mock(Result.class);
        org.neo4j.driver.Record record = mock(org.neo4j.driver.Record.class);

        when(session.run(anyString(), any(Value.class))).thenReturn(result);
        when(result.list(any(Function.class))).thenAnswer(invocation -> {
            Function<org.neo4j.driver.Record, EdgeRow> mapper =
                (Function<org.neo4j.driver.Record, EdgeRow>) invocation.getArgument(0);
            return List.of(mapper.apply(record));
        });

        when(record.get("edge_id")).thenReturn(Values.value("E_TX_1001_1002"));
        when(record.get("from_node_id")).thenReturn(Values.value("N_PARTY_1001"));
        when(record.get("to_node_id")).thenReturn(Values.value("N_PARTY_1002"));
        when(record.get("edge_type")).thenReturn(Values.value("TRANSFERS_TO"));
        when(record.get("directed")).thenReturn(Values.value(true));
        when(record.get("tx_count")).thenReturn(Values.value(7L));
        when(record.get("tx_sum")).thenReturn(Values.value(15000.5));
        when(record.get("relation_family")).thenReturn(Values.value("ACCOUNT_FLOW"));
        when(record.get("strength_score")).thenReturn(Values.value(0.91));
        when(record.get("evidence_count")).thenReturn(Values.value(3L));
        when(record.get("source_system")).thenReturn(Values.value("neo4j-sync"));
        when(record.get("first_seen_at")).thenReturn(Values.value("2026-04-01T00:00:00Z"));
        when(record.get("last_seen_at")).thenReturn(Values.value("2026-04-02T00:00:00Z"));
        when(record.get("attrs_json")).thenReturn(Values.value("{\"channel\":\"mobile\"}"));

        List<EdgeRow> rows = backend.findExpandEdges(
            List.of("N_PARTY_1001"),
            "ACCOUNT_FLOW",
            List.of(),
            Direction.OUTBOUND,
            20
        );

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).edgeId()).isEqualTo("E_TX_1001_1002");
        assertThat(rows.get(0).relationFamily()).isEqualTo("ACCOUNT_FLOW");
        assertThat(rows.get(0).attrs()).containsEntry("channel", "mobile");
    }

    @Test
    void findShortestPath_shouldMapNodeAndEdgeIds() {
        Result result = mock(Result.class);
        org.neo4j.driver.Record record = mock(org.neo4j.driver.Record.class);

        when(session.run(anyString(), any(Value.class))).thenReturn(result);
        when(result.hasNext()).thenReturn(true);
        when(result.next()).thenReturn(record);

        when(record.get("node_ids")).thenReturn(Values.value(List.of("N_PARTY_1001", "N_ACC_2001")));
        when(record.get("edge_ids")).thenReturn(Values.value(List.of("E_OWN_1001_2001")));
        when(record.get("hop_count")).thenReturn(Values.value(1));

        PathRow pathRow = backend.findShortestPath(
                "N_PARTY_1001",
                "N_ACC_2001",
                "CUSTOMER_OWNERSHIP",
                Direction.OUTBOUND,
                2
            )
            .orElseThrow();

        assertThat(pathRow.nodeIds()).containsExactly("N_PARTY_1001", "N_ACC_2001");
        assertThat(pathRow.edgeIds()).containsExactly("E_OWN_1001_2001");
        assertThat(pathRow.hopCount()).isEqualTo(1);
    }

}
