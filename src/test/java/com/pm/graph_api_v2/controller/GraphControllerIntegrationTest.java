package com.pm.graph_api_v2.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class GraphControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void expand_shouldReturnDuckPgqOneHopGraph() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "PARTY_RK", "value": "PARTY_1002"}
              ],
              "relationFamily": "PERSON_KNOWS_PERSON",
              "direction": "OUTBOUND",
              "maxNeighborsPerSeed": 1,
              "maxNodes": 100,
              "maxEdges": 100,
              "includeAttributes": true
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(2))
            .andExpect(jsonPath("$.edges.length()").value(1))
            .andExpect(jsonPath("$.meta.source").value("DUCKPGQ"))
            .andExpect(jsonPath("$.meta.relationFamily").value("PERSON_KNOWS_PERSON"))
            .andExpect(jsonPath("$.meta.rankingStrategy").value("INVESTIGATION_DEFAULT"))
            .andExpect(jsonPath("$.meta.warnings", hasItem("Per-seed neighbor budget filtered lower-ranked neighbors")))
            .andExpect(jsonPath("$.edges[*].type", hasItem("KNOWS")));
    }

    @Test
    void pgqProjection_shouldBuildRelationSpecificTables() {
        Integer allKnowsCopies = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM g_pgq_edges WHERE edge_id = 'E_TX_1001_1002'",
            Integer.class
        );
        Integer knowsCopies = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM g_pgq_edges_knows WHERE edge_id = 'E_TX_1001_1002'",
            Integer.class
        );
        Integer relativeCopiesInsideKnows = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM g_pgq_edges_knows WHERE edge_id = 'E_REL_1001_1004'",
            Integer.class
        );

        org.assertj.core.api.Assertions.assertThat(allKnowsCopies).isEqualTo(2);
        org.assertj.core.api.Assertions.assertThat(knowsCopies).isEqualTo(2);
        org.assertj.core.api.Assertions.assertThat(relativeCopiesInsideKnows).isZero();
    }

    @Test
    void expand_shouldValidateInput() throws Exception {
        String payload = """
            {
              "seeds": [],
              "direction": "OUTBOUND"
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
    }

    @Test
    void shortestPath_shouldReturnPath() throws Exception {
        String payload = """
            {
              "source": {"type": "PARTY_RK", "value": "PARTY_1001"},
              "target": {"type": "PARTY_RK", "value": "PARTY_1003"},
              "relationFamily": "PERSON_KNOWS_PERSON",
              "direction": "OUTBOUND",
              "maxDepth": 4
            }
            """;

        mockMvc.perform(post("/api/v1/graph/shortest-path")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.path.hopCount").value(2))
            .andExpect(jsonPath("$.path.orderedNodeIds[0]").value("N_PARTY_1001"))
            .andExpect(jsonPath("$.path.orderedNodeIds[2]").value("N_PARTY_1003"))
            .andExpect(jsonPath("$.meta.source").value("DUCKPGQ"));
    }

    @Test
    void shortestPath_shouldReturnNotFoundWhenNoPath() throws Exception {
        String payload = """
            {
              "source": {"type": "PARTY_RK", "value": "PARTY_1001"},
              "target": {"type": "PARTY_RK", "value": "PARTY_1003"},
              "relationFamily": "PERSON_SAME_CITY_PERSON",
              "direction": "OUTBOUND",
              "maxDepth": 2
            }
            """;

        mockMvc.perform(post("/api/v1/graph/shortest-path")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.code").value("NOT_FOUND"));
    }

    @Test
    void shortestPath_shouldTraverseUndirectedRelativeEdgeInReverseDirection() throws Exception {
        String payload = """
            {
              "source": {"type": "PARTY_RK", "value": "PARTY_1004"},
              "target": {"type": "PARTY_RK", "value": "PARTY_1001"},
              "relationFamily": "PERSON_RELATIVE_PERSON",
              "direction": "OUTBOUND",
              "maxDepth": 2
            }
            """;

        mockMvc.perform(post("/api/v1/graph/shortest-path")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.path.hopCount").value(1))
            .andExpect(jsonPath("$.path.orderedNodeIds[0]").value("N_PARTY_1004"))
            .andExpect(jsonPath("$.path.orderedNodeIds[1]").value("N_PARTY_1001"))
            .andExpect(jsonPath("$.meta.source").value("DUCKPGQ"));
    }

    @Test
    void dictionary_shouldReturnLegendData() throws Exception {
        mockMvc.perform(get("/api/v1/graph/dictionary"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.edgeTypes", hasItem("KNOWS")))
            .andExpect(jsonPath("$.nodeStatuses", hasItem("BLACKLIST")));
    }

    @Test
    void expand_shouldIncludeUndirectedEdgeForInboundRequest() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "PARTY_RK", "value": "PARTY_1001"}
              ],
              "relationFamily": "PERSON_RELATIVE_PERSON",
              "direction": "INBOUND",
              "maxNeighborsPerSeed": 10,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.edges[*].type", hasItem("RELATIVE")));
    }

    @Test
    void export_shouldReturnCsvAttachment() throws Exception {
        String payload = """
            {
              "nodes": [
                {
                  "nodeId": "N1",
                  "displayName": "Node 1",
                  "identifiers": {"party_rk":"PARTY_1"},
                  "statuses": ["VIP"],
                  "attributes": {"a":1}
                }
              ],
              "edges": [
                {
                  "edgeId": "E1",
                  "fromNodeId": "N1",
                  "toNodeId": "N2",
                  "type": "KNOWS",
                  "directed": true,
                  "weight": 10.5,
                  "attributes": {"currency":"RUB"}
                }
              ]
            }
            """;

        mockMvc.perform(post("/api/v1/graph/export?format=CSV")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(header().string("Content-Disposition", containsString("graph-export.csv")))
            .andExpect(content().contentType("text/csv"))
            .andExpect(content().string(containsString("section,node_id,display_name")));
    }

    @Test
    void export_shouldReturnNdjsonAttachment() throws Exception {
        String payload = """
            {
              "nodes": [
                {
                  "nodeId": "N1",
                  "displayName": "Node 1"
                }
              ],
              "edges": []
            }
            """;

        mockMvc.perform(post("/api/v1/graph/export?format=NDJSON")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(header().string("Content-Disposition", containsString("graph-export.ndjson")))
            .andExpect(content().contentType("application/x-ndjson"))
            .andExpect(content().string(containsString("\"kind\":\"node\"")));
    }

    @Test
    void export_shouldReturnBadRequestOnUnknownFormat() throws Exception {
        String payload = """
            {
              "nodes": [
                {
                  "nodeId": "N1",
                  "displayName": "Node 1",
                  "identifiers": {"party_rk":"PARTY_1"},
                  "statuses": ["VIP"],
                  "attributes": {"a":1}
                }
              ],
              "edges": []
            }
            """;

        mockMvc.perform(post("/api/v1/graph/export?format=XML")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }
}
