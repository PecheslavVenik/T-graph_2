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
    void expand_shouldReturnOneHopGraph() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "PARTY_RK", "value": "PARTY_1001"}
              ],
              "direction": "OUTBOUND",
              "maxNeighborsPerSeed": 10,
              "maxNodes": 100,
              "maxEdges": 100,
              "includeAttributes": true
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(3))
            .andExpect(jsonPath("$.edges.length()").value(2))
            .andExpect(jsonPath("$.meta.source").value("SQL_FALLBACK"))
            .andExpect(jsonPath("$.edges[*].type", hasItem("TRANSFER")));
    }

    @Test
    void pgqProjection_shouldDuplicateOnlyUndirectedEdges() {
        Integer undirectedCopies = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM g_pgq_edges WHERE edge_id = 'E_REL_1001_1004'",
            Integer.class
        );
        Integer directedCopies = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM g_pgq_edges WHERE edge_id = 'E_TX_1001_1002'",
            Integer.class
        );
        Integer reverseUndirected = jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*)
            FROM g_pgq_edges
            WHERE edge_id = 'E_REL_1001_1004'
              AND traversal_from_node_id = 'N_PARTY_1004'
              AND traversal_to_node_id = 'N_PARTY_1001'
            """,
            Integer.class
        );

        org.assertj.core.api.Assertions.assertThat(undirectedCopies).isEqualTo(2);
        org.assertj.core.api.Assertions.assertThat(directedCopies).isEqualTo(1);
        org.assertj.core.api.Assertions.assertThat(reverseUndirected).isEqualTo(1);
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
              "target": {"type": "PARTY_RK", "value": "PARTY_1004"},
              "direction": "OUTBOUND",
              "maxDepth": 4
            }
            """;

        mockMvc.perform(post("/api/v1/graph/shortest-path")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.path.hopCount").value(1))
            .andExpect(jsonPath("$.path.orderedNodeIds[0]").value("N_PARTY_1001"))
            .andExpect(jsonPath("$.path.orderedNodeIds[1]").value("N_PARTY_1004"));
    }

    @Test
    void shortestPath_shouldReturnNotFoundWhenNoPath() throws Exception {
        String payload = """
            {
              "source": {"type": "PARTY_RK", "value": "PARTY_1003"},
              "target": {"type": "PARTY_RK", "value": "PARTY_1001"},
              "direction": "OUTBOUND",
              "maxDepth": 1
            }
            """;

        mockMvc.perform(post("/api/v1/graph/shortest-path")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.code").value("NOT_FOUND"));
    }

    @Test
    void shortestPath_shouldTraverseUndirectedEdgeInReverseDirection() throws Exception {
        String payload = """
            {
              "source": {"type": "PARTY_RK", "value": "PARTY_1004"},
              "target": {"type": "PARTY_RK", "value": "PARTY_1001"},
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
            .andExpect(jsonPath("$.meta.source").value("SQL_FALLBACK"));
    }

    @Test
    void dictionary_shouldReturnLegendData() throws Exception {
        mockMvc.perform(get("/api/v1/graph/dictionary"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.edgeTypes", hasItem("TRANSFER")))
            .andExpect(jsonPath("$.nodeStatuses", hasItem("BLACKLIST")));
    }

    @Test
    void expand_shouldIncludeUndirectedEdgeForInboundRequest() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "PARTY_RK", "value": "PARTY_1001"}
              ],
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
    void expand_shouldMergeWithExistingGraph() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "PARTY_RK", "value": "PARTY_1001"}
              ],
              "direction": "OUTBOUND",
              "maxNeighborsPerSeed": 10,
              "maxNodes": 100,
              "maxEdges": 100,
              "includeAttributes": true,
              "existingGraph": {
                "nodes": [
                  {
                    "nodeId": "N_PARTY_1001",
                    "displayName": "Alice layout copy",
                    "identifiers": {"custom":"C1"},
                    "statuses": ["PINNED"],
                    "attributes": {"x":123,"y":456}
                  },
                  {
                    "nodeId": "N_LEGACY_1",
                    "displayName": "Legacy node",
                    "identifiers": {"legacy":"L1"},
                    "statuses": ["ARCHIVED"],
                    "attributes": {"x":1}
                  }
                ],
                "edges": [
                  {
                    "edgeId": "E_TX_1001_1002",
                    "fromNodeId": "N_PARTY_1001",
                    "toNodeId": "N_PARTY_1002",
                    "type": "TRANSFER",
                    "directed": true,
                    "weight": 1.0,
                    "attributes": {"selected": true, "legacy": "keep"}
                  },
                  {
                    "edgeId": "E_LEGACY_1",
                    "fromNodeId": "N_LEGACY_1",
                    "toNodeId": "N_PARTY_1001",
                    "type": "LINK",
                    "directed": false,
                    "attributes": {"selected": true}
                  }
                ],
                "meta": {
                  "truncated": true,
                  "executionMs": 1,
                  "source": "SQL_FALLBACK"
                }
              }
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(4))
            .andExpect(jsonPath("$.edges.length()").value(3))
            .andExpect(jsonPath("$.nodes[*].nodeId", hasItem("N_LEGACY_1")))
            .andExpect(jsonPath("$.edges[*].edgeId", hasItem("E_LEGACY_1")))
            .andExpect(jsonPath("$.nodes[?(@.nodeId=='N_PARTY_1001')].attributes.x", hasItem(123)))
            .andExpect(jsonPath("$.nodes[?(@.nodeId=='N_PARTY_1001')].statuses[*]", hasItem("PINNED")))
            .andExpect(jsonPath("$.nodes[?(@.nodeId=='N_PARTY_1001')].statuses[*]", hasItem("BLACKLIST")))
            .andExpect(jsonPath("$.edges[?(@.edgeId=='E_TX_1001_1002')].attributes.selected", hasItem(true)))
            .andExpect(jsonPath("$.edges[?(@.edgeId=='E_TX_1001_1002')].attributes.legacy", hasItem("keep")))
            .andExpect(jsonPath("$.meta.truncated").value(true));
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
                  "type": "TRANSFER",
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
