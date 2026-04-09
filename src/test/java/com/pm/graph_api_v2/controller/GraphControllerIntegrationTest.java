package com.pm.graph_api_v2.controller;

import com.pm.graph_api_v2.dto.GraphRelationFamily;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
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
            .andExpect(jsonPath("$.meta.rankingStrategy").value("GENERIC_ONE_HOP_RANKING"))
            .andExpect(jsonPath("$.meta.warnings", hasItem("Per-seed neighbor budget filtered lower-ranked neighbors")))
            .andExpect(jsonPath("$.edges[*].type", hasItem("KNOWS")));
    }

    @Test
    void expand_withoutRelationFamily_shouldUseConfiguredDefault() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "PARTY_RK", "value": "PARTY_1002"}
              ],
              "direction": "OUTBOUND",
              "maxNeighborsPerSeed": 1,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.meta.relationFamily").value("PERSON_KNOWS_PERSON"));
    }

    @Test
    void pgqProjection_shouldBuildRelationSpecificTables() {
        Integer allKnowsCopies = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM g_pgq_edges WHERE edge_id = 'E_TX_1001_1002'",
            Integer.class
        );
        Integer knowsCopies = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM " + GraphRelationFamily.PERSON_KNOWS_PERSON.projectionTableName() + " WHERE edge_id = 'E_TX_1001_1002'",
            Integer.class
        );
        Integer relativeCopiesInsideKnows = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM " + GraphRelationFamily.PERSON_KNOWS_PERSON.projectionTableName() + " WHERE edge_id = 'E_REL_1001_1004'",
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
    void shortestPath_shouldKeepNodesInPathOrderForGenericData() throws Exception {
        String payload = """
            {
              "source": {"type": "PARTY_RK", "value": "PARTY_1001"},
              "target": {"type": "ACCOUNT_NO", "value": "40817810000000002001"},
              "relationFamily": "CUSTOMER_OWNERSHIP",
              "direction": "OUTBOUND",
              "maxDepth": 2
            }
            """;

        mockMvc.perform(post("/api/v1/graph/shortest-path")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.path.orderedNodeIds[0]").value("N_PARTY_1001"))
            .andExpect(jsonPath("$.path.orderedNodeIds[1]").value("N_ACC_2001"))
            .andExpect(jsonPath("$.nodes[0].nodeId").value("N_PARTY_1001"))
            .andExpect(jsonPath("$.nodes[1].nodeId").value("N_ACC_2001"));
    }

    @Test
    void dictionary_shouldReturnLegendData() throws Exception {
        mockMvc.perform(get("/api/v1/graph/dictionary"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.edgeTypes", hasItem("KNOWS")))
            .andExpect(jsonPath("$.edgeTypes", hasItem("TRANSFERS_TO")))
            .andExpect(jsonPath("$.relationFamilies", hasItem("PERSON_KNOWS_PERSON")))
            .andExpect(jsonPath("$.relationFamilies", hasItem("ACCOUNT_FLOW")))
            .andExpect(jsonPath("$.nodeTypes", hasItem("PERSON")))
            .andExpect(jsonPath("$.nodeTypes", hasItem("ACCOUNT")))
            .andExpect(jsonPath("$.nodeStatuses", hasItem("BLACKLIST")));
    }

    @Test
    void dictionary_shouldReturnNonEmptyStyleHints() throws Exception {
        mockMvc.perform(get("/api/v1/graph/dictionary"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.styleHints", hasKey("BLACKLIST")))
            .andExpect(jsonPath("$.styleHints.BLACKLIST", equalTo("legend:status:blacklist")))
            .andExpect(jsonPath("$.styleHints", hasKey("ACCOUNT")));
    }

    @Test
    void nodeSummary_shouldReturnOverviewForClickedNode() throws Exception {
        mockMvc.perform(get("/api/v1/graph/node-summary")
                .param("nodeId", "N_PARTY_1001"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.node.nodeId").value("N_PARTY_1001"))
            .andExpect(jsonPath("$.summary.requestedDirection").value("BOTH"))
            .andExpect(jsonPath("$.summary.relationFamily").value("ALL_RELATIONS"))
            .andExpect(jsonPath("$.summary.adjacentEdgeCount").value(4))
            .andExpect(jsonPath("$.summary.uniqueNeighborCount").value(4))
            .andExpect(jsonPath("$.summary.outboundEdgeCount").value(4))
            .andExpect(jsonPath("$.summary.inboundEdgeCount").value(2))
            .andExpect(jsonPath("$.relationFamilies[*].key", hasItem("CUSTOMER_OWNERSHIP")))
            .andExpect(jsonPath("$.relationFamilies[*].key", hasItem("CORPORATE_CONTROL")))
            .andExpect(jsonPath("$.edgeTypes[*].key", hasItem("OWNS")))
            .andExpect(jsonPath("$.edgeTypes[*].key", hasItem("BENEFICIAL_OWNS")))
            .andExpect(jsonPath("$.neighborNodeTypes[*].key", hasItem("PERSON")))
            .andExpect(jsonPath("$.expandPreview.defaultMaxNeighborsPerSeed").value(25))
            .andExpect(jsonPath("$.expandPreview.wouldTruncateByNeighborBudget").value(false));
    }

    @Test
    void nodeSummary_shouldRespectRelationFamilyAndDirectionFilter() throws Exception {
        mockMvc.perform(get("/api/v1/graph/node-summary")
                .param("nodeId", "N_PARTY_1001")
                .param("relationFamily", "CUSTOMER_OWNERSHIP")
                .param("direction", "OUTBOUND"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.summary.requestedDirection").value("OUTBOUND"))
            .andExpect(jsonPath("$.summary.relationFamily").value("CUSTOMER_OWNERSHIP"))
            .andExpect(jsonPath("$.summary.adjacentEdgeCount").value(1))
            .andExpect(jsonPath("$.summary.uniqueNeighborCount").value(1))
            .andExpect(jsonPath("$.summary.outboundEdgeCount").value(1))
            .andExpect(jsonPath("$.summary.inboundEdgeCount").value(0))
            .andExpect(jsonPath("$.relationFamilies.length()").value(1))
            .andExpect(jsonPath("$.relationFamilies[0].key").value("CUSTOMER_OWNERSHIP"))
            .andExpect(jsonPath("$.edgeTypes[0].key").value("OWNS"))
            .andExpect(jsonPath("$.neighborNodeTypes[0].key").value("ACCOUNT"));
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
    void expand_shouldResolveGenericAccountSeedAndReturnAccountFlow() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "ACCOUNT_NO", "value": "40817810000000002001"}
              ],
              "relationFamily": "ACCOUNT_FLOW",
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
            .andExpect(jsonPath("$.meta.relationFamily").value("ACCOUNT_FLOW"))
            .andExpect(jsonPath("$.edges[0].type").value("TRANSFERS_TO"))
            .andExpect(jsonPath("$.nodes[*].nodeType", hasItem("ACCOUNT")));
    }

    @Test
    void shortestPath_shouldResolveGenericCompanySeedByTaxId() throws Exception {
        String payload = """
            {
              "source": {"type": "PARTY_RK", "value": "PARTY_1001"},
              "target": {"type": "TAX_ID", "value": "7701234567"},
              "relationFamily": "CORPORATE_CONTROL",
              "direction": "OUTBOUND",
              "maxDepth": 2
            }
            """;

        mockMvc.perform(post("/api/v1/graph/shortest-path")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.path.hopCount").value(1))
            .andExpect(jsonPath("$.nodes[*].nodeType", hasItem("COMPANY")));
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
            .andExpect(content().string(containsString("section,node_id,node_type,display_name")));
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
