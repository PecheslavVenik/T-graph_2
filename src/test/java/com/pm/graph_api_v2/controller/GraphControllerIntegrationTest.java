package com.pm.graph_api_v2.controller;

import com.pm.graph_api_v2.util.GraphRelationFamilies;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
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
    void expand_shouldExcludeKnownNodesFromNodesButReturnNewEdgesToThem() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "NODE_ID", "value": "N_PARTY_1001"}
              ],
              "direction": "OUTBOUND",
              "filters": {
                "relationFamilies": ["CUSTOMER_OWNERSHIP"]
              },
              "exclude": {
                "nodeIds": ["N_PARTY_1001", "N_ACC_2001"],
                "edgeIds": []
              },
              "maxNeighborsPerSeed": 50,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(0))
            .andExpect(jsonPath("$.edges.length()").value(1))
            .andExpect(jsonPath("$.edges[0].edgeId").value("E_OWNS_1001_2001"))
            .andExpect(jsonPath("$.edges[0].toNodeId").value("N_ACC_2001"));
    }

    @Test
    void expandPreview_shouldUseSameExcludeAndFilterContract() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "NODE_ID", "value": "N_PARTY_1001"}
              ],
              "direction": "OUTBOUND",
              "filters": {
                "relationFamilies": ["CUSTOMER_OWNERSHIP"],
                "nodeTypes": ["ACCOUNT"]
              },
              "exclude": {
                "nodeIds": ["N_PARTY_1001", "N_ACC_2001"],
                "edgeIds": []
              },
              "maxNeighborsPerSeed": 50,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand/preview")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.summary.adjacentEdgeCount").value(1))
            .andExpect(jsonPath("$.summary.uniqueNeighborCount").value(1))
            .andExpect(jsonPath("$.summary.newNodeCount").value(0))
            .andExpect(jsonPath("$.summary.newEdgeCount").value(1))
            .andExpect(jsonPath("$.facets.relationFamilies[0].key").value("CUSTOMER_OWNERSHIP"))
            .andExpect(jsonPath("$.facets.edgeTypes[0].key").value("OWNS"))
            .andExpect(jsonPath("$.facets.neighborNodeTypes[0].key").value("ACCOUNT"))
            .andExpect(jsonPath("$.expandPreview.wouldTruncateByNeighborBudget").value(false));
    }

    @Test
    void expand_shouldExcludeKnownEdgesFromResult() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "NODE_ID", "value": "N_PARTY_1001"}
              ],
              "direction": "OUTBOUND",
              "filters": {
                "relationFamilies": ["CUSTOMER_OWNERSHIP"]
              },
              "exclude": {
                "nodeIds": ["N_PARTY_1001", "N_ACC_2001"],
                "edgeIds": ["E_OWNS_1001_2001"]
              },
              "maxNeighborsPerSeed": 50,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(0))
            .andExpect(jsonPath("$.edges.length()").value(0));
    }

    @Test
    void expand_shouldSupportMultipleRelationFamilies() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "NODE_ID", "value": "N_PARTY_1001"}
              ],
              "direction": "OUTBOUND",
              "filters": {
                "relationFamilies": ["CUSTOMER_OWNERSHIP", "CORPORATE_CONTROL"]
              },
              "exclude": {
                "nodeIds": ["N_PARTY_1001"],
                "edgeIds": []
              },
              "maxNeighborsPerSeed": 50,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(2))
            .andExpect(jsonPath("$.edges.length()").value(2))
            .andExpect(jsonPath("$.edges[*].relationFamily", hasItem("CUSTOMER_OWNERSHIP")))
            .andExpect(jsonPath("$.edges[*].relationFamily", hasItem("CORPORATE_CONTROL")));
    }

    @Test
    void expand_shouldApplyNodeAndEdgeAttributeFilters() throws Exception {
        String nodeAttributePayload = """
            {
              "seeds": [
                {"type": "NODE_ID", "value": "N_PARTY_1002"}
              ],
              "direction": "OUTBOUND",
              "filters": {
                "relationFamilies": ["PERSON_KNOWS_PERSON"],
                "nodeAttributes": {
                  "city": {"eq": "Khabarovsk"}
                }
              },
              "exclude": {
                "nodeIds": ["N_PARTY_1002"],
                "edgeIds": []
              },
              "maxNeighborsPerSeed": 50,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(nodeAttributePayload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(1))
            .andExpect(jsonPath("$.nodes[0].nodeId").value("N_PARTY_1003"))
            .andExpect(jsonPath("$.edges.length()").value(1));

        String edgeAttributePayload = """
            {
              "seeds": [
                {"type": "NODE_ID", "value": "N_ACC_2001"}
              ],
              "direction": "OUTBOUND",
              "filters": {
                "relationFamilies": ["ACCOUNT_FLOW"],
                "edgeAttributes": {
                  "amount": {"gte": 100000, "lte": 130000}
                }
              },
              "exclude": {
                "nodeIds": ["N_ACC_2001"],
                "edgeIds": []
              },
              "maxNeighborsPerSeed": 50,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(edgeAttributePayload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(1))
            .andExpect(jsonPath("$.nodes[0].nodeId").value("N_ACC_2002"))
            .andExpect(jsonPath("$.edges.length()").value(1))
            .andExpect(jsonPath("$.edges[0].edgeId").value("E_TRANS_2001_2002"));
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
            .andExpect(jsonPath("$.meta.relationFamily").value("ALL_RELATIONS"));
    }

    @Test
    void pgqProjection_shouldBuildRelationSpecificTables() {
        Integer allKnowsCopies = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM g_pgq_edges WHERE edge_id = 'E_TX_1001_1002'",
            Integer.class
        );
        Integer knowsCopies = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM " + GraphRelationFamilies.projectionTableName("PERSON_KNOWS_PERSON") + " WHERE edge_id = 'E_TX_1001_1002'",
            Integer.class
        );
        Integer relativeCopiesInsideKnows = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM " + GraphRelationFamilies.projectionTableName("PERSON_KNOWS_PERSON") + " WHERE edge_id = 'E_REL_1001_1004'",
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
    void query_shouldStartInvestigationFromSeedSql() throws Exception {
        String payload = """
            {
              "sql": "select node_id from g_nodes where node_id = 'N_PARTY_1001'",
              "resultMode": "SEEDS",
              "relationFamily": "CUSTOMER_OWNERSHIP",
              "direction": "OUTBOUND",
              "maxNeighborsPerSeed": 10,
              "maxNodes": 100,
              "maxEdges": 100,
              "includeAttributes": true
            }
            """;

        mockMvc.perform(post("/api/v1/graph/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes[*].nodeId", hasItem("N_PARTY_1001")))
            .andExpect(jsonPath("$.nodes[*].nodeId", hasItem("N_ACC_2001")))
            .andExpect(jsonPath("$.edges[*].edgeId", hasItem("E_OWNS_1001_2001")))
            .andExpect(jsonPath("$.meta.relationFamily").value("CUSTOMER_OWNERSHIP"))
            .andExpect(jsonPath("$.meta.rankingStrategy").value("SQL_SEED_QUERY"));
    }

    @Test
    void query_shouldReturnGraphSliceFromEdgeSql() throws Exception {
        String payload = """
            {
              "sql": "select edge_id from g_edges where edge_id = 'E_OWNS_1001_2001'",
              "resultMode": "GRAPH",
              "maxNodes": 100,
              "maxEdges": 100,
              "includeAttributes": true
            }
            """;

        mockMvc.perform(post("/api/v1/graph/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes[*].nodeId", hasItem("N_PARTY_1001")))
            .andExpect(jsonPath("$.nodes[*].nodeId", hasItem("N_ACC_2001")))
            .andExpect(jsonPath("$.edges.length()").value(1))
            .andExpect(jsonPath("$.edges[0].edgeId").value("E_OWNS_1001_2001"))
            .andExpect(jsonPath("$.meta.rankingStrategy").value("SQL_GRAPH_QUERY"));
    }

    @Test
    void query_shouldRejectUnsafeSql() throws Exception {
        String payload = """
            {
              "sql": "drop table g_nodes",
              "resultMode": "SEEDS"
            }
            """;

        mockMvc.perform(post("/api/v1/graph/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }

    @Test
    void importPreview_shouldParseCsvGraph() throws Exception {
        String csv = """
            record_type,node_id,node_type,display_name,from_node_id,to_node_id,edge_id,edge_type,relation_family,directed
            NODE,N_IMPORT_PREVIEW_1,PERSON,Imported Person,,,,,,
            NODE,N_IMPORT_PREVIEW_2,ACCOUNT,Imported Account,,,,,,
            EDGE,,,,N_IMPORT_PREVIEW_1,N_IMPORT_PREVIEW_2,E_IMPORT_PREVIEW_1,OWNS,CUSTOMER_OWNERSHIP,true
            """;

        mockMvc.perform(multipart("/api/v1/graph/import/preview")
                .file(csvFile(csv)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("PREVIEW"))
            .andExpect(jsonPath("$.parsedNodeCount").value(2))
            .andExpect(jsonPath("$.parsedEdgeCount").value(1))
            .andExpect(jsonPath("$.invalidRowCount").value(0))
            .andExpect(jsonPath("$.insertedNodeCount").value(0))
            .andExpect(jsonPath("$.insertedEdgeCount").value(0));
    }

    @Test
    void importPreview_shouldRejectBadNumericAndDateFields() throws Exception {
        String csv = """
            record_type,node_id,node_type,display_name,pagerank_score,from_node_id,to_node_id,edge_id,edge_type,relation_family,directed,tx_count,tx_sum,first_seen_at
            NODE,N_IMPORT_BAD_SCORE,PERSON,Bad Score,not-a-number,,,,,,,,,
            EDGE,,,,,N_PARTY_1001,N_PARTY_1002,E_IMPORT_BAD_EDGE,KNOWS,PERSON_KNOWS_PERSON,true,not-a-long,42.1,not-an-instant
            """;

        mockMvc.perform(multipart("/api/v1/graph/import/preview")
                .file(csvFile(csv)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("PREVIEW"))
            .andExpect(jsonPath("$.parsedNodeCount").value(0))
            .andExpect(jsonPath("$.parsedEdgeCount").value(0))
            .andExpect(jsonPath("$.invalidRowCount").value(2))
            .andExpect(jsonPath("$.errors.length()").value(3))
            .andExpect(jsonPath("$.errors[0].rowNumber").value(2))
            .andExpect(jsonPath("$.errors[0].field").value("pagerank_score"))
            .andExpect(jsonPath("$.errors[0].value").value("not-a-number"))
            .andExpect(jsonPath("$.errors[1].rowNumber").value(3))
            .andExpect(jsonPath("$.errors[1].field").value("tx_count"))
            .andExpect(jsonPath("$.errors[1].value").value("not-a-long"))
            .andExpect(jsonPath("$.errors[2].rowNumber").value(3))
            .andExpect(jsonPath("$.errors[2].field").value("first_seen_at"))
            .andExpect(jsonPath("$.errors[2].value").value("not-an-instant"));
    }

    @Test
    void importCommit_shouldReturnBadRequestWithFieldErrorsForInvalidCsv() throws Exception {
        String csv = """
            record_type,node_id,node_type,display_name,pagerank_score,from_node_id,to_node_id,edge_id,edge_type,relation_family,directed,tx_count,tx_sum,first_seen_at
            NODE,N_IMPORT_BAD_COMMIT,PERSON,Bad Commit,not-a-number,,,,,,,,,
            EDGE,,,,,N_PARTY_1001,N_PARTY_1002,E_IMPORT_BAD_COMMIT,KNOWS,PERSON_KNOWS_PERSON,true,not-a-long,42.1,not-an-instant
            """;

        mockMvc.perform(multipart("/api/v1/graph/import/commit")
                .file(csvFile(csv)))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("BAD_REQUEST"))
            .andExpect(jsonPath("$.details.length()").value(3))
            .andExpect(jsonPath("$.details[0].field").value("pagerank_score"))
            .andExpect(jsonPath("$.details[0].value").value("not-a-number"))
            .andExpect(jsonPath("$.details[1].field").value("tx_count"))
            .andExpect(jsonPath("$.details[1].value").value("not-a-long"))
            .andExpect(jsonPath("$.details[2].field").value("first_seen_at"))
            .andExpect(jsonPath("$.details[2].value").value("not-an-instant"));
    }

    @Test
    void importCommit_shouldImportCsvGraphAndMakeItExpandable() throws Exception {
        String csv = """
            record_type,node_id,node_type,display_name,party_rk,account_no,from_node_id,to_node_id,edge_id,edge_type,relation_family,directed
            NODE,N_IMPORT_COMMIT_1,PERSON,Imported Customer,PARTY_IMPORT_1,,,,,,,
            NODE,N_IMPORT_COMMIT_2,ACCOUNT,Imported Account,,40817810000000999999,,,,,,
            EDGE,,,,,,N_IMPORT_COMMIT_1,N_IMPORT_COMMIT_2,E_IMPORT_COMMIT_1,OWNS,CUSTOMER_OWNERSHIP,true
            """;

        mockMvc.perform(multipart("/api/v1/graph/import/commit")
                .file(csvFile(csv)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("COMMITTED"))
            .andExpect(jsonPath("$.parsedNodeCount").value(2))
            .andExpect(jsonPath("$.parsedEdgeCount").value(1))
            .andExpect(jsonPath("$.invalidRowCount").value(0))
            .andExpect(jsonPath("$.insertedNodeCount").value(2))
            .andExpect(jsonPath("$.insertedEdgeCount").value(1));

        String expandPayload = """
            {
              "seeds": [
                {"type": "NODE_ID", "value": "N_IMPORT_COMMIT_1"}
              ],
              "relationFamily": "CUSTOMER_OWNERSHIP",
              "direction": "OUTBOUND",
              "maxNeighborsPerSeed": 10,
              "maxNodes": 100,
              "maxEdges": 100,
              "includeAttributes": true
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(expandPayload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes[*].nodeId", hasItem("N_IMPORT_COMMIT_1")))
            .andExpect(jsonPath("$.nodes[*].nodeId", hasItem("N_IMPORT_COMMIT_2")))
            .andExpect(jsonPath("$.edges[*].edgeId", hasItem("E_IMPORT_COMMIT_1")))
            .andExpect(jsonPath("$.meta.source").value("DUCKPGQ"));
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
    void health_shouldReportDuckPgqLoadedUp() throws Exception {
        mockMvc.perform(get("/actuator/health"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.components.duckpgqLoaded.status").value("UP"))
            .andExpect(jsonPath("$.components.duckpgqLoaded.details['duckpgq.loaded']").value(true));
    }

    @Test
    void cors_shouldAllowViteLoopbackDevOrigin() throws Exception {
        mockMvc.perform(options("/api/v1/graph/dictionary")
                .header("Origin", "http://127.0.0.1:5173")
                .header("Access-Control-Request-Method", "GET"))
            .andExpect(status().isOk())
            .andExpect(header().string("Access-Control-Allow-Origin", "http://127.0.0.1:5173"));
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
    void nodeSummary_shouldReturnBadRequestForBlankNodeId() throws Exception {
        mockMvc.perform(get("/api/v1/graph/node-summary")
                .param("nodeId", " "))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
            .andExpect(jsonPath("$.details[0].field").value("nodeId"));
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
    void nodeSearch_shouldFindNodeByName() throws Exception {
        mockMvc.perform(get("/api/v1/graph/nodes/search")
                .param("query", "alice")
                .param("nodeType", "person")
                .param("limit", "5"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(1))
            .andExpect(jsonPath("$.nodes[0].nodeId").value("N_PARTY_1001"))
            .andExpect(jsonPath("$.nodes[0].displayName").value("Alice Ivanova"))
            .andExpect(jsonPath("$.nodes[0].statuses", hasItem("BLACKLIST")))
            .andExpect(jsonPath("$.meta.query").value("alice"))
            .andExpect(jsonPath("$.meta.nodeType").value("PERSON"))
            .andExpect(jsonPath("$.meta.limit").value(5))
            .andExpect(jsonPath("$.meta.returnedNodeCount").value(1))
            .andExpect(jsonPath("$.meta.truncated").value(false));
    }

    @Test
    void nodeSearch_shouldSearchIdentifiersAndReportTruncation() throws Exception {
        mockMvc.perform(get("/api/v1/graph/nodes/search")
                .param("query", "PARTY")
                .param("nodeType", "PERSON")
                .param("limit", "1")
                .param("includeAttributes", "false"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.nodes.length()").value(1))
            .andExpect(jsonPath("$.nodes[0].nodeType").value("PERSON"))
            .andExpect(jsonPath("$.nodes[0].identifiers.party_rk", containsString("PARTY_")))
            .andExpect(jsonPath("$.nodes[0].attributes").isMap())
            .andExpect(jsonPath("$.nodes[0].attributes", not(hasKey("pagerankScore"))))
            .andExpect(jsonPath("$.meta.returnedNodeCount").value(1))
            .andExpect(jsonPath("$.meta.truncated").value(true));
    }

    @Test
    void nodeSearch_shouldValidateLimit() throws Exception {
        mockMvc.perform(get("/api/v1/graph/nodes/search")
                .param("query", "Alice")
                .param("limit", "101"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }

    @Test
    void nodeSearch_shouldValidateQuery() throws Exception {
        mockMvc.perform(get("/api/v1/graph/nodes/search")
                .param("query", " "))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
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
    void expand_shouldAcceptLowercaseSeedTypeAliasAndEdgeTypeFilter() throws Exception {
        String payload = """
            {
              "seeds": [
                {"type": "party", "value": "PARTY_1001"}
              ],
              "relationFamily": "ALL_RELATIONS",
              "edgeTypes": ["beneficial_owns"],
              "direction": "OUTBOUND",
              "maxNeighborsPerSeed": 10,
              "maxNodes": 100,
              "maxEdges": 100
            }
            """;

        mockMvc.perform(post("/api/v1/graph/expand")
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.edges.length()").value(1))
            .andExpect(jsonPath("$.edges[0].type").value("BENEFICIAL_OWNS"))
            .andExpect(jsonPath("$.meta.relationFamily").value("ALL_RELATIONS"));
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

    private MockMultipartFile csvFile(String csv) {
        return new MockMultipartFile(
            "file",
            "graph-import.csv",
            "text/csv",
            csv.getBytes(StandardCharsets.UTF_8)
        );
    }
}
