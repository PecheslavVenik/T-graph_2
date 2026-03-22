ALTER TABLE g_nodes ADD COLUMN IF NOT EXISTS city VARCHAR;
ALTER TABLE g_nodes ADD COLUMN IF NOT EXISTS pagerank_score DOUBLE DEFAULT 0;
ALTER TABLE g_nodes ADD COLUMN IF NOT EXISTS hub_score DOUBLE DEFAULT 0;

ALTER TABLE g_edges ADD COLUMN IF NOT EXISTS relation_family VARCHAR DEFAULT 'ALL_RELATIONS';
ALTER TABLE g_edges ADD COLUMN IF NOT EXISTS strength_score DOUBLE DEFAULT 0;
ALTER TABLE g_edges ADD COLUMN IF NOT EXISTS evidence_count BIGINT DEFAULT 0;

UPDATE g_nodes
SET
    city = CASE node_id
        WHEN 'N_PARTY_1001' THEN 'Vladivostok'
        WHEN 'N_PARTY_1002' THEN 'Vladivostok'
        WHEN 'N_PARTY_1003' THEN 'Khabarovsk'
        WHEN 'N_PARTY_1004' THEN 'Khabarovsk'
        ELSE city
    END,
    pagerank_score = CASE node_id
        WHEN 'N_PARTY_1001' THEN 0.82
        WHEN 'N_PARTY_1002' THEN 0.76
        WHEN 'N_PARTY_1003' THEN 0.54
        WHEN 'N_PARTY_1004' THEN 0.61
        ELSE pagerank_score
    END,
    hub_score = CASE node_id
        WHEN 'N_PARTY_1001' THEN 0.28
        WHEN 'N_PARTY_1002' THEN 0.67
        WHEN 'N_PARTY_1003' THEN 0.24
        WHEN 'N_PARTY_1004' THEN 0.33
        ELSE hub_score
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE node_id IN ('N_PARTY_1001', 'N_PARTY_1002', 'N_PARTY_1003', 'N_PARTY_1004');

UPDATE g_edges
SET
    edge_type = 'KNOWS',
    directed = FALSE,
    relation_family = 'PERSON_KNOWS_PERSON',
    strength_score = 0.96,
    evidence_count = 7,
    tx_count = 7,
    tx_sum = 0,
    attrs_json = '{"confidence":0.96,"evidence":"shared_devices"}',
    updated_at = CURRENT_TIMESTAMP
WHERE edge_id = 'E_TX_1001_1002';

UPDATE g_edges
SET
    edge_type = 'KNOWS',
    directed = FALSE,
    relation_family = 'PERSON_KNOWS_PERSON',
    strength_score = 0.71,
    evidence_count = 4,
    tx_count = 4,
    tx_sum = 0,
    attrs_json = '{"confidence":0.71,"evidence":"shared_contacts"}',
    updated_at = CURRENT_TIMESTAMP
WHERE edge_id = 'E_TK_1002_1003';

UPDATE g_edges
SET
    edge_type = 'KNOWS',
    directed = FALSE,
    relation_family = 'PERSON_KNOWS_PERSON',
    strength_score = 0.64,
    evidence_count = 3,
    tx_count = 3,
    tx_sum = 0,
    attrs_json = '{"confidence":0.64,"evidence":"manual_review"}',
    updated_at = CURRENT_TIMESTAMP
WHERE edge_id = 'E_TX_1003_1004';

UPDATE g_edges
SET
    edge_type = 'RELATIVE',
    directed = FALSE,
    relation_family = 'PERSON_RELATIVE_PERSON',
    strength_score = 0.89,
    evidence_count = 2,
    tx_count = 0,
    tx_sum = 0,
    attrs_json = '{"degree":1,"confidence":0.89}',
    updated_at = CURRENT_TIMESTAMP
WHERE edge_id = 'E_REL_1001_1004';

INSERT INTO g_edges (
    edge_id,
    from_node_id,
    to_node_id,
    edge_type,
    directed,
    tx_count,
    tx_sum,
    relation_family,
    strength_score,
    evidence_count,
    attrs_json
)
SELECT
    'E_CITY_1002_1004',
    'N_PARTY_1002',
    'N_PARTY_1004',
    'SAME_CITY',
    FALSE,
    0,
    0,
    'PERSON_SAME_CITY_PERSON',
    0.58,
    1,
    '{"city":"Vladivostok","confidence":0.58}'
WHERE NOT EXISTS (
    SELECT 1
    FROM g_edges
    WHERE edge_id = 'E_CITY_1002_1004'
);

DELETE FROM g_pgq_edges;

INSERT INTO g_pgq_edges (
    pgq_edge_id,
    edge_id,
    from_node_id,
    to_node_id,
    traversal_from_node_id,
    traversal_to_node_id,
    edge_type,
    directed,
    tx_count,
    tx_sum,
    attrs_json,
    created_at,
    updated_at
)
SELECT
    edge_id || ':fwd',
    edge_id,
    from_node_id,
    to_node_id,
    from_node_id,
    to_node_id,
    edge_type,
    directed,
    tx_count,
    tx_sum,
    attrs_json,
    created_at,
    updated_at
FROM g_edges;

INSERT INTO g_pgq_edges (
    pgq_edge_id,
    edge_id,
    from_node_id,
    to_node_id,
    traversal_from_node_id,
    traversal_to_node_id,
    edge_type,
    directed,
    tx_count,
    tx_sum,
    attrs_json,
    created_at,
    updated_at
)
SELECT
    edge_id || ':rev',
    edge_id,
    from_node_id,
    to_node_id,
    to_node_id,
    from_node_id,
    edge_type,
    directed,
    tx_count,
    tx_sum,
    attrs_json,
    created_at,
    updated_at
FROM g_edges
WHERE directed = FALSE;
