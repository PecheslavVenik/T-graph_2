INSERT INTO g_nodes (
    node_id,
    full_name,
    attrs_json,
    node_type,
    display_name,
    source_system
)
SELECT
    'N_ACC_2001',
    NULL,
    '{"currency":"RUB","account_status":"ACTIVE","bank_code":"044525225"}',
    'ACCOUNT',
    'Account **** 2001',
    'core_banking'
WHERE NOT EXISTS (
    SELECT 1 FROM g_nodes WHERE node_id = 'N_ACC_2001'
);

INSERT INTO g_nodes (
    node_id,
    full_name,
    attrs_json,
    node_type,
    display_name,
    source_system
)
SELECT
    'N_ACC_2002',
    NULL,
    '{"currency":"RUB","account_status":"ACTIVE","bank_code":"044525225"}',
    'ACCOUNT',
    'Account **** 2002',
    'core_banking'
WHERE NOT EXISTS (
    SELECT 1 FROM g_nodes WHERE node_id = 'N_ACC_2002'
);

INSERT INTO g_nodes (
    node_id,
    full_name,
    attrs_json,
    node_type,
    display_name,
    source_system
)
SELECT
    'N_COMP_3001',
    NULL,
    '{"country":"RU","industry":"Wholesale","risk_score":0.61}',
    'COMPANY',
    'OOO Romashka',
    'kyb_registry'
WHERE NOT EXISTS (
    SELECT 1 FROM g_nodes WHERE node_id = 'N_COMP_3001'
);

INSERT INTO g_nodes (
    node_id,
    full_name,
    attrs_json,
    node_type,
    display_name,
    source_system
)
SELECT
    'N_DEV_4001',
    NULL,
    '{"device_kind":"ANDROID","geo_country":"RU"}',
    'DEVICE',
    'Android device D-4001',
    'mobile_sdk'
WHERE NOT EXISTS (
    SELECT 1 FROM g_nodes WHERE node_id = 'N_DEV_4001'
);

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT 'N_ACC_2001', 'ACCOUNT_NO', '40817810000000002001'
WHERE NOT EXISTS (
    SELECT 1 FROM g_identifiers
    WHERE node_id = 'N_ACC_2001' AND id_type = 'ACCOUNT_NO' AND id_value = '40817810000000002001'
);

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT 'N_ACC_2001', 'CARD_MASK', '220012******2001'
WHERE NOT EXISTS (
    SELECT 1 FROM g_identifiers
    WHERE node_id = 'N_ACC_2001' AND id_type = 'CARD_MASK' AND id_value = '220012******2001'
);

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT 'N_ACC_2002', 'ACCOUNT_NO', '40817810000000002002'
WHERE NOT EXISTS (
    SELECT 1 FROM g_identifiers
    WHERE node_id = 'N_ACC_2002' AND id_type = 'ACCOUNT_NO' AND id_value = '40817810000000002002'
);

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT 'N_ACC_2002', 'CARD_MASK', '220012******2002'
WHERE NOT EXISTS (
    SELECT 1 FROM g_identifiers
    WHERE node_id = 'N_ACC_2002' AND id_type = 'CARD_MASK' AND id_value = '220012******2002'
);

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT 'N_COMP_3001', 'TAX_ID', '7701234567'
WHERE NOT EXISTS (
    SELECT 1 FROM g_identifiers
    WHERE node_id = 'N_COMP_3001' AND id_type = 'TAX_ID' AND id_value = '7701234567'
);

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT 'N_DEV_4001', 'DEVICE_ID', 'D-4001'
WHERE NOT EXISTS (
    SELECT 1 FROM g_identifiers
    WHERE node_id = 'N_DEV_4001' AND id_type = 'DEVICE_ID' AND id_value = 'D-4001'
);

INSERT INTO g_identifiers (node_id, id_type, id_value)
SELECT 'N_DEV_4001', 'IP', '10.10.10.10'
WHERE NOT EXISTS (
    SELECT 1 FROM g_identifiers
    WHERE node_id = 'N_DEV_4001' AND id_type = 'IP' AND id_value = '10.10.10.10'
);

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
    source_system,
    first_seen_at,
    last_seen_at,
    attrs_json
)
SELECT
    'E_OWNS_1001_2001',
    'N_PARTY_1001',
    'N_ACC_2001',
    'OWNS',
    TRUE,
    0,
    0,
    'CUSTOMER_OWNERSHIP',
    0.99,
    1,
    'core_banking',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    '{"ownership_kind":"PRIMARY_OWNER","confidence":0.99}'
WHERE NOT EXISTS (
    SELECT 1 FROM g_edges WHERE edge_id = 'E_OWNS_1001_2001'
);

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
    source_system,
    first_seen_at,
    last_seen_at,
    attrs_json
)
SELECT
    'E_OWNS_1002_2002',
    'N_PARTY_1002',
    'N_ACC_2002',
    'OWNS',
    TRUE,
    0,
    0,
    'CUSTOMER_OWNERSHIP',
    0.98,
    1,
    'core_banking',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    '{"ownership_kind":"PRIMARY_OWNER","confidence":0.98}'
WHERE NOT EXISTS (
    SELECT 1 FROM g_edges WHERE edge_id = 'E_OWNS_1002_2002'
);

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
    source_system,
    first_seen_at,
    last_seen_at,
    attrs_json
)
SELECT
    'E_TRANS_2001_2002',
    'N_ACC_2001',
    'N_ACC_2002',
    'TRANSFERS_TO',
    TRUE,
    3,
    125000.00,
    'ACCOUNT_FLOW',
    0.78,
    3,
    'payments_ledger',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    '{"currency":"RUB","channel":"SBP","scenario":"burst_transfers"}'
WHERE NOT EXISTS (
    SELECT 1 FROM g_edges WHERE edge_id = 'E_TRANS_2001_2002'
);

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
    source_system,
    first_seen_at,
    last_seen_at,
    attrs_json
)
SELECT
    'E_USES_DEVICE_1002_4001',
    'N_PARTY_1002',
    'N_DEV_4001',
    'USES_DEVICE',
    TRUE,
    0,
    0,
    'SHARED_INFRASTRUCTURE',
    0.84,
    2,
    'mobile_sdk',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    '{"evidence":"login+otp","confidence":0.84}'
WHERE NOT EXISTS (
    SELECT 1 FROM g_edges WHERE edge_id = 'E_USES_DEVICE_1002_4001'
);

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
    source_system,
    first_seen_at,
    last_seen_at,
    attrs_json
)
SELECT
    'E_BENEFICIAL_1001_3001',
    'N_PARTY_1001',
    'N_COMP_3001',
    'BENEFICIAL_OWNS',
    TRUE,
    0,
    0,
    'CORPORATE_CONTROL',
    0.88,
    2,
    'ubo_registry',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    '{"ownership_share":0.75,"control_kind":"DIRECT"}'
WHERE NOT EXISTS (
    SELECT 1 FROM g_edges WHERE edge_id = 'E_BENEFICIAL_1001_3001'
);
