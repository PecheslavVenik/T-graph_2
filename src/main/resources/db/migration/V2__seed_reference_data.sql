INSERT INTO g_nodes (node_id, party_rk, person_id, phone_no, full_name, is_blacklist, is_vip, employer, attrs_json)
VALUES
    ('N_PARTY_1001', 'PARTY_1001', 'PERS_501', '+70000000001', 'Alice Ivanova', TRUE, FALSE, 'Acme Bank', '{"country":"RU","risk_score":0.93}'),
    ('N_PARTY_1002', 'PARTY_1002', 'PERS_502', '+70000000002', 'Bob Petrov', FALSE, TRUE, 'Northwind', '{"country":"RU","risk_score":0.21}'),
    ('N_PARTY_1003', 'PARTY_1003', 'PERS_503', '+70000000003', 'Carol Sidorova', FALSE, FALSE, 'Contoso', '{"country":"RU","risk_score":0.40}'),
    ('N_PARTY_1004', 'PARTY_1004', 'PERS_504', '+70000000004', 'Dan Smirnov', FALSE, FALSE, 'Fabrikam', '{"country":"RU","risk_score":0.37}');

INSERT INTO g_identifiers (node_id, id_type, id_value)
VALUES
    ('N_PARTY_1001', 'PARTY_RK', 'PARTY_1001'),
    ('N_PARTY_1001', 'PERSON_ID', 'PERS_501'),
    ('N_PARTY_1001', 'PHONE_NO', '+70000000001'),

    ('N_PARTY_1002', 'PARTY_RK', 'PARTY_1002'),
    ('N_PARTY_1002', 'PERSON_ID', 'PERS_502'),
    ('N_PARTY_1002', 'PHONE_NO', '+70000000002'),

    ('N_PARTY_1003', 'PARTY_RK', 'PARTY_1003'),
    ('N_PARTY_1003', 'PERSON_ID', 'PERS_503'),
    ('N_PARTY_1003', 'PHONE_NO', '+70000000003'),

    ('N_PARTY_1004', 'PARTY_RK', 'PARTY_1004'),
    ('N_PARTY_1004', 'PERSON_ID', 'PERS_504'),
    ('N_PARTY_1004', 'PHONE_NO', '+70000000004');

INSERT INTO g_edges (edge_id, from_node_id, to_node_id, edge_type, directed, tx_count, tx_sum, attrs_json)
VALUES
    ('E_TX_1001_1002', 'N_PARTY_1001', 'N_PARTY_1002', 'TRANSFER', TRUE, 3, 12000.50, '{"currency":"RUB"}'),
    ('E_TK_1002_1003', 'N_PARTY_1002', 'N_PARTY_1003', 'TK_LINK', TRUE, 1, 0.0, '{"source":"telecom"}'),
    ('E_TX_1003_1004', 'N_PARTY_1003', 'N_PARTY_1004', 'TRANSFER', TRUE, 2, 870.00, '{"currency":"RUB"}'),
    ('E_REL_1001_1004', 'N_PARTY_1001', 'N_PARTY_1004', 'RELATIVE', FALSE, 0, 0.0, '{"degree":1}');
