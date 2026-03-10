CREATE TABLE IF NOT EXISTS g_nodes (
    node_id VARCHAR PRIMARY KEY,
    party_rk VARCHAR,
    person_id VARCHAR,
    phone_no VARCHAR,
    full_name VARCHAR,
    is_blacklist BOOLEAN DEFAULT FALSE,
    is_vip BOOLEAN DEFAULT FALSE,
    employer VARCHAR,
    attrs_json VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_g_nodes_party_rk ON g_nodes(party_rk);
CREATE INDEX IF NOT EXISTS ix_g_nodes_person_id ON g_nodes(person_id);
CREATE INDEX IF NOT EXISTS ix_g_nodes_phone_no ON g_nodes(phone_no);

CREATE TABLE IF NOT EXISTS g_edges (
    edge_id VARCHAR PRIMARY KEY,
    from_node_id VARCHAR NOT NULL,
    to_node_id VARCHAR NOT NULL,
    edge_type VARCHAR NOT NULL,
    directed BOOLEAN DEFAULT TRUE,
    tx_count BIGINT DEFAULT 0,
    tx_sum DOUBLE DEFAULT 0,
    attrs_json VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_g_edges_from_node_id ON g_edges(from_node_id);
CREATE INDEX IF NOT EXISTS ix_g_edges_to_node_id ON g_edges(to_node_id);
CREATE INDEX IF NOT EXISTS ix_g_edges_edge_type ON g_edges(edge_type);

CREATE TABLE IF NOT EXISTS g_identifiers (
    node_id VARCHAR NOT NULL,
    id_type VARCHAR NOT NULL,
    id_value VARCHAR NOT NULL,
    PRIMARY KEY (node_id, id_type, id_value)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_g_identifiers_lookup ON g_identifiers(id_type, id_value, node_id);
