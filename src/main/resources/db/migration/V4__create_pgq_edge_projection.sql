CREATE TABLE IF NOT EXISTS g_pgq_edges (
    pgq_edge_id VARCHAR PRIMARY KEY,
    edge_id VARCHAR NOT NULL,
    from_node_id VARCHAR NOT NULL,
    to_node_id VARCHAR NOT NULL,
    traversal_from_node_id VARCHAR NOT NULL,
    traversal_to_node_id VARCHAR NOT NULL,
    edge_type VARCHAR NOT NULL,
    directed BOOLEAN DEFAULT TRUE,
    tx_count BIGINT DEFAULT 0,
    tx_sum DOUBLE DEFAULT 0,
    attrs_json VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

CREATE INDEX IF NOT EXISTS ix_g_pgq_edges_traversal_from ON g_pgq_edges(traversal_from_node_id);
CREATE INDEX IF NOT EXISTS ix_g_pgq_edges_traversal_to ON g_pgq_edges(traversal_to_node_id);
CREATE INDEX IF NOT EXISTS ix_g_pgq_edges_edge_type ON g_pgq_edges(edge_type);
