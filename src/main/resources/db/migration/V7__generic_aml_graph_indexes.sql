CREATE INDEX IF NOT EXISTS ix_g_nodes_node_type ON g_nodes(node_type);
CREATE INDEX IF NOT EXISTS ix_g_nodes_source_system ON g_nodes(source_system);

CREATE INDEX IF NOT EXISTS ix_g_edges_relation_family ON g_edges(relation_family);
CREATE INDEX IF NOT EXISTS ix_g_edges_source_system ON g_edges(source_system);
CREATE INDEX IF NOT EXISTS ix_g_edges_first_seen_at ON g_edges(first_seen_at);
CREATE INDEX IF NOT EXISTS ix_g_edges_last_seen_at ON g_edges(last_seen_at);
