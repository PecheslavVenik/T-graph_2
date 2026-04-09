ALTER TABLE g_nodes
    ADD COLUMN IF NOT EXISTS node_type VARCHAR DEFAULT 'PERSON';

ALTER TABLE g_nodes
    ADD COLUMN IF NOT EXISTS display_name VARCHAR;

ALTER TABLE g_nodes
    ADD COLUMN IF NOT EXISTS source_system VARCHAR DEFAULT 'manual_seed';

UPDATE g_nodes
SET
    node_type = COALESCE(NULLIF(node_type, ''), 'PERSON'),
    display_name = COALESCE(NULLIF(display_name, ''), full_name, party_rk, person_id, phone_no, node_id),
    source_system = COALESCE(NULLIF(source_system, ''), 'manual_seed'),
    updated_at = CURRENT_TIMESTAMP
WHERE node_type IS NULL
   OR node_type = ''
   OR display_name IS NULL
   OR display_name = ''
   OR source_system IS NULL
   OR source_system = '';

ALTER TABLE g_edges
    ADD COLUMN IF NOT EXISTS source_system VARCHAR DEFAULT 'manual_seed';

ALTER TABLE g_edges
    ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP;

ALTER TABLE g_edges
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP;

UPDATE g_edges
SET
    source_system = COALESCE(NULLIF(source_system, ''), 'manual_seed'),
    first_seen_at = COALESCE(first_seen_at, created_at),
    last_seen_at = COALESCE(last_seen_at, updated_at),
    updated_at = CURRENT_TIMESTAMP
WHERE source_system IS NULL
   OR source_system = ''
   OR first_seen_at IS NULL
   OR last_seen_at IS NULL;
