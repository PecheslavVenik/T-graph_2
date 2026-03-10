CREATE TABLE IF NOT EXISTS g_engine_state (
    state_key VARCHAR PRIMARY KEY,
    state_value VARCHAR NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELETE FROM g_engine_state WHERE state_key = 'duckpgq.loaded';

INSERT INTO g_engine_state (state_key, state_value, updated_at)
VALUES
    (
        'duckpgq.loaded',
        (
            SELECT CASE
                     WHEN EXISTS (
                         SELECT 1
                         FROM duckdb_extensions()
                         WHERE extension_name = 'duckpgq' AND loaded = TRUE
                     )
                     THEN 'true'
                     ELSE 'false'
                END
        ),
        CURRENT_TIMESTAMP
    );

-- Best effort note:
-- If your environment allows extension install/load, execute manually:
-- INSTALL duckpgq;
-- LOAD duckpgq;
