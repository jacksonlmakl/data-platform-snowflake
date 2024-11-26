CALL CONTROL.CODE.DYNAMIC_TABLE(
    'CORE',                                        -- db_name
    'PLATFORM',                                    -- schema_name
    'POKEMON',                                     -- table_name
    'POKEMON_SK',                                  -- primary_sk
    '60 minutes',                                  -- target_lag
    'COMPUTE_WH',                                  -- warehouse_name
    'INCREMENTAL',                                 -- refresh_mode
    'on_create',                                   -- initialize_mode
    $$
    
    WITH SRC AS(
        SELECT DISTINCT
            hash(json:name::STRING) AS pokemon_sk,
            json:name::STRING AS pokemon_name,
            json:url::STRING AS api_url,
            insert_timestamp,
            RANK() OVER(PARTITION BY POKEMON_SK ORDER BY INSERT_TIMESTAMP DESC) AS num
        FROM RAW.S3.POKEMON QUALIFY num = 1
    ) 
    SELECT 
        pokemon_sk,
        pokemon_name,
        api_url,
        insert_timestamp
    FROM SRC;
    
    $$                                    -- dynamic_table_sql
);
