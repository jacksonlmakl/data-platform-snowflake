CREATE DATABASE IF NOT EXISTS CONTROL;
CREATE SCHEMA IF NOT EXISTS CODE;

CREATE OR REPLACE PROCEDURE CONTROL.CODE.DYNAMIC_TABLE(
    db_name STRING,          -- Database name
    schema_name STRING,      -- Schema name
    table_name STRING,       -- Dynamic table name
    primary_sk STRING,       -- Primary key column
    target_lag STRING,       -- Target lag for the dynamic table
    warehouse_name STRING,   -- Warehouse for the dynamic table
    refresh_mode STRING,     -- Refresh mode for the dynamic table
    initialize_mode STRING,  -- Initialize mode (on_create or manual)
    dynamic_table_sql STRING -- SQL query for the dynamic table (after AS clause)
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    full_table_name STRING;
BEGIN
    -- Construct full table name
    full_table_name := db_name || '.' || schema_name || '.' || table_name;

    -- Create the dynamic table
    EXECUTE IMMEDIATE 'CREATE OR REPLACE DYNAMIC TABLE ' || full_table_name || '
        TARGET_LAG = ''' || target_lag || '''
        WAREHOUSE = ' || warehouse_name || '
        REFRESH_MODE = ' || refresh_mode || '
        INITIALIZE = ' || initialize_mode || '
        AS ' || dynamic_table_sql || ';';

    -- Assign data quality checks
    EXECUTE IMMEDIATE 'ALTER TABLE ' || full_table_name || ' SET
        DATA_METRIC_SCHEDULE = ''TRIGGER_ON_CHANGES'';';

    EXECUTE IMMEDIATE 'ALTER TABLE IF EXISTS ' || full_table_name || '
        ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (' || primary_sk || ');';

    EXECUTE IMMEDIATE 'ALTER TABLE IF EXISTS ' || full_table_name || '
        ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (' || primary_sk || ');';

    -- Force refresh
    EXECUTE IMMEDIATE 'ALTER DYNAMIC TABLE ' || full_table_name || ' REFRESH;';

    RETURN 'Dynamic table pipeline created successfully for table: ' || full_table_name;
END;
$$;


CREATE OR REPLACE PROCEDURE CONTROL.CODE.REPLICATION(
    table_name STRING,      -- Target table name
    stage_directory STRING, -- S3 directory for the stage
    task_cron STRING        -- CRON schedule for the task
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    stage_name STRING;
    task_name STRING;
BEGIN
    -- Dynamically construct the stage and task names
    stage_name := 'RAW.S3.' || table_name || '_STAGE';
    task_name := 'RAW.S3.' || table_name || '_REFRESH';

    -- Create the target table
    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS RAW.S3.' || table_name || ' (
        JSON VARIANT,
        INSERT_TIMESTAMP TIMESTAMP
    );';

    -- Create the S3 stage
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STAGE ' || stage_name || '
        URL = ''' || stage_directory || ''' 
        CREDENTIALS = ( AWS_KEY_ID = ''{{ AWS_SECRET_KEY_ID}}'' AWS_SECRET_KEY = ''{{AWS_SECRET_KEY}}'' )
        DIRECTORY = ( ENABLE = true AUTO_REFRESH = true );';

    -- Create the refresh task
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TASK ' || task_name || '
        WAREHOUSE = COMPUTE_WH
        SCHEDULE = ''' || task_cron || '''
        AS
        COPY INTO RAW.S3.' || table_name || '
        FROM (
            SELECT $1::VARIANT, CURRENT_TIMESTAMP() AS INSERT_TIMESTAMP
            FROM @' || stage_name || '
        )
        FILE_FORMAT = (TYPE = ''PARQUET'')
        ON_ERROR = ''CONTINUE'';';

    -- Resume the task
    EXECUTE IMMEDIATE 'ALTER TASK IF EXISTS ' || task_name || ' RESUME;';

    -- Execute the task
    EXECUTE IMMEDIATE 'EXECUTE TASK ' || task_name || ';';

    RETURN 'Pipeline created successfully for table: ' || table_name;
END;
$$;


CREATE OR REPLACE PROCEDURE CONTROL.CODE.DATA_QUALITY(TABLE_NAME STRING,TABLE_SCHEMA STRING, TABLE_DATABASE STRING)
RETURNS TABLE (
    MEASUREMENT_TIME STRING,
    METRIC_NAME STRING,
    TARGET_COLUMNS STRING,
    VALUE STRING
)
LANGUAGE SQL
AS
$$
  DECLARE
    res RESULTSET DEFAULT (SELECT MEASUREMENT_TIME::STRING AS MEASUREMENT_TIME, METRIC_NAME::STRING AS METRIC_NAME, ARGUMENT_NAMES::STRING AS TARGET_COLUMNS, VALUE::STRING AS VALUE
FROM (
    SELECT *, 
           RANK() OVER(PARTITION BY METRIC_NAME, TABLE_DATABASE, TABLE_NAME, TABLE_SCHEMA 
                       ORDER BY MEASUREMENT_TIME DESC) AS NUM
    FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
)
WHERE TABLE_DATABASE = TABLE_DATABASE
  AND TABLE_NAME = TABLE_NAME
  AND TABLE_SCHEMA = TABLE_SCHEMA
  AND NUM = 1);
  
BEGIN
  
RETURN TABLE(res);
END;
  $$;
