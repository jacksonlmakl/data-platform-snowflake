CALL CONTROL.CODE.REPLICATION(
    'POKEMON',                     -- table_name
    's3://jm531/stage',            -- stage_directory
    'USING CRON 0/60 * * * * UTC'  -- task_cron (run every 60 minutes)
);
