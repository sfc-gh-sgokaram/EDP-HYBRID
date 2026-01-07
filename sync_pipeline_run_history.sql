-- ============================================================================
-- SYNC PROCESS: ETL_PIPELINE_RUN_HISTORY_LOG -> ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION
-- Uses watermark-based change tracking via LAST_UPDATE_DATE
-- Compatible with Snowflake Hybrid Tables (no stream support required)
-- Stores full history of every sync run
-- ============================================================================

-- ============================================================================
-- STEP 1: Create a control table to track sync history
-- ============================================================================
CREATE OR REPLACE TABLE ETL_PIPELINE_SYNC_CONTROL (
    SYNC_RUN_ID NUMBER(38,0) NOT NULL PRIMARY KEY AUTOINCREMENT,
    TABLE_NAME VARCHAR(250) NOT NULL,
    SYNC_START_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    SYNC_END_TIMESTAMP TIMESTAMP_NTZ(9),
    WATERMARK_FROM TIMESTAMP_NTZ(9),
    WATERMARK_TO TIMESTAMP_NTZ(9),
    ROWS_INSERTED NUMBER(38,0) DEFAULT 0,
    ROWS_UPDATED NUMBER(38,0) DEFAULT 0,
    ROWS_PROCESSED NUMBER(38,0) DEFAULT 0,
    SYNC_STATUS VARCHAR(20) DEFAULT 'RUNNING',
    ERROR_MESSAGE VARCHAR(16777216)
);

-- ============================================================================
-- STEP 2: Create a stored procedure to handle the sync logic with history
-- ============================================================================
CREATE OR REPLACE PROCEDURE SYNC_PIPELINE_RUN_HISTORY_PROC()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_sync_run_id NUMBER;
    v_last_sync_ts TIMESTAMP_NTZ;
    v_new_sync_ts TIMESTAMP_NTZ;
    v_rows_merged NUMBER;
    v_rows_inserted NUMBER;
    v_rows_updated NUMBER;
    v_error_msg VARCHAR;
BEGIN
    -- Get the last watermark from the most recent successful sync
    SELECT COALESCE(MAX(WATERMARK_TO), '1900-01-01 00:00:00'::TIMESTAMP_NTZ) INTO v_last_sync_ts
    FROM ETL_PIPELINE_SYNC_CONTROL
    WHERE TABLE_NAME = 'ETL_PIPELINE_RUN_HISTORY_LOG'
      AND SYNC_STATUS = 'SUCCESS';
    
    -- Insert a new sync run record (status = RUNNING)
    INSERT INTO ETL_PIPELINE_SYNC_CONTROL (
        TABLE_NAME, 
        SYNC_START_TIMESTAMP, 
        WATERMARK_FROM, 
        SYNC_STATUS
    )
    VALUES (
        'ETL_PIPELINE_RUN_HISTORY_LOG', 
        CURRENT_TIMESTAMP(), 
        :v_last_sync_ts, 
        'RUNNING'
    );
    
    -- Get the sync run ID
    SELECT MAX(SYNC_RUN_ID) INTO v_sync_run_id
    FROM ETL_PIPELINE_SYNC_CONTROL
    WHERE TABLE_NAME = 'ETL_PIPELINE_RUN_HISTORY_LOG';
    
    BEGIN
        -- Get the current max timestamp from source
        SELECT COALESCE(MAX(LAST_UPDATE_DATE), :v_last_sync_ts) INTO v_new_sync_ts
        FROM ETL_PIPELINE_RUN_HISTORY_LOG
        WHERE LAST_UPDATE_DATE > :v_last_sync_ts;
        
        -- Count rows to be inserted (new records)
        SELECT COUNT(*) INTO v_rows_inserted
        FROM ETL_PIPELINE_RUN_HISTORY_LOG src
        WHERE src.LAST_UPDATE_DATE > :v_last_sync_ts
          AND NOT EXISTS (
              SELECT 1 FROM ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION tgt
              WHERE tgt.PIPELINE_RUN_ID = src.PIPELINE_RUN_ID
          );
        
        -- Count rows to be updated (existing records)
        SELECT COUNT(*) INTO v_rows_updated
        FROM ETL_PIPELINE_RUN_HISTORY_LOG src
        WHERE src.LAST_UPDATE_DATE > :v_last_sync_ts
          AND EXISTS (
              SELECT 1 FROM ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION tgt
              WHERE tgt.PIPELINE_RUN_ID = src.PIPELINE_RUN_ID
          );
        
        -- Merge changes into audit table
        MERGE INTO ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION AS tgt
        USING (
            SELECT 
                PIPELINE_RUN_ID,
                PIPELINE_ID,
                DOMAIN_ID,
                PROVIDER_SOURCE_ID,
                ORCHESTRATION_PIPELINE_NM,
                ORCHESTRATION_PIPELINE_RUN_CD,
                PIPELINE_START_DT,
                PIPELINE_END_DT,
                PIPELINE_RUN_STATUS,
                LAST_UPDATED_BY,
                LAST_UPDATE_DATE,
                VERSION_NM,
                PIPELINE_EXCEPTION_DESC,
                PARENT_ORCHESTRATION_PIPELINE_NM,
                PARENT_ORCHESTRATION_PIPELINE_RUN_CD,
                ADOPTER_ID,
                CALENDAR_FL,
                CALENDAR_CODE,
                CALENDAR_BUSINESS_DAY_TAG,
                SCHEDULE_TIMEZONE,
                TIMEZONE_OFFSET
            FROM ETL_PIPELINE_RUN_HISTORY_LOG
            WHERE LAST_UPDATE_DATE > :v_last_sync_ts
        ) AS src
        ON tgt.PIPELINE_RUN_ID = src.PIPELINE_RUN_ID
        
        -- When record exists, update it
        WHEN MATCHED THEN UPDATE SET
            tgt.PIPELINE_ID = src.PIPELINE_ID,
            tgt.DOMAIN_ID = src.DOMAIN_ID,
            tgt.PROVIDER_SOURCE_ID = src.PROVIDER_SOURCE_ID,
            tgt.ORCHESTRATION_PIPELINE_NM = src.ORCHESTRATION_PIPELINE_NM,
            tgt.ORCHESTRATION_PIPELINE_RUN_CD = src.ORCHESTRATION_PIPELINE_RUN_CD,
            tgt.PIPELINE_START_DT = src.PIPELINE_START_DT,
            tgt.PIPELINE_END_DT = src.PIPELINE_END_DT,
            tgt.PIPELINE_RUN_STATUS = src.PIPELINE_RUN_STATUS,
            tgt.LAST_UPDATED_BY = src.LAST_UPDATED_BY,
            tgt.LAST_UPDATE_DATE = src.LAST_UPDATE_DATE,
            tgt.VERSION_NM = src.VERSION_NM,
            tgt.PIPELINE_EXCEPTION_DESC = src.PIPELINE_EXCEPTION_DESC,
            tgt.PARENT_ORCHESTRATION_PIPELINE_NM = src.PARENT_ORCHESTRATION_PIPELINE_NM,
            tgt.PARENT_ORCHESTRATION_PIPELINE_RUN_CD = src.PARENT_ORCHESTRATION_PIPELINE_RUN_CD,
            tgt.ADOPTER_ID = src.ADOPTER_ID,
            tgt.CALENDAR_FL = src.CALENDAR_FL,
            tgt.CALENDAR_CODE = src.CALENDAR_CODE,
            tgt.CALENDAR_BUSINESS_DAY_TAG = src.CALENDAR_BUSINESS_DAY_TAG,
            tgt.SCHEDULE_TIMEZONE = src.SCHEDULE_TIMEZONE,
            tgt.TIMEZONE_OFFSET = src.TIMEZONE_OFFSET
        
        -- When record doesn't exist, insert it
        WHEN NOT MATCHED THEN INSERT (
            PIPELINE_RUN_ID,
            PIPELINE_ID,
            DOMAIN_ID,
            PROVIDER_SOURCE_ID,
            ORCHESTRATION_PIPELINE_NM,
            ORCHESTRATION_PIPELINE_RUN_CD,
            PIPELINE_START_DT,
            PIPELINE_END_DT,
            PIPELINE_RUN_STATUS,
            LAST_UPDATED_BY,
            LAST_UPDATE_DATE,
            VERSION_NM,
            PIPELINE_EXCEPTION_DESC,
            PARENT_ORCHESTRATION_PIPELINE_NM,
            PARENT_ORCHESTRATION_PIPELINE_RUN_CD,
            ADOPTER_ID,
            CALENDAR_FL,
            CALENDAR_CODE,
            CALENDAR_BUSINESS_DAY_TAG,
            SCHEDULE_TIMEZONE,
            TIMEZONE_OFFSET
        ) VALUES (
            src.PIPELINE_RUN_ID,
            src.PIPELINE_ID,
            src.DOMAIN_ID,
            src.PROVIDER_SOURCE_ID,
            src.ORCHESTRATION_PIPELINE_NM,
            src.ORCHESTRATION_PIPELINE_RUN_CD,
            src.PIPELINE_START_DT,
            src.PIPELINE_END_DT,
            src.PIPELINE_RUN_STATUS,
            src.LAST_UPDATED_BY,
            src.LAST_UPDATE_DATE,
            src.VERSION_NM,
            src.PIPELINE_EXCEPTION_DESC,
            src.PARENT_ORCHESTRATION_PIPELINE_NM,
            src.PARENT_ORCHESTRATION_PIPELINE_RUN_CD,
            src.ADOPTER_ID,
            src.CALENDAR_FL,
            src.CALENDAR_CODE,
            src.CALENDAR_BUSINESS_DAY_TAG,
            src.SCHEDULE_TIMEZONE,
            src.TIMEZONE_OFFSET
        );
        
        -- Get the number of rows affected
        v_rows_merged := SQLROWCOUNT;
        
        -- Update the sync run record with success
        UPDATE ETL_PIPELINE_SYNC_CONTROL
        SET SYNC_END_TIMESTAMP = CURRENT_TIMESTAMP(),
            WATERMARK_TO = :v_new_sync_ts,
            ROWS_INSERTED = :v_rows_inserted,
            ROWS_UPDATED = :v_rows_updated,
            ROWS_PROCESSED = :v_rows_merged,
            SYNC_STATUS = 'SUCCESS'
        WHERE SYNC_RUN_ID = :v_sync_run_id;
        
        RETURN 'Sync completed. Run ID: ' || v_sync_run_id || 
               '. Rows inserted: ' || v_rows_inserted || 
               '. Rows updated: ' || v_rows_updated ||
               '. Watermark: ' || v_last_sync_ts || ' -> ' || v_new_sync_ts;
    
    EXCEPTION
        WHEN OTHER THEN
            v_error_msg := SQLERRM;
            
            -- Update the sync run record with failure
            UPDATE ETL_PIPELINE_SYNC_CONTROL
            SET SYNC_END_TIMESTAMP = CURRENT_TIMESTAMP(),
                SYNC_STATUS = 'FAILED',
                ERROR_MESSAGE = :v_error_msg
            WHERE SYNC_RUN_ID = :v_sync_run_id;
            
            RETURN 'Sync failed. Run ID: ' || v_sync_run_id || '. Error: ' || v_error_msg;
    END;
END;
$$;

-- ============================================================================
-- STEP 3: Create a Task to run the sync procedure on a schedule
-- ============================================================================
CREATE OR REPLACE TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION
    WAREHOUSE = 'YOUR_WAREHOUSE'  -- Replace with your warehouse name
    SCHEDULE = '1 minute'
    COMMENT = 'Syncs changes from ETL_PIPELINE_RUN_HISTORY_LOG to ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION using watermark'
AS
    CALL SYNC_PIPELINE_RUN_HISTORY_PROC();

-- ============================================================================
-- STEP 4: Resume the task to start the sync process
-- ============================================================================
ALTER TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION RESUME;

-- ============================================================================
-- HELPER QUERIES FOR MONITORING
-- ============================================================================

-- View all sync history
-- SELECT * FROM ETL_PIPELINE_SYNC_CONTROL ORDER BY SYNC_RUN_ID DESC;

-- View recent sync runs (last 24 hours)
-- SELECT * FROM ETL_PIPELINE_SYNC_CONTROL 
-- WHERE SYNC_START_TIMESTAMP > DATEADD('hour', -24, CURRENT_TIMESTAMP())
-- ORDER BY SYNC_RUN_ID DESC;

-- View failed syncs
-- SELECT * FROM ETL_PIPELINE_SYNC_CONTROL 
-- WHERE SYNC_STATUS = 'FAILED'
-- ORDER BY SYNC_RUN_ID DESC;

-- View sync statistics summary
-- SELECT 
--     DATE(SYNC_START_TIMESTAMP) AS SYNC_DATE,
--     COUNT(*) AS TOTAL_RUNS,
--     SUM(CASE WHEN SYNC_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESS_COUNT,
--     SUM(CASE WHEN SYNC_STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_COUNT,
--     SUM(ROWS_INSERTED) AS TOTAL_INSERTED,
--     SUM(ROWS_UPDATED) AS TOTAL_UPDATED,
--     AVG(TIMESTAMPDIFF('second', SYNC_START_TIMESTAMP, SYNC_END_TIMESTAMP)) AS AVG_DURATION_SECONDS
-- FROM ETL_PIPELINE_SYNC_CONTROL
-- WHERE SYNC_STATUS IN ('SUCCESS', 'FAILED')
-- GROUP BY DATE(SYNC_START_TIMESTAMP)
-- ORDER BY SYNC_DATE DESC;

-- Check task status
-- SHOW TASKS LIKE 'SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION';

-- Manually test the procedure
-- CALL SYNC_PIPELINE_RUN_HISTORY_PROC();

-- Suspend task if needed
-- ALTER TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION SUSPEND;
