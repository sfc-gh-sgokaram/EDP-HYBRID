# ETL Pipeline Run History Sync Process

A Snowflake-based solution for synchronizing data between Hybrid Tables using watermark-based change tracking.

---

## Overview

This solution synchronizes data from `ETL_PIPELINE_RUN_HISTORY_LOG` to `ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION` using a scheduled task with watermark-based change detection. It's designed specifically for Snowflake Hybrid Tables, which do not support streams.

---

## Architecture

```
┌─────────────────────────────────────┐
│  ETL_PIPELINE_RUN_HISTORY_LOG       │
│  (Source - Hybrid Table)            │
│                                     │
│  • Continuous inserts/updates       │
│  • LAST_UPDATE_DATE tracks changes  │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│  SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION │
│  (Snowflake Task)                   │
│                                     │
│  • Runs every 1 minute (configurable)│
│  • Calls stored procedure           │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│  SYNC_PIPELINE_RUN_HISTORY_PROC     │
│  (Stored Procedure)                 │
│                                     │
│  1. Read last watermark             │
│  2. Find changed records            │
│  3. MERGE into target               │
│  4. Log sync history                │
└──────────────────┬──────────────────┘
                   │
          ┌────────┴────────┐
          ▼                 ▼
┌─────────────────┐  ┌─────────────────┐
│ AUDIT Table     │  │ SYNC_CONTROL    │
│ (Target)        │  │ (History Log)   │
└─────────────────┘  └─────────────────┘
```

---

## Design Approach

### Watermark-Based Change Detection

Since Snowflake Hybrid Tables do not support streams, this solution uses a **watermark pattern** to detect changes:

1. **Watermark Column**: `LAST_UPDATE_DATE` on the source table
2. **Watermark Storage**: `ETL_PIPELINE_SYNC_CONTROL` table stores the last successful sync timestamp
3. **Change Query**: `WHERE LAST_UPDATE_DATE > last_watermark`

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **MERGE statement** | Handles both INSERTs and UPDATEs in a single atomic operation |
| **Stored Procedure** | Encapsulates sync logic with proper error handling |
| **Sync History Table** | Full audit trail of every sync run for monitoring and debugging |
| **Task with Schedule** | Automated execution without external orchestration |

### Assumptions

- `LAST_UPDATE_DATE` is reliably maintained on all INSERT/UPDATE operations
- No DELETE operations occur on the source table
- Clock skew between concurrent transactions is minimal

---

## Components

### 1. Source Table: `ETL_PIPELINE_RUN_HISTORY_LOG`

The primary table where pipeline run data is continuously written.

- **Primary Key**: `PIPELINE_RUN_ID`
- **Change Tracking Column**: `LAST_UPDATE_DATE`
- **Table Type**: Hybrid Table

### 2. Target Table: `ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION`

The audit/replica table that mirrors the source.

- **Primary Key**: `PIPELINE_RUN_ID`
- **Table Type**: Hybrid Table

### 3. Control Table: `ETL_PIPELINE_SYNC_CONTROL`

Tracks the history of every sync execution.

| Column | Type | Description |
|--------|------|-------------|
| `SYNC_RUN_ID` | NUMBER (PK) | Auto-incrementing run identifier |
| `TABLE_NAME` | VARCHAR | Source table being synced |
| `SYNC_START_TIMESTAMP` | TIMESTAMP_NTZ | When sync started |
| `SYNC_END_TIMESTAMP` | TIMESTAMP_NTZ | When sync completed |
| `WATERMARK_FROM` | TIMESTAMP_NTZ | Start of change window |
| `WATERMARK_TO` | TIMESTAMP_NTZ | End of change window |
| `ROWS_INSERTED` | NUMBER | New records added |
| `ROWS_UPDATED` | NUMBER | Existing records updated |
| `ROWS_PROCESSED` | NUMBER | Total rows processed |
| `SYNC_STATUS` | VARCHAR | `RUNNING`, `SUCCESS`, or `FAILED` |
| `ERROR_MESSAGE` | VARCHAR | Error details if failed |

### 4. Stored Procedure: `SYNC_PIPELINE_RUN_HISTORY_PROC`

Core sync logic with the following flow:

```
1. Get last successful watermark from SYNC_CONTROL
2. Create new SYNC_CONTROL record (status = RUNNING)
3. Count records to insert vs update
4. Execute MERGE statement
5. Update SYNC_CONTROL with results (status = SUCCESS/FAILED)
```

### 5. Task: `SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION`

Scheduled task that executes the stored procedure.

- **Schedule**: Every 1 minute (configurable)
- **Warehouse**: User-specified

---

## Installation

### Prerequisites

- Snowflake account with appropriate permissions
- A warehouse for task execution
- Source and target Hybrid Tables created

### Deployment Steps

1. **Update the warehouse name** in `sync_pipeline_run_history.sql`:
   ```sql
   WAREHOUSE = 'YOUR_WAREHOUSE'  -- Replace with your warehouse
   ```

2. **Run the SQL script**:
   ```sql
   -- Execute in Snowflake
   !source sync_pipeline_run_history.sql
   ```

3. **Verify task is running**:
   ```sql
   SHOW TASKS LIKE 'SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION';
   ```

---

## Configuration

### Task Schedule

Modify the schedule based on your latency requirements:

```sql
-- Every 5 minutes
ALTER TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION SET SCHEDULE = '5 minute';

-- Every hour
ALTER TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION SET SCHEDULE = '60 minute';

-- Using CRON (every 15 minutes)
ALTER TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION SET SCHEDULE = 'USING CRON */15 * * * * UTC';
```

### Suspend/Resume Task

```sql
-- Suspend
ALTER TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION SUSPEND;

-- Resume
ALTER TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION RESUME;
```

---

## Monitoring

### View Recent Sync History

```sql
SELECT * 
FROM ETL_PIPELINE_SYNC_CONTROL 
ORDER BY SYNC_RUN_ID DESC 
LIMIT 20;
```

### Check for Failed Syncs

```sql
SELECT * 
FROM ETL_PIPELINE_SYNC_CONTROL 
WHERE SYNC_STATUS = 'FAILED'
ORDER BY SYNC_RUN_ID DESC;
```

### Daily Sync Statistics

```sql
SELECT 
    DATE(SYNC_START_TIMESTAMP) AS SYNC_DATE,
    COUNT(*) AS TOTAL_RUNS,
    SUM(CASE WHEN SYNC_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESS_COUNT,
    SUM(CASE WHEN SYNC_STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_COUNT,
    SUM(ROWS_INSERTED) AS TOTAL_INSERTED,
    SUM(ROWS_UPDATED) AS TOTAL_UPDATED,
    AVG(TIMESTAMPDIFF('second', SYNC_START_TIMESTAMP, SYNC_END_TIMESTAMP)) AS AVG_DURATION_SEC
FROM ETL_PIPELINE_SYNC_CONTROL
WHERE SYNC_STATUS IN ('SUCCESS', 'FAILED')
GROUP BY DATE(SYNC_START_TIMESTAMP)
ORDER BY SYNC_DATE DESC;
```

### Task Execution History

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

---

## Manual Execution

To run the sync manually (for testing or recovery):

```sql
CALL SYNC_PIPELINE_RUN_HISTORY_PROC();
```

---

## Troubleshooting

### Task Not Running

1. Check task status:
   ```sql
   SHOW TASKS LIKE 'SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION';
   ```
2. Ensure task is resumed:
   ```sql
   ALTER TASK SYNC_PIPELINE_RUN_HISTORY_TO_REPLICATION RESUME;
   ```
3. Verify warehouse exists and is accessible

### Sync Failures

1. Check error message in control table:
   ```sql
   SELECT SYNC_RUN_ID, ERROR_MESSAGE 
   FROM ETL_PIPELINE_SYNC_CONTROL 
   WHERE SYNC_STATUS = 'FAILED';
   ```
2. Verify source/target table permissions
3. Check for schema mismatches between tables

### Data Not Syncing

1. Verify `LAST_UPDATE_DATE` is being updated on source:
   ```sql
   SELECT MAX(LAST_UPDATE_DATE) FROM ETL_PIPELINE_RUN_HISTORY_LOG;
   ```
2. Check current watermark:
   ```sql
   SELECT MAX(WATERMARK_TO) 
   FROM ETL_PIPELINE_SYNC_CONTROL 
   WHERE SYNC_STATUS = 'SUCCESS';
   ```
3. Look for records that should have been synced:
   ```sql
   SELECT COUNT(*) 
   FROM ETL_PIPELINE_RUN_HISTORY_LOG 
   WHERE LAST_UPDATE_DATE > (
       SELECT COALESCE(MAX(WATERMARK_TO), '1900-01-01') 
       FROM ETL_PIPELINE_SYNC_CONTROL 
       WHERE SYNC_STATUS = 'SUCCESS'
   );
   ```

---

## File Structure

```
EDP_HYBRID_POC/
├── README.md                      # This file
├── run_history_log.sql            # Table definitions
└── sync_pipeline_run_history.sql  # Sync process (procedure + task)
```

---

## Limitations

1. **No stream support** - Cannot use Snowflake streams with Hybrid Tables
2. **Clock dependency** - Relies on `LAST_UPDATE_DATE` being accurately maintained
3. **No delete detection** - Only handles INSERTs and UPDATEs (as per requirements)
4. **Single table sync** - Current implementation is specific to one table pair

---

## Future Enhancements

- Parameterize table names for reusability
- Add data validation/reconciliation checks
- Implement alerting for failed syncs
- Add support for batch processing large change sets

