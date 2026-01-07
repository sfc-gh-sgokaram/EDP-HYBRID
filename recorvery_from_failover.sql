-- Recovery from failover
--Truncate the Hybrid table
truncate table EDP_META_DB.EDP_ETL_METADATA.ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION;
--Insert from the replication table into the Hybrid table
insert into EDP_META_DB.EDP_ETL_METADATA.ETL_PIPELINE_RUN_HISTORY_LOG select * from EDP_META_DB.EDP_ETL_METADATA.ETL_PIPELINE_RUN_HISTORY_LOG_REPLICATION;