CREATE TABLE "<schemaName>"."temp_table" (LIKE "<schemaName>"."sync_telemetry");
INSERT INTO "<schemaName>"."temp_table"(id, uuid,
                    user_id,
                    organisation_id,
                    sync_status,
                    sync_start_time,
                    sync_end_time,
                    device_name,
                    android_version,
                    app_version,
                    device_info,
                    sync_source)
SELECT DISTINCT ON (id) id, uuid,
                    user_id,
                    organisation_id,
                    sync_status,
                    sync_start_time,
                    sync_end_time,
                    device_name,
                    android_version,
                    app_version,
                    device_info,
                    sync_source
FROM "<schemaName>"."sync_telemetry";
DROP TABLE "<schemaName>"."sync_telemetry";
ALTER TABLE "<schemaName>"."temp_table"  
RENAME TO "sync_telemetry";