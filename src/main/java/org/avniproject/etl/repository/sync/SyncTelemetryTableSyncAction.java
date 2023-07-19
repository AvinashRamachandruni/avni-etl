package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.AvniMetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.util.Date;
import java.util.HashMap;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;
import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

@Repository
public class SyncTelemetryTableSyncAction implements EntitySyncAction {
    private final JdbcTemplate jdbcTemplate;
    private final AvniMetadataRepository avniMetadataRepository;
    private static final String syncTelemetrySql = readSqlFile("syncTelemetry.sql.st");

    private static final String deleteDuplicateSyncTelemetrySql = readSqlFile("deleteDuplicateSyncTelemetry.sql.st");

    @Autowired
    public SyncTelemetryTableSyncAction(JdbcTemplate jdbcTemplate, AvniMetadataRepository metadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.avniMetadataRepository = metadataRepository;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !tableMetadata.getType().equals(TableMetadata.Type.SyncTelemetry);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        if (this.doesntSupport(tableMetadata)) {
            return;
        }

                insertData(tableMetadata);
    }

    private void insertData(TableMetadata syncTelemetryTableMetadata) {
        syncNewerRows(syncTelemetryTableMetadata);

        deleteDuplicateRows();
    }

    private void syncNewerRows(TableMetadata syncTelemetryTableMetadata) {

        ST template = new ST(syncTelemetrySql)
                .add("schemaName", wrapInQuotes(OrgIdentityContextHolder.getDbSchema()))
                .add("tableName", wrapInQuotes(syncTelemetryTableMetadata.getName()));

        String sql = template.render();

        runInOrgContext(() -> {
            jdbcTemplate.execute(sql);
            return NullObject.instance();
        }, jdbcTemplate);
    }

    private void deleteDuplicateRows() {
        String schema = OrgIdentityContextHolder.getDbSchema();
        String sql = new ST(deleteDuplicateSyncTelemetrySql)
                .add("schemaName", schema)
                .render();
        HashMap<String, Object> params = new HashMap<>();
//        params.put("lastSyncTime", lastSyncTime);

        runInOrgContext(() -> {
            new NamedParameterJdbcTemplate(jdbcTemplate).update(sql, params);
            return NullObject.instance();
        }, jdbcTemplate);
    }


    private String wrapInQuotes(String parameter) {
        return parameter == null ? "null" : "\"" + parameter + "\"";

    }

}
