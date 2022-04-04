package org.avniproject.etl.repository;

import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.syncstatus.EntitySyncStatus;
import org.avniproject.etl.repository.dynamicInsert.SqlGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.Date;

@Repository
public class EntityRepository {
    private JdbcTemplate jdbcTemplate;

    @Autowired
    public EntityRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void save(EntitySyncStatus entitySyncStatus) {

    }

    public void saveEntities(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime) {
        try {
            jdbcTemplate.execute(new SqlGenerator().generateSql(tableMetadata, lastSyncTime, dataSyncBoundaryTime));
        } catch (IOException e) {
            //todo: set status to failure if possible here
            e.printStackTrace();
        }
    }
}
