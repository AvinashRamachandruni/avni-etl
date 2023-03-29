package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.repository.rowMappers.ColumnMetadataMapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class MediaTable extends Table {

    @Override
    public String name(Map<String, Object> tableDetails) {
        return "media";
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withCommonColumns()
                .withColumn(new Column("address_id", Column.Type.numeric, Column.ColumnType.index))
                .withColumn(new Column("image_url", Column.Type.text))
                .withColumn(new Column("sync_parameter_key1", Column.Type.text))
                .withColumn(new Column("sync_parameter_value1", Column.Type.text))
                .withColumn(new Column("sync_parameter_key2", Column.Type.text))
                .withColumn(new Column("sync_parameter_value2", Column.Type.text))
                .withColumn(new Column("subject_type_name", Column.Type.text))
                .withColumn(new Column("encounter_type_name", Column.Type.text))
                .withColumn(new Column("program_name", Column.Type.text))
                .withColumn(new Column("concept_name", Column.Type.text))
                .withColumn(new Column("entity_id", Column.Type.integer))
                .build();
    }
}
