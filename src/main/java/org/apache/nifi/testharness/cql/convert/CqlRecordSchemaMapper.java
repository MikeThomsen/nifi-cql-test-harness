package org.apache.nifi.testharness.cql.convert;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.DataType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds a NiFi {@link RecordSchema} from a CQL {@link ResultSet}'s column metadata, following the
 * same pipeline as nifi-cql-bundle's {@code CassandraCQLExecutionService#createSchema}: each
 * column's CQL {@link DataType} is converted to an Avro {@link Schema} via
 * {@link CqlUdtSchemaMapper} (the only way to represent a UDT, since Avro has no native concept of
 * one), assembled into a whole-row Avro record schema, and that Avro schema is then converted into
 * a {@link RecordSchema} via {@link AvroTypeUtil#createSchema(Schema)}.
 */
public final class CqlRecordSchemaMapper {

    private static final String DEFAULT_RECORD_NAME = "NiFi_CQL_Query_Record";

    private CqlRecordSchemaMapper() {
    }

    public static RecordSchema toRecordSchema(final ResultSet resultSet) {
        return AvroTypeUtil.createSchema(toAvroSchema(resultSet));
    }

    public static Schema toAvroSchema(final ResultSet resultSet) {
        final ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
        final int columnCount = columnDefinitions == null ? 0 : columnDefinitions.size();

        String recordName = DEFAULT_RECORD_NAME;
        if (columnCount > 0) {
            final String tableNameFromMetadata = columnDefinitions.get(0).getTable().toString();
            if (tableNameFromMetadata != null && !tableNameFromMetadata.isBlank()) {
                recordName = tableNameFromMetadata;
            }
        }

        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(recordName).namespace("any.data").fields();
        if (columnDefinitions != null) {
            final Map<String, Schema> udtSchemaCache = new HashMap<>();
            for (int i = 0; i < columnCount; i++) {
                final DataType dataType = columnDefinitions.get(i).getType();
                if (dataType == null) {
                    throw new IllegalArgumentException("No data type for column[" + i + "] with name "
                            + columnDefinitions.get(i).getName());
                }

                builder.name(columnDefinitions.get(i).getName().toString())
                        .type(CqlUdtSchemaMapper.toAvroSchema(dataType, udtSchemaCache))
                        .noDefault();
            }
        }
        return builder.endRecord();
    }
}
