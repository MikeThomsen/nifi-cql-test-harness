package org.apache.nifi.testharness.cql.convert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;
import java.util.Map;

import static com.datastax.oss.driver.api.core.type.DataTypes.ASCII;

/**
 * Recursively converts a Cassandra/ScyllaDB CQL {@link DataType} into a nullable-union Avro
 * {@link Schema}, including support for User Defined Types (UDTs) - Avro has no native concept of
 * one, so a UDT becomes a nested named Avro record whose fields are converted the same way. A UDT
 * nested inside another UDT, or a list/set/map holding UDT values, is handled without any
 * special-casing at the call site, since the same recursion resolves a UDT field's type too.
 *
 * <p>Ported from nifi-cql-bundle's {@code CassandraUdtSchemaMapper}, which
 * {@code CassandraCQLExecutionService#createSchema} uses to build the Avro schema
 * {@code AvroTypeUtil.createSchema} then converts into a NiFi {@code RecordSchema} - see
 * {@link CqlRecordSchemaMapper}, which reproduces that same pipeline in this project.
 */
public final class CqlUdtSchemaMapper {

    private CqlUdtSchemaMapper() {
    }

    /**
     * Converts the given CQL type into a nullable Avro union schema.
     *
     * @param cqlType the CQL type to convert
     * @param udtSchemaCache UDT schemas already built during this conversion, keyed by "keyspace.typeName".
     *                        Avro rejects two distinct schema objects that declare the same record name within
     *                        one document, so a UDT referenced by more than one column, or more than one field
     *                        across nested UDTs, must reuse the same generated {@link Schema} instance. Callers
     *                        should pass a fresh, empty map per top-level schema-building call.
     */
    public static Schema toAvroSchema(final DataType cqlType, final Map<String, Schema> udtSchemaCache) {
        if (cqlType instanceof UserDefinedType udtType) {
            return nullableUnion(toUdtRecordSchema(udtType, udtSchemaCache));
        }

        if (cqlType instanceof ListType listType) {
            return nullableUnion(SchemaBuilder.array().items(toAvroSchema(listType.getElementType(), udtSchemaCache)));
        }

        if (cqlType instanceof SetType setType) {
            return nullableUnion(SchemaBuilder.array().items(toAvroSchema(setType.getElementType(), udtSchemaCache)));
        }

        if (cqlType instanceof MapType mapType) {
            return nullableUnion(SchemaBuilder.map().values(toAvroSchema(mapType.getValueType(), udtSchemaCache)));
        }

        return nullableUnion(toPrimitiveAvroSchema(cqlType));
    }

    private static Schema toUdtRecordSchema(final UserDefinedType udtType, final Map<String, Schema> udtSchemaCache) {
        final String namespace = udtType.getKeyspace() == null ? "cassandra" : udtType.getKeyspace().asInternal();
        final String schemaKey = namespace + "." + udtType.getName().asInternal();

        final Schema cached = udtSchemaCache.get(schemaKey);
        if (cached != null) {
            return cached;
        }

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(udtType.getName().asInternal())
                .namespace(namespace)
                .fields();

        final List<CqlIdentifier> fieldNames = udtType.getFieldNames();
        final List<DataType> fieldTypes = udtType.getFieldTypes();

        for (int i = 0; i < fieldNames.size(); i++) {
            fields = fields.name(fieldNames.get(i).asInternal()).type(toAvroSchema(fieldTypes.get(i), udtSchemaCache)).noDefault();
        }

        final Schema schema = fields.endRecord();
        udtSchemaCache.put(schemaKey, schema);
        return schema;
    }

    private static Schema nullableUnion(final Schema schema) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(schema).endUnion();
    }

    private static Schema toPrimitiveAvroSchema(final DataType cqlType) {
        final SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();

        if (cqlType.equals(ASCII)
                || cqlType.equals(DataTypes.TEXT)
                || cqlType.equals(DataTypes.TIMESTAMP)
                || cqlType.equals(DataTypes.TIMEUUID)
                || cqlType.equals(DataTypes.UUID)
                || cqlType.equals(DataTypes.INET)
                || cqlType.equals(DataTypes.VARINT)
                || cqlType.equals(DataTypes.DATE)
                || cqlType.equals(DataTypes.TIME)
                || cqlType.equals(DataTypes.DECIMAL)) {
            return typeBuilder.stringType();
        }

        if (cqlType.equals(DataTypes.BOOLEAN)) {
            return typeBuilder.booleanType();
        }

        if (cqlType.equals(DataTypes.INT)
                // Avro has no byte/short primitive, so these widen to Avro's int
                || cqlType.equals(DataTypes.TINYINT)
                || cqlType.equals(DataTypes.SMALLINT)) {
            return typeBuilder.intType();
        }

        if (cqlType.equals(DataTypes.BIGINT) || cqlType.equals(DataTypes.COUNTER)) {
            return typeBuilder.longType();
        }

        if (cqlType.equals(DataTypes.FLOAT)) {
            return typeBuilder.floatType();
        }

        if (cqlType.equals(DataTypes.DOUBLE)) {
            return typeBuilder.doubleType();
        }

        if (cqlType.equals(DataTypes.BLOB)) {
            return typeBuilder.bytesType();
        }

        throw new IllegalArgumentException("Unsupported CQL data type " + cqlType + " cannot be converted to an Avro type");
    }
}
