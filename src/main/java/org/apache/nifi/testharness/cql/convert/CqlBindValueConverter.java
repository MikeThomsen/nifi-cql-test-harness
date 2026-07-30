package org.apache.nifi.testharness.cql.convert;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Converts NiFi {@link Record} field values into values ready to bind to a CQL
 * {@link PreparedStatement}, faithfully ported from nifi-cql-bundle's
 * {@code CassandraCQLExecutionService#convertForCqlType}/{@code #getBindValues}. Most scalar types
 * are handled by the driver's own default codecs (or, for the coercion-tolerant cases NiFi's
 * {@code DataTypeUtils} allows - e.g. a numeric String into an {@code int} column - by the
 * {@code Flexible*}/{@code JavaSQL*} codecs in {@link org.apache.nifi.testharness.cql.convert.codec},
 * once registered via {@link CqlCodecs#registerAll}); this class handles the cases that need
 * explicit recursion: UDTs, collections (including a collection of UDTs), and UUID/TIMEUUID
 * normalization.
 */
public final class CqlBindValueConverter {

    private CqlBindValueConverter() {
    }

    /**
     * Builds the positional bind-value array for {@code preparedStatement}, taking each named
     * field's value from {@code record} and converting it for that bind marker's actual CQL type.
     * Looks up each field's bind-marker position by name rather than assuming it matches
     * {@code fieldNamesInOrder}'s own order, since a statement may declare bind markers (e.g. a
     * write-timestamp marker) that aren't 1:1 with the record's own fields.
     */
    public static Object[] getBindValues(final Record record, final PreparedStatement preparedStatement, final List<String> fieldNamesInOrder) {
        final ColumnDefinitions variableDefinitions = preparedStatement.getVariableDefinitions();
        final Object[] result = new Object[variableDefinitions.size()];

        for (final String fieldName : fieldNamesInOrder) {
            final int index = variableDefinitions.firstIndexOf(fieldName);
            final Object value = record.getValue(fieldName);
            final DataType cqlType = variableDefinitions.get(index).getType();
            result[index] = convertForCqlType(value, cqlType);
        }

        return result;
    }

    /**
     * Converts a nested {@code Record} or a raw {@code Map} value into a {@link UdtValue} when the
     * target column is a Cassandra User Defined Type, recursing into any UDT-typed fields it
     * contains; converts each element of a {@code List}, {@code Set}, or {@code Object[]} value the
     * same way when the target column is a Cassandra list or set (including a list/set of UDTs) -
     * {@code Object[]} is accepted alongside {@code List}/{@code Set} since it's NiFi's own
     * canonical ARRAY representation ({@code DataTypeUtils#toArray} always returns one); and
     * converts each value of a {@code Map} value the same way when the target column is a Cassandra
     * map. For uuid and timeuuid columns, the value is normalized via {@link DataTypeUtils} so that
     * a compatible but non-exact value - for example a {@code String} representation of a UUID -
     * still binds correctly. Every other value is passed through unchanged, since either the
     * driver's default codec or one of the registered {@code Flexible*}/{@code JavaSQL*} codecs
     * already handles it.
     */
    public static Object convertForCqlType(final Object value, final DataType cqlType) {
        if (value == null) {
            return null;
        }

        if (cqlType instanceof UserDefinedType udtType && (value instanceof Record || value instanceof Map<?, ?>)) {
            final Function<String, Object> fieldValueLookup = value instanceof Record nestedRecord
                    ? nestedRecord::getValue
                    : ((Map<?, ?>) value)::get;

            UdtValue udtValue = udtType.newValue();
            final List<String> fieldNames = udtType.getFieldNames().stream().map(name -> name.asInternal()).toList();

            for (int i = 0; i < fieldNames.size(); i++) {
                final String fieldName = fieldNames.get(i);
                final DataType fieldCqlType = udtType.getFieldTypes().get(i);
                final Object fieldValue = convertForCqlType(fieldValueLookup.apply(fieldName), fieldCqlType);
                udtValue = udtValue.set(fieldName, fieldValue, (Class<Object>) (fieldValue == null ? Object.class : fieldValue.getClass()));
            }

            return udtValue;
        }

        if (cqlType instanceof ListType listType && (value instanceof List<?> || value instanceof Object[])) {
            final DataType elementCqlType = listType.getElementType();
            return toElementStream(value).map(element -> convertForCqlType(element, elementCqlType)).toList();
        }

        if (cqlType instanceof SetType setType && (value instanceof Set<?> || value instanceof Object[])) {
            final DataType elementCqlType = setType.getElementType();
            return toElementStream(value).map(element -> convertForCqlType(element, elementCqlType)).collect(Collectors.toSet());
        }

        if (value instanceof Map<?, ?> map && cqlType instanceof MapType mapType) {
            final DataType keyCqlType = mapType.getKeyType();
            final DataType valueCqlType = mapType.getValueType();
            final Map<Object, Object> converted = new HashMap<>();

            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                converted.put(convertForCqlType(entry.getKey(), keyCqlType), convertForCqlType(entry.getValue(), valueCqlType));
            }

            return converted;
        }

        if (cqlType.equals(DataTypes.UUID) || cqlType.equals(DataTypes.TIMEUUID)) {
            return DataTypeUtils.toUUID(value);
        }

        return value;
    }

    private static Stream<?> toElementStream(final Object value) {
        return (value instanceof Object[] array) ? Arrays.stream(array) : ((Collection<?>) value).stream();
    }
}
