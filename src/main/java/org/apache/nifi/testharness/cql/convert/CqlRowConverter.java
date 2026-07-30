package org.apache.nifi.testharness.cql.convert;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts a CQL {@link Row} into a NiFi {@link MapRecord}, ported from the row-building loop in
 * nifi-cql-bundle's {@code CassandraCQLExecutionService#query}. Unlike the write path
 * ({@link CqlBindValueConverter}), no extra conversion happens here: each column's value is taken
 * directly from {@link Row#getObject(int)}, relying entirely on the driver's default codecs and the
 * registered {@code Flexible*}/{@code JavaSQL*} codecs (see {@link CqlCodecs#registerAll}) to have
 * already produced a record-compatible Java value during decoding.
 */
public final class CqlRowConverter {

    private CqlRowConverter() {
    }

    public static MapRecord toRecord(final Row row, final RecordSchema schema) {
        final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final Map<String, Object> values = new LinkedHashMap<>();

        for (int i = 0; i < columnDefinitions.size(); i++) {
            values.put(columnDefinitions.get(i).getName().toString(), row.getObject(i));
        }

        return new MapRecord(schema, values);
    }
}
