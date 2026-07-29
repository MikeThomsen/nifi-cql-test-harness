package org.apache.nifi.testharness.cql;

import picocli.CommandLine.ITypeConverter;

import java.util.Locale;

public enum SslMode {

    ONE_WAY,
    TWO_WAY;

    /** Accepts the CLI-friendly "one-way" / "two-way" spelling picocli's default enum converter would reject. */
    public static final class Converter implements ITypeConverter<SslMode> {
        @Override
        public SslMode convert(final String value) {
            return SslMode.valueOf(value.trim().toUpperCase(Locale.ROOT).replace('-', '_'));
        }
    }
}
