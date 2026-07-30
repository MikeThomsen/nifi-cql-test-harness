package org.apache.nifi.testharness.cql;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Converts a camelCase/PascalCase identifier (Avro field and record names are conventionally one of
 * these) into snake_case, for use as a generated CQL column/table name. Acronym-aware: a run of
 * capitals followed by a lowercase letter is treated as "acronym then next word" (so
 * {@code HTTPServer} becomes {@code http_server}, not {@code h_t_t_p_server}), and an isolated
 * capital preceded by a lowercase letter or digit starts a new word (so {@code userID} becomes
 * {@code user_id}, not {@code user_i_d}). An identifier that's already snake_case (or otherwise has
 * no uppercase letters) passes through unchanged.
 */
final class SnakeCaseConverter {

    private static final Pattern ACRONYM_THEN_WORD_BOUNDARY = Pattern.compile("(?<=[A-Z])(?=[A-Z][a-z])");

    private static final Pattern LOWER_OR_DIGIT_THEN_UPPER_BOUNDARY = Pattern.compile("(?<=[a-z0-9])(?=[A-Z])");

    private SnakeCaseConverter() {
    }

    static String toSnakeCase(final String identifier) {
        final String withAcronymBoundaries = ACRONYM_THEN_WORD_BOUNDARY.matcher(identifier).replaceAll("_");
        final String withAllBoundaries = LOWER_OR_DIGIT_THEN_UPPER_BOUNDARY.matcher(withAcronymBoundaries).replaceAll("_");
        return withAllBoundaries.toLowerCase(Locale.ROOT);
    }
}
