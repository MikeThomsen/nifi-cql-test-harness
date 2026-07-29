package org.apache.nifi.testharness.cql;

/**
 * Minimal line-oriented editing for the Cassandra/ScyllaDB config YAML this tool extracts live from each
 * container image (see {@link StartCommand}). Deliberately not a real YAML parser - a full parse/re-serialize
 * round-trip risks reformatting parts of the file this tool never touches, and the config shape needed here
 * (top-level scalar keys, one top-level block key) is simple enough that line scanning is both sufficient and
 * easy to audit, matching how the existing nifi-cql-bundle integration tests patch these same files.
 */
final class YamlPatcher {

    private YamlPatcher() {
    }

    /** Replaces a top-level {@code key: value} line if present, or appends one if not. */
    static String setScalar(final String yaml, final String key, final String value) {
        final String withoutExisting = removeTopLevelKey(yaml, key);
        return withoutExisting.stripTrailing() + "\n" + key + ": " + value + "\n";
    }

    /** Replaces a top-level {@code key:} block (the key line plus every indented/blank line under it) if
     * present, or appends one if not. {@code blockBody} must already be indented (e.g. two-space prefixed). */
    static String replaceBlock(final String yaml, final String key, final String blockBody) {
        final String withoutExisting = removeTopLevelKey(yaml, key);
        return withoutExisting.stripTrailing() + "\n\n" + key + ":\n" + blockBody.stripTrailing() + "\n";
    }

    private static String removeTopLevelKey(final String yaml, final String key) {
        final String[] lines = yaml.split("\n", -1);
        final StringBuilder result = new StringBuilder();
        boolean inBlock = false;
        for (final String line : lines) {
            if (line.equals(key + ":") || line.startsWith(key + ": ") || line.startsWith(key + ":\t")) {
                inBlock = true;
                continue;
            }
            if (inBlock) {
                if (line.isEmpty() || line.startsWith(" ") || line.startsWith("\t")) {
                    continue;
                }
                inBlock = false;
            }
            result.append(line).append('\n');
        }
        return result.toString();
    }
}
