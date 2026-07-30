package org.apache.nifi.testharness.cql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A small JSON file at {@link #DEFAULT_PATH} tracking every container {@code start} has launched - engine, SSL
 * material (if any), and test-user credentials (if any) - so {@code client <container-name>} can reconnect with
 * matching cqlsh options without the caller having to remember or re-supply them. One registry file covers every
 * container, keyed by name, so it works across multiple containers started over time.
 *
 * <p>This hand-rolled reader/writer only supports the flat, two-level, string-or-null shape this class actually
 * produces (an object of objects, no arrays/numbers/booleans) - deliberately not a general-purpose JSON library,
 * consistent with this project's existing preference for shelling out / minimal bespoke parsing (see
 * {@link DockerCli}, {@link YamlPatcher}) over pulling in a new dependency for something this narrow.
 */
final class ContainerRegistry {

    static final Path DEFAULT_PATH = Path.of("nifi-cql-test-harness-output", "containers.json");

    record ContainerInfo(Engine engine, SslMode sslMode, String serverCertPem, String clientKeyPem, String clientCertPem,
                          String caCertPem, String username, String password) {
    }

    private ContainerRegistry() {
    }

    static void upsert(final Path registryPath, final String containerName, final ContainerInfo info) throws IOException {
        final Map<String, ContainerInfo> registry = readAll(registryPath);
        registry.put(containerName, info);
        Files.createDirectories(registryPath.toAbsolutePath().getParent());
        Files.writeString(registryPath, toJson(registry));
    }

    static ContainerInfo lookup(final Path registryPath, final String containerName) throws IOException {
        return readAll(registryPath).get(containerName);
    }

    private static Map<String, ContainerInfo> readAll(final Path registryPath) throws IOException {
        if (!Files.isRegularFile(registryPath)) {
            return new LinkedHashMap<>();
        }
        final String json = Files.readString(registryPath).trim();
        if (json.isEmpty()) {
            return new LinkedHashMap<>();
        }
        final Map<String, ContainerInfo> registry = new LinkedHashMap<>();
        final Parser parser = new Parser(json);
        parser.expect('{');
        parser.skipWhitespace();
        if (parser.peek() == '}') {
            return registry;
        }
        while (true) {
            parser.skipWhitespace();
            final String containerName = parser.parseString();
            parser.skipWhitespace();
            parser.expect(':');
            parser.skipWhitespace();
            registry.put(containerName, toContainerInfo(parser.parseFlatObject()));
            parser.skipWhitespace();
            if (parser.peek() == ',') {
                parser.next();
                continue;
            }
            break;
        }
        parser.skipWhitespace();
        parser.expect('}');
        return registry;
    }

    private static ContainerInfo toContainerInfo(final Map<String, String> fields) {
        final String sslMode = fields.get("sslMode");
        return new ContainerInfo(
                Engine.valueOf(fields.get("engine")),
                sslMode == null ? null : SslMode.valueOf(sslMode),
                fields.get("serverCertPem"),
                fields.get("clientKeyPem"),
                fields.get("clientCertPem"),
                fields.get("caCertPem"),
                fields.get("username"),
                fields.get("password"));
    }

    private static String toJson(final Map<String, ContainerInfo> registry) {
        final StringBuilder json = new StringBuilder("{\n");
        int index = 0;
        for (final Map.Entry<String, ContainerInfo> entry : registry.entrySet()) {
            final ContainerInfo info = entry.getValue();
            json.append("  ").append(quote(entry.getKey())).append(": {\n")
                    .append("    \"engine\": ").append(quote(info.engine().name())).append(",\n")
                    .append("    \"sslMode\": ").append(stringOrNull(info.sslMode() == null ? null : info.sslMode().name())).append(",\n")
                    .append("    \"serverCertPem\": ").append(stringOrNull(info.serverCertPem())).append(",\n")
                    .append("    \"clientKeyPem\": ").append(stringOrNull(info.clientKeyPem())).append(",\n")
                    .append("    \"clientCertPem\": ").append(stringOrNull(info.clientCertPem())).append(",\n")
                    .append("    \"caCertPem\": ").append(stringOrNull(info.caCertPem())).append(",\n")
                    .append("    \"username\": ").append(stringOrNull(info.username())).append(",\n")
                    .append("    \"password\": ").append(stringOrNull(info.password())).append('\n')
                    .append("  }").append(++index < registry.size() ? ",\n" : "\n");
        }
        json.append("}\n");
        return json.toString();
    }

    private static String stringOrNull(final String value) {
        return value == null ? "null" : quote(value);
    }

    private static String quote(final String value) {
        final StringBuilder quoted = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            switch (c) {
                case '"' -> quoted.append("\\\"");
                case '\\' -> quoted.append("\\\\");
                case '\n' -> quoted.append("\\n");
                case '\r' -> quoted.append("\\r");
                case '\t' -> quoted.append("\\t");
                default -> {
                    if (c < 0x20) {
                        quoted.append(String.format("\\u%04x", (int) c));
                    } else {
                        quoted.append(c);
                    }
                }
            }
        }
        return quoted.append('"').toString();
    }

    /** Minimal recursive-descent parser for exactly the subset of JSON {@link #toJson} emits: objects, strings,
     * and null - no arrays, numbers, or booleans, since this schema never needs them. */
    private static final class Parser {
        private final String text;
        private int pos;

        Parser(final String text) {
            this.text = text;
        }

        char peek() {
            return text.charAt(pos);
        }

        void next() {
            pos++;
        }

        void skipWhitespace() {
            while (pos < text.length() && Character.isWhitespace(peek())) {
                next();
            }
        }

        void expect(final char c) {
            if (pos >= text.length() || peek() != c) {
                throw new IllegalStateException("Expected '" + c + "' at position " + pos + " in " + DEFAULT_PATH);
            }
            next();
        }

        String parseString() {
            expect('"');
            final StringBuilder value = new StringBuilder();
            while (peek() != '"') {
                final char c = peek();
                if (c == '\\') {
                    next();
                    final char escaped = peek();
                    switch (escaped) {
                        case '"' -> value.append('"');
                        case '\\' -> value.append('\\');
                        case '/' -> value.append('/');
                        case 'n' -> value.append('\n');
                        case 'r' -> value.append('\r');
                        case 't' -> value.append('\t');
                        case 'u' -> {
                            value.append((char) Integer.parseInt(text.substring(pos + 1, pos + 5), 16));
                            pos += 4;
                        }
                        default -> value.append(escaped);
                    }
                    next();
                } else {
                    value.append(c);
                    next();
                }
            }
            next();
            return value.toString();
        }

        String parseStringOrNull() {
            if (text.startsWith("null", pos)) {
                pos += 4;
                return null;
            }
            return parseString();
        }

        Map<String, String> parseFlatObject() {
            final Map<String, String> fields = new LinkedHashMap<>();
            expect('{');
            skipWhitespace();
            if (peek() == '}') {
                next();
                return fields;
            }
            while (true) {
                skipWhitespace();
                final String key = parseString();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                fields.put(key, parseStringOrNull());
                skipWhitespace();
                if (peek() == ',') {
                    next();
                    continue;
                }
                break;
            }
            skipWhitespace();
            expect('}');
            return fields;
        }
    }
}
