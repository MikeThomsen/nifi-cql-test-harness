package org.apache.nifi.testharness.cql;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "start",
        description = {
                "Launch a Cassandra or ScyllaDB container for validating nifi-cql-bundle.",
                "",
                "The container is started detached with --rm, so a later `docker stop` cleans it up automatically;",
                "it is NOT stopped when this command exits, which is the point - it is meant to be left running",
                "while you point NiFi at it. Print the report at the end of a run for the contact point, any TLS",
                "material generated, and the exact command to remove the container yourself.",
                "",
                "By default the CQL port (9042) is published to a random available host port; use --host-port to",
                "pin a specific host port instead. Use --with-ssl to generate one-way or two-way TLS material and",
                "enable it on the container, and --with-test-user to provision username/password authentication",
                "with the given credentials.",
                "",
                "At least one option is required - bare `start` with nothing after it prints this help and exits",
                "with an error, rather than silently launching a plain, unauthenticated, no-TLS Cassandra container."
        },
        footer = {
                "",
                "Examples:",
                "  start --engine cassandra",
                "  start --engine scylladb --container-name my-scylla --host-port 9042",
                "  start --with-ssl=two-way --with-test-user --username admin --password secret",
                "  start --specific-image cassandra:5.0-beta1"
        }
)
final class StartCommand implements Callable<Integer> {

    @Spec
    private CommandSpec spec;

    // Fixed, non-secret bootstrap identity for ScyllaDB 2026.2+, which (unlike Cassandra) no longer provisions
    // a default superuser automatically. Reused as-is from ScyllaAuthenticationIT in nifi-cql-bundle: the
    // salted hash is a pre-computed SHA-512-crypt ($6$) of the plaintext password below, in the form
    // auth_superuser_salted_password expects. This identity is only ever used once, to create the real,
    // caller-requested role - there's no reason to compute a fresh hash per run for a throwaway bootstrap user.
    private static final String SCYLLA_BOOTSTRAP_USERNAME = "bootstrap_admin";
    private static final String SCYLLA_BOOTSTRAP_PASSWORD = "cassandra-bootstrap-only";
    private static final String SCYLLA_BOOTSTRAP_SALTED_PASSWORD =
            "$6$Yk/rTi56stmYje0c$JfFJrNQa5wUQAV/ZyyHqaAtGs.4CLcHbLh6RANryG6/pxeHvmPO0/eA8mSUY5HHs5w9sNFtwp.008MCHKo5ty.";

    private static final String CASSANDRA_BOOTSTRAP_USERNAME = "cassandra";
    private static final String CASSANDRA_BOOTSTRAP_PASSWORD = "cassandra";

    private static final int CQL_PORT = 9042;

    private static final Duration READINESS_TIMEOUT = Duration.ofMinutes(3);

    @Option(names = "--engine", description = "Database engine to launch: ${COMPLETION-CANDIDATES}. Default: ${DEFAULT-VALUE}.", defaultValue = "CASSANDRA")
    private Engine engine;

    @Option(names = "--specific-image", description = "Full image name and tag to use instead of the default image for --engine. Must be compatible with --engine.")
    private String specificImage;

    @Option(names = "--with-ssl", description = "Enable TLS. Optional value: one-way (default) or two-way.",
            arity = "0..1", fallbackValue = "ONE_WAY", converter = SslMode.Converter.class)
    private SslMode sslMode;

    @Option(names = "--with-test-user", description = "Provision username/password authentication. Requires --username and --password.")
    private boolean withTestUser;

    @Option(names = "--username", description = "Username to create when --with-test-user is set.")
    private String username;

    @Option(names = "--password", description = "Password to create when --with-test-user is set.")
    private String password;

    @Option(names = "--host-port", description = "Host port to map to the container's CQL port (" + CQL_PORT
            + "). If omitted, Docker assigns a random available port.")
    private Integer requestedHostPort;

    @Option(names = "--container-name", description = "Name to give the new Docker container. Default: an auto-generated name.")
    private String containerNameOption;

    @Override
    public Integer call() throws Exception {
        if (spec.commandLine().getParseResult().matchedOptions().isEmpty()) {
            spec.commandLine().usage(System.err);
            return 2;
        }
        if (specificImage != null && !engine.isCompatibleImage(specificImage)) {
            System.err.println("--specific-image '" + specificImage + "' does not look like a " + engine + " image (expected it to contain \""
                    + engine.imageNameToken() + "\"-like content, e.g. \"" + engine.defaultImage() + "\")");
            return 2;
        }
        if (withTestUser && (username == null || password == null)) {
            System.err.println("--with-test-user requires both --username and --password");
            return 2;
        }
        if (!DockerCli.isAvailable()) {
            System.err.println("Could not reach the Docker CLI. Is Docker installed and running?");
            return 2;
        }

        final String image = specificImage != null ? specificImage : engine.defaultImage();
        final String containerName = containerNameOption != null
                ? containerNameOption
                : "nifi-cql-harness-" + engine.name().toLowerCase() + "-" + randomHex(4);
        final Path outputDirectory = Path.of("nifi-cql-test-harness-output", containerName);
        Files.createDirectories(outputDirectory);

        System.out.println("Extracting default configuration from " + image + " ...");
        String config = extractDefaultConfig(image, outputDirectory);

        final List<String> mountArgs = new ArrayList<>();

        SslProvisioner.SslSetupResult sslSetupResult = null;
        if (sslMode != null) {
            System.out.println("Generating " + (sslMode == SslMode.ONE_WAY ? "one-way" : "two-way") + " TLS material ...");
            sslSetupResult = SslProvisioner.provision(engine, sslMode, outputDirectory, mountArgs);
            config = sslSetupResult.applyTo(config);
        }

        final String bootstrapUsername;
        final String bootstrapPassword;
        if (withTestUser) {
            System.out.println("Generating authentication configuration ...");
            config = YamlPatcher.setScalar(config, "authenticator", "PasswordAuthenticator");
            if (engine == Engine.SCYLLADB) {
                config = YamlPatcher.setScalar(config, "auth_superuser_name", SCYLLA_BOOTSTRAP_USERNAME);
                config = YamlPatcher.setScalar(config, "auth_superuser_salted_password", "\"" + SCYLLA_BOOTSTRAP_SALTED_PASSWORD + "\"");
                bootstrapUsername = SCYLLA_BOOTSTRAP_USERNAME;
                bootstrapPassword = SCYLLA_BOOTSTRAP_PASSWORD;
            } else {
                bootstrapUsername = CASSANDRA_BOOTSTRAP_USERNAME;
                bootstrapPassword = CASSANDRA_BOOTSTRAP_PASSWORD;
            }
        } else {
            bootstrapUsername = null;
            bootstrapPassword = null;
        }

        final Path patchedConfigPath = outputDirectory.resolve(configFileName());
        Files.writeString(patchedConfigPath, config);
        mountArgs.add("-v");
        // Not read-only - see the comment in SslProvisioner.addMount for why.
        mountArgs.add(patchedConfigPath.toAbsolutePath() + ":" + engine.configContainerPath());

        System.out.println("Starting container " + containerName + " (" + image + ") ...");
        final String containerId = DockerCli.runDetached(image, containerName, CQL_PORT, requestedHostPort, mountArgs);

        final int mappedHostPort = DockerCli.getMappedPort(containerId, CQL_PORT);
        System.out.println("Mapped host port " + mappedHostPort + " -> container CQL port " + CQL_PORT);
        final String contactPoint = "127.0.0.1:" + mappedHostPort;

        System.out.println("Waiting for " + contactPoint + " to accept connections ...");
        waitForPort("127.0.0.1", mappedHostPort, READINESS_TIMEOUT);

        if (withTestUser) {
            System.out.println("Creating role '" + username + "' ...");
            final var bootstrapSslContext = sslSetupResult == null ? null : sslSetupResult.bootstrapSslContext();
            TestUserProvisioner.createRole(contactPoint, bootstrapUsername, bootstrapPassword, username, password, bootstrapSslContext);
        }

        printReport(containerName, containerId, image, contactPoint, sslSetupResult, outputDirectory);
        return 0;
    }

    private String extractDefaultConfig(final String image, final Path outputDirectory) throws IOException, InterruptedException {
        final String tempContainerId = DockerCli.createContainer(image);
        try {
            final Path extractedPath = outputDirectory.resolve("default-" + configFileName());
            DockerCli.copyFromContainer(tempContainerId, engine.configContainerPath(), extractedPath);
            return Files.readString(extractedPath, StandardCharsets.UTF_8);
        } finally {
            DockerCli.removeContainer(tempContainerId);
        }
    }

    private String configFileName() {
        return engine == Engine.CASSANDRA ? "cassandra.yaml" : "scylla.yaml";
    }

    private static void waitForPort(final String host, final int port, final Duration timeout) throws InterruptedException {
        final Instant deadline = Instant.now().plus(timeout);
        IOException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 1000);
                return;
            } catch (final IOException e) {
                lastFailure = e;
                Thread.sleep(2000);
            }
        }
        throw new IllegalStateException("Timed out waiting for " + host + ":" + port + " to accept connections", lastFailure);
    }

    private static String randomHex(final int bytes) {
        final byte[] value = new byte[bytes];
        new SecureRandom().nextBytes(value);
        return HexFormat.of().formatHex(value);
    }

    private void printReport(final String containerName, final String containerId, final String image, final String contactPoint,
                              final SslProvisioner.SslSetupResult sslSetupResult, final Path outputDirectory) {
        System.out.println();
        System.out.println("Container ready.");
        System.out.println("  Name:          " + containerName);
        System.out.println("  Id:            " + containerId);
        System.out.println("  Image:         " + image);
        System.out.println("  Contact point: " + contactPoint);
        if (sslSetupResult != null) {
            System.out.println("  TLS:           " + (sslMode == SslMode.ONE_WAY ? "one-way" : "two-way"));
            System.out.println("    Truststore (JKS): " + sslSetupResult.trustStore().path().toAbsolutePath());
            System.out.println("    Truststore password: " + sslSetupResult.trustStore().password());
            if (sslSetupResult.clientPkcs12() != null) {
                System.out.println("    Client identity (PKCS12, no password): " + sslSetupResult.clientPkcs12().toAbsolutePath());
            }
        }
        if (withTestUser) {
            System.out.println("  Test user:     " + username + " / " + password);
        }
        System.out.println("  Output dir:    " + outputDirectory.toAbsolutePath());
        System.out.println();
        System.out.println("The container keeps running after this process exits (started with --rm, so it is");
        System.out.println("removed automatically once stopped). Stop it later with:");
        System.out.println("  docker stop " + containerName);
    }
}
