package org.apache.nifi.testharness.cql;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Shells out to {@code docker exec} to open a {@code cqlsh} session inside a container previously launched by
 * {@code start}, using whatever TLS/auth configuration {@code start} recorded for it in
 * {@link ContainerRegistry}. Running cqlsh inside the container (rather than connecting from the host) means the
 * mapped host port is irrelevant here - cqlsh's own default of {@code 127.0.0.1:9042} is exactly the container's
 * own CQL port from inside its network namespace.
 */
@Command(
        name = "client",
        description = {
                "Open a cqlsh session against a container `start` previously launched, with matching TLS and",
                "authentication already wired up - reads the connection details `start` recorded for it",
                "(engine, SSL material, test-user credentials) from the registry at",
                "nifi-cql-test-harness-output/containers.json, rather than requiring them to be re-supplied here.",
                "",
                "Runs cqlsh inside the container's own network namespace via `docker exec`, so the container's",
                "mapped host port doesn't matter - cqlsh connects to 127.0.0.1:9042 there directly, which is the",
                "container's own CQL port.",
                "",
                "Only works for containers `start` itself launched: this reads the registry `start` writes, not",
                "arbitrary running containers."
        },
        footer = {
                "",
                "Examples:",
                "  client my-cassandra",
                "  client my-cassandra -e \"SELECT * FROM my_keyspace.my_table\""
        }
)
final class ClientCommand implements Callable<Integer> {

    private static final String CONTAINER_CLIENT_DIR = "/tmp/nifi-cql-test-harness-client";

    @Parameters(index = "0", description = "Name of a container previously started with `start`.")
    private String containerName;

    @Option(names = {"-e", "--execute"}, description = "Run this CQL statement and exit, instead of opening an interactive session.")
    private String execute;

    @Override
    public Integer call() throws Exception {
        if (!DockerCli.isAvailable()) {
            System.err.println("Could not reach the Docker CLI. Is Docker installed and running?");
            return 2;
        }

        final ContainerRegistry.ContainerInfo info = ContainerRegistry.lookup(ContainerRegistry.DEFAULT_PATH, containerName);
        if (info == null) {
            System.err.println("No record of container '" + containerName + "' in " + ContainerRegistry.DEFAULT_PATH.toAbsolutePath()
                    + " - it wasn't started with `start`. Run `start` first.");
            return 2;
        }

        final List<String> cqlshArgs = new ArrayList<>();
        cqlshArgs.add("cqlsh");

        if (info.sslMode() != null) {
            DockerCli.exec(containerName, "mkdir", "-p", CONTAINER_CLIENT_DIR);

            // One-way: the server's self-signed leaf is its own trust anchor - there's no CA. Two-way: the
            // server's leaf is CA-signed (not self-signed), so the CA itself has to be the trust anchor for
            // chain validation to succeed universally - see SslProvisioner's caCertPem javadoc.
            final boolean twoWay = info.sslMode() == SslMode.TWO_WAY;
            final String trustAnchorPem = twoWay ? info.caCertPem() : info.serverCertPem();
            DockerCli.copyToContainer(Path.of(trustAnchorPem), containerName, CONTAINER_CLIENT_DIR + "/trust-anchor.pem");
            final StringBuilder cqlshrc = new StringBuilder("[ssl]\n")
                    .append("certfile = ").append(CONTAINER_CLIENT_DIR).append("/trust-anchor.pem\n")
                    .append("validate = true\n");

            if (twoWay) {
                DockerCli.copyToContainer(Path.of(info.clientKeyPem()), containerName, CONTAINER_CLIENT_DIR + "/client-key.pem");
                DockerCli.copyToContainer(Path.of(info.clientCertPem()), containerName, CONTAINER_CLIENT_DIR + "/client-cert.pem");
                cqlshrc.append("userkey = ").append(CONTAINER_CLIENT_DIR).append("/client-key.pem\n")
                        .append("usercert = ").append(CONTAINER_CLIENT_DIR).append("/client-cert.pem\n");
            }

            final Path localCqlshrc = Files.createTempFile("nifi-cql-test-harness-cqlshrc", ".tmp");
            try {
                Files.writeString(localCqlshrc, cqlshrc.toString());
                DockerCli.copyToContainer(localCqlshrc, containerName, CONTAINER_CLIENT_DIR + "/cqlshrc");
            } finally {
                Files.deleteIfExists(localCqlshrc);
            }

            // `docker cp` preserves the copying (host) user's numeric UID on the files it writes into the
            // container. That UID happens to be able to read them back via `docker exec` on some images
            // (e.g. Cassandra's, if its default exec user's UID happens to match) but not others (e.g.
            // ScyllaDB's) - made world-readable here so cqlsh can read them regardless of which UID it
            // actually runs as. As root: only the owning UID (or root) can chmod a file, and the default
            // exec user generally isn't the owning UID here.
            DockerCli.execAsRoot(containerName, "chmod", "-R", "a+r", CONTAINER_CLIENT_DIR);

            cqlshArgs.add("--cqlshrc");
            cqlshArgs.add(CONTAINER_CLIENT_DIR + "/cqlshrc");
            cqlshArgs.add("--ssl");
        }

        if (info.username() != null) {
            cqlshArgs.add("-u");
            cqlshArgs.add(info.username());
            cqlshArgs.add("-p");
            cqlshArgs.add(info.password());
        }

        if (execute != null) {
            cqlshArgs.add("-e");
            cqlshArgs.add(execute);
        }

        // cqlsh's own 127.0.0.1 default doesn't hold for every image - see DockerCli.inspectIpAddress.
        cqlshArgs.add(DockerCli.inspectIpAddress(containerName));

        final List<String> dockerArgs = new ArrayList<>();
        dockerArgs.add("exec");
        // -it (allocate + attach a pty) is only for the interactive REPL; a one-shot -e query streams its
        // output through inherited stdio just fine without one, and forcing a pty could interfere if the
        // caller is piping/redirecting that output.
        if (execute == null) {
            dockerArgs.add("-it");
        }
        dockerArgs.add(containerName);
        dockerArgs.addAll(cqlshArgs);

        return DockerCli.runInheritIO(dockerArgs.toArray(new String[0]));
    }
}
