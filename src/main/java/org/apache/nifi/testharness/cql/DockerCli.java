package org.apache.nifi.testharness.cql;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Thin wrapper around the {@code docker} CLI via {@link ProcessBuilder}. Deliberately shells out rather than
 * pulling in Testcontainers or the Docker Java client: this tool's containers are meant to keep running after
 * the process exits (unlike Testcontainers' default reaper-driven lifecycle), and a plain CLI dependency is a
 * much better fit for a GraalVM native-image build than either of those, which need substantial reflection
 * configuration to work under native-image at all.
 */
final class DockerCli {

    private static final Pattern PORT_OUTPUT_PATTERN = Pattern.compile(":(\\d+)\\s*$");

    private DockerCli() {
    }

    /** {@code docker create <image>} - materializes the image's filesystem without starting it, so its
     * pristine default config can be read back with {@link #copyFromContainer}. */
    static String createContainer(final String image) throws IOException, InterruptedException {
        return run("create", image);
    }

    static void copyFromContainer(final String containerId, final String containerPath, final Path hostPath) throws IOException, InterruptedException {
        run("cp", containerId + ":" + containerPath, hostPath.toString());
    }

    static void removeContainer(final String containerId) throws IOException, InterruptedException {
        run("rm", "-f", containerId);
    }

    static void copyToContainer(final Path hostPath, final String containerName, final String containerPath) throws IOException, InterruptedException {
        run("cp", hostPath.toString(), containerName + ":" + containerPath);
    }

    /** {@code docker exec <containerName> <command...>}, waiting for and discarding output - for short-lived
     * setup commands (e.g. {@code mkdir -p}) that don't need a terminal attached. */
    static void exec(final String containerName, final String... command) throws IOException, InterruptedException {
        exec(containerName, null, command);
    }

    /** As {@link #exec(String, String...)}, but as root - only the file's owner or root can {@code chmod} it,
     * and {@code docker cp} writes files under the copying (host) user's numeric UID, which doesn't
     * necessarily correspond to any user - let alone the file's owner - inside the container. */
    static void execAsRoot(final String containerName, final String... command) throws IOException, InterruptedException {
        exec(containerName, "root", command);
    }

    private static void exec(final String containerName, final String user, final String[] command) throws IOException, InterruptedException {
        final List<String> args = new ArrayList<>();
        args.add("exec");
        if (user != null) {
            args.add("-u");
            args.add(user);
        }
        args.add(containerName);
        args.addAll(List.of(command));
        run(args.toArray(new String[0]));
    }

    /**
     * {@code docker run --rm -d --name <name> -p [<hostPort>:]<containerPort> <mountArgs...> <image>}. Returns
     * the new container's id. {@code --rm} only cleans up once the container is later stopped (e.g. via
     * {@code docker stop}) - it does not stop the container when this process exits, which is still the point
     * of running detached. If {@code hostPort} is null, Docker assigns a random available host port.
     */
    static String runDetached(final String image, final String name, final int containerPort, final Integer hostPort, final List<String> mountArgs)
            throws IOException, InterruptedException {
        final List<String> args = new ArrayList<>();
        args.add("run");
        args.add("--rm");
        args.add("-d");
        args.add("--name");
        args.add(name);
        args.add("-p");
        args.add(hostPort == null ? String.valueOf(containerPort) : (hostPort + ":" + containerPort));
        args.addAll(mountArgs);
        args.add(image);
        return run(args.toArray(new String[0]));
    }

    /** Parses {@code docker port <id> <containerPort>/tcp}, e.g. "0.0.0.0:54213", for the assigned host port. */
    static int getMappedPort(final String containerId, final int containerPort) throws IOException, InterruptedException {
        final String output = run("port", containerId, containerPort + "/tcp");
        final Matcher matcher = PORT_OUTPUT_PATTERN.matcher(output.lines().findFirst().orElse(""));
        if (!matcher.find()) {
            throw new IOException("Could not parse host port from 'docker port' output: " + output);
        }
        return Integer.parseInt(matcher.group(1));
    }

    /**
     * Runs a docker command with the current process's stdio inherited, for {@code client}'s cqlsh session -
     * unlike {@link #run}, which buffers output for parsing and isn't suitable for a long-lived interactive
     * subprocess (an interactive cqlsh REPL needs a real terminal attached, and even a one-shot {@code -e}
     * query should stream straight to our own stdout rather than being captured and re-printed). Returns the
     * subprocess's exit code directly rather than throwing on non-zero, matching normal CLI passthrough
     * semantics - docker's/cqlsh's own error output is already visible via the inherited stderr.
     */
    static int runInheritIO(final String... args) throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        command.add("docker");
        command.addAll(List.of(args));
        final Process process = new ProcessBuilder(command).inheritIO().start();
        return process.waitFor();
    }

    /** The container's own bridge-network IP, reachable from inside the container's own network namespace
     * (i.e. from `docker exec`) as an alternative to cqlsh's {@code 127.0.0.1} default - which doesn't work
     * for every image: ScyllaDB's, unlike Cassandra's, listens only on this address, not loopback. */
    static String inspectIpAddress(final String containerName) throws IOException, InterruptedException {
        return run("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerName);
    }

    static boolean isAvailable() {
        try {
            run("version", "--format", "{{.Server.Version}}");
            return true;
        } catch (final IOException | InterruptedException e) {
            return false;
        }
    }

    private static String run(final String... args) throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        command.add("docker");
        command.addAll(List.of(args));

        final Process process = new ProcessBuilder(command).start();
        final String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        final String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        final int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new IOException("docker " + String.join(" ", args) + " failed (exit " + exitCode + "): " + stderr);
        }
        return stdout;
    }
}
