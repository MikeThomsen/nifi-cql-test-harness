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

    /** {@code docker run -d --name <name> -p <containerPort> <mountArgs...> <image>}. Returns the new container's id. */
    static String runDetached(final String image, final String name, final int containerPort, final List<String> mountArgs)
            throws IOException, InterruptedException {
        final List<String> args = new ArrayList<>();
        args.add("run");
        args.add("-d");
        args.add("--name");
        args.add(name);
        args.add("-p");
        args.add(String.valueOf(containerPort));
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
