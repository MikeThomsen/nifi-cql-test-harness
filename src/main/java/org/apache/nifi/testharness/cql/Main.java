package org.apache.nifi.testharness.cql;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "nifi-cql-test-harness",
        mixinStandardHelpOptions = true,
        version = "nifi-cql-test-harness 1.0.0-SNAPSHOT",
        description = {
                "CLI utility for setting up Cassandra and ScyllaDB to validate nifi-cql-bundle.",
                "",
                "It launches a real Cassandra or ScyllaDB Docker container, optionally with TLS (one-way or",
                "two-way) and/or username/password authentication already configured, and reports the contact",
                "point and any generated credentials/certificates you need to point nifi-cql-bundle at it.",
                "",
                "Run 'nifi-cql-test-harness start --help' for the full set of options."
        },
        subcommands = StartCommand.class
)
public class Main implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    public static void main(final String[] args) {
        final CommandLine commandLine = new CommandLine(new Main());
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        System.exit(commandLine.execute(args));
    }
}
