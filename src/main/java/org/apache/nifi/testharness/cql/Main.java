package org.apache.nifi.testharness.cql;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
        name = "nifi-cql-test-harness",
        version = "nifi-cql-test-harness 1.0.0-SNAPSHOT",
        description = {
                "CLI utility for setting up Cassandra and ScyllaDB to validate nifi-cql-bundle.",
                "",
                "It launches a real Cassandra or ScyllaDB Docker container, optionally with TLS (one-way or",
                "two-way) and/or username/password authentication already configured, and reports the contact",
                "point and any generated credentials/certificates you need to point nifi-cql-bundle at it.",
                "",
                "Run 'nifi-cql-test-harness help start' (or 'help <command>' generally) for the full set of",
                "options for a given command."
        },
        subcommands = {StartCommand.class, RegisterTypeCommand.class, GenerateTableCommand.class, HelpCommand.class}
)
public class Main implements Runnable {

    @Option(names = {"-V", "--version"}, versionHelp = true, description = "Print version information and exit.")
    private boolean versionRequested;

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
