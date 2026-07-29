package org.apache.nifi.testharness.cql;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "nifi-cql-test-harness",
        mixinStandardHelpOptions = true,
        version = "nifi-cql-test-harness 1.0.0-SNAPSHOT",
        description = "CLI utility for setting up Cassandra and ScyllaDB to validate nifi-cql-bundle."
)
public class Main implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    public static void main(final String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }
}
