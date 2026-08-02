# nifi-cql-test-harness

## Overview

_TODO: add a project overview here._

## Building

### Prerequisites

- JDK 21
- Apache Maven
- [Docker](https://docs.docker.com/get-docker/), for anything that launches or talks to a
  container (`start`, `client`, and the non-`--dry-run` path of `generate-test-data`). The
  `register-type`, `generate-table`, and `generate-test-data --dry-run` commands only read a
  local Avro schema file and don't need Docker.
- [GraalVM](https://www.graalvm.org/) (with the `native-image` component installed) instead of a
  regular JDK, only if you want a native executable - see "Native build" below. A regular JDK is
  enough for the standard jar build.

### Driver profile: Cassandra vs. ScyllaDB

The project builds against exactly one CQL driver at a time, selected by Maven profile:

- `cassandra` - the Apache Cassandra Java driver. Active by default; no flag needed.
- `scylladb` - ScyllaDB's fork of the same driver.

Pass `-P scylladb` to any of the commands below to build against the ScyllaDB driver instead. The
two profiles are mutually exclusive (`org.apache.cassandra:java-driver-core` vs.
`com.scylladb:java-driver-core`); whichever one is active is baked into the jar or native
executable you build.

### Standard build (jar)

From the project root:

```
mvn package
```

This produces a runnable jar at `target/nifi-cql-test-harness-1.0.0-SNAPSHOT.jar` (the manifest
already points `Main-Class` at `org.apache.nifi.testharness.cql.Main`), runnable with:

```
java -jar target/nifi-cql-test-harness-1.0.0-SNAPSHOT.jar --help
```

### Native build

Native builds skip the jar+JVM step entirely and produce a single self-contained native
executable, built with [GraalVM Native Image](https://www.graalvm.org/latest/reference-manual/native-image/)
via the `native` Maven profile:

```
mvn -Pnative package
```

`JAVA_HOME` must point at a GraalVM distribution that has the `native-image` tool installed (either
bundled already, or added with `gu install native-image` on older distributions) - a regular JDK
cannot run this profile.

Combine it with the `scylladb` profile the same way as any other profile combination:

```
mvn -Pnative,scylladb package
```

The build produces a native executable at `target/nifi-cql-test-harness`. That's the executable
used throughout the "Usage" section below.

## Usage

The examples below assume you've built the native executable (`target/nifi-cql-test-harness`) and
copied or symlinked it somewhere convenient - substitute your own path, or
`java -jar target/nifi-cql-test-harness-1.0.0-SNAPSHOT.jar` if you built the jar instead.

```
nifi-cql-test-harness --help
nifi-cql-test-harness --version
nifi-cql-test-harness help <command>
```

Every subcommand also accepts its own `--help` (or `nifi-cql-test-harness help <command>`) for the
full, exact set of options - what follows is a comprehensive tour, not the last word; the `--help`
output is always authoritative.

### `start` - launch a Cassandra/ScyllaDB container

Launches a real Cassandra or ScyllaDB Docker container to test against, optionally with TLS
and/or username/password authentication already configured. The container is started detached
with `--rm` and is **not** stopped when `start` exits - it's meant to be left running while you
point NiFi (or `nifi-cql-bundle`) at it.

```
# Plain Cassandra container, default image, random host port
nifi-cql-test-harness start --engine cassandra

# ScyllaDB, named container, fixed host port
nifi-cql-test-harness start --engine scylladb --container-name my-scylla --host-port 9042

# Two-way TLS plus a provisioned test user
nifi-cql-test-harness start --with-ssl=two-way --with-test-user --username admin --password secret

# A specific image/tag instead of the engine default
nifi-cql-test-harness start --specific-image cassandra:5.0-beta1

# Custom keyspace name
nifi-cql-test-harness start --engine cassandra --keyspace my_keyspace
```

Key options:

| Option | Description |
|---|---|
| `--engine` | `cassandra` (default) or `scylladb`. |
| `--specific-image` | Overrides the default image for `--engine`; must still match the engine (e.g. a `scylladb` image name for `--engine scylladb`). |
| `--with-ssl[=one-way\|two-way]` | Generates TLS material and enables it on the container. Bare `--with-ssl` defaults to one-way. |
| `--with-test-user` | Provisions username/password authentication. Requires `--username` and `--password`. |
| `--host-port` | Host port to map to the container's CQL port (9042). Omit to let Docker pick a random free port. |
| `--container-name` | Name for the new container. Omit for an auto-generated name. |
| `--keyspace` | Keyspace to create once the container is ready, with SimpleStrategy and replication_factor 1. Default: `nifi_test`. |

At least one option is required - a bare `start` prints help and exits with an error rather than
silently launching an unauthenticated, no-TLS container.

The keyspace is always created (even if `--keyspace` isn't explicitly passed, since it has a
default), right after the container accepts connections and before any `--with-test-user` role is
created - using whatever bootstrap identity is on hand (unauthenticated if `--with-test-user` was
not requested).

`start` prints a report at the end with the contact point, any generated TLS material or test-user
credentials, the container's output directory (under `nifi-cql-test-harness-output/`), and the
exact `docker stop` command to remove it later. It also records the container (engine, TLS mode,
credentials) in a local registry at `nifi-cql-test-harness-output/containers.json`, which `client`
reads back.

### `client` - open a `cqlsh` session

Opens an interactive (or one-shot) `cqlsh` session against a container `start` previously
launched, automatically wiring up whatever TLS/auth `start` configured for it - no need to
re-supply connection details.

```
# Interactive session
nifi-cql-test-harness client my-cassandra

# Run a single statement and exit
nifi-cql-test-harness client my-cassandra -e "SELECT * FROM my_keyspace.my_table"
```

This only works for containers `start` itself launched (it reads the registry `start` writes), not
arbitrary running containers.

### `register-type` - Avro schema to CQL `CREATE TYPE`

Converts an Avro schema file into CQL `CREATE TYPE` statement(s) and prints them to stdout. Nested
records become their own nested user-defined types, printed before the type that references them,
so the output can be pasted into `cqlsh` top-to-bottom without reordering. Does not connect to a
container - it only prints CQL for you to run yourself.

```
nifi-cql-test-harness register-type --schema-file address.avsc
nifi-cql-test-harness register-type --schema-file address.avsc --keyspace myks
```

| Option | Description |
|---|---|
| `--schema-file` | (required) Path to an Avro schema (`.avsc`) file. |
| `--keyspace` | Qualifies every generated type name with this keyspace. Left unqualified if omitted. |

### `generate-table` - Avro schema to CQL `CREATE TABLE`

Converts an Avro schema file into a CQL `CREATE TABLE` statement. Column and table names are
snake_case conversions of the Avro field/record names (`tenantId` -> `tenant_id`, `PersonRecord`
-> `person_record`). A nested record field becomes its own `CREATE TYPE`, printed before the
table, using the same conversion as `register-type`. Does not connect to a container.

```
nifi-cql-test-harness generate-table --schema-file person.avsc --primary-key id
nifi-cql-test-harness generate-table --schema-file person.avsc \
    --primary-key "(tenantId,userId),createdAt" --keyspace myks
nifi-cql-test-harness generate-table --schema-file person-with-embedded-key.avsc
```

The primary key can be supplied either via `--primary-key`, or embedded directly in the Avro
schema as a `primaryKey` property on the record (a convention of this tool, not a standard Avro
property):

```json
{ "type": "record", "name": "Person", "primaryKey": "(tenantId,userId),createdAt", "fields": [...] }
```

`--primary-key` always wins if both are present; if neither is present, that's an error. Either
way, the primary key uses the original Avro field names (not the converted column names) and
mirrors CQL's own `PRIMARY KEY` syntax:

| Syntax | Meaning |
|---|---|
| `field` | A single-column partition key, no clustering columns. |
| `field1,field2` | `field1` is the partition key; `field2` is a clustering column. |
| `(field1,field2),field3` | A composite partition key (`field1`, `field2`); `field3` is clustering. |

A primary key field whose type is an unfrozen collection (Avro array/map) is rejected, since CQL
requires those to be frozen to appear in a primary key.

| Option | Description |
|---|---|
| `--schema-file` | (required) Path to an Avro schema (`.avsc`) file. |
| `--primary-key` | Primary key spec, in Avro field names (see syntax above). Overrides a schema-embedded `primaryKey` property if both are present; required if the schema doesn't declare one. |
| `--keyspace` | Qualifies the table name (and any generated UDT names) with this keyspace. Left unqualified if omitted. |

### `generate-test-data` - fake data from an Avro schema

Generates fake test data matching an Avro schema, using [DataFaker](https://www.datafaker.net/)
for realistic-looking values (a field named e.g. `email` or `city` gets a matching generator;
anything else falls back to generic text). By default it writes live to a running container; pass
`--dry-run` to write the generated CQL to a file instead.

```
# Write 5000 (default) generated records straight to a running container
nifi-cql-test-harness generate-test-data --schema-file person.avsc \
    --contact-point 127.0.0.1:33193 --keyspace myks

# Generate CQL to a file instead of touching a container
nifi-cql-test-harness generate-test-data --schema-file person.avsc --dry-run --record-count 100

# Write live, and also archive the same generated CQL to a file
nifi-cql-test-harness generate-test-data --schema-file person.avsc \
    --contact-point 127.0.0.1:33193 --keyspace myks --archive-generated-cql-to-path ./archives
```

| Option | Description |
|---|---|
| `--schema-file` | (required) Path to an Avro schema (`.avsc`) file. |
| `--record-count` | Number of records to generate. Default: 5000. |
| `--dry-run` | Write generated CQL to a file (`<RecordName>-dry-run<timestamp>.cql`) instead of a container. Mutually exclusive with `--archive-generated-cql-to-path`. |
| `--archive-generated-cql-to-path <dir>` | Keeps the normal live write, but additionally saves the same generated CQL to a file in `<dir>` (`<RecordName>-archive<timestamp>.cql`). |
| `--contact-point` | `host:port` of an already-running container. Required unless `--dry-run`. |
| `--username` / `--password` | Credentials, if the container requires authentication. Must be supplied together. |
| `--keyspace` | Target keyspace. Required unless `--dry-run`. |
| `--randomize-temporal-fields` | Generate random `DATE`/`TIME`/`TIMESTAMP` values instead of the default per-field sequence (a plausible, incrementing time series). |

Every generated `INSERT` is emitted as literal CQL text - the same text whether it's written to a
file or executed live - so dry-run/archive output is exactly what a live run would have sent to
the database. If the schema references a nested record, this prints a non-blocking warning
reminding you to run `register-type` for it first; it doesn't check a live container for whether
that's actually been done.

## A note on how this project was built

This project is primarily developed with [Claude Code](https://claude.com/product/claude-code) to
facilitate testing of `nifi-cql-bundle`, but it contains original human input throughout,
including code adapted from Apache NiFi itself (see `NOTICE.md` for exactly which files, and what
they're derived from).
