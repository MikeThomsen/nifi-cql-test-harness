# NOTICE

This project (`nifi-cql-test-harness`) is not part of the Apache NiFi release process; this file
tracks the license and provenance of its dependencies for reference, not as an ASF-format NOTICE.

## Direct dependencies

| Dependency | Version | License | Notes |
|---|---|---|---|
| `info.picocli:picocli` | 4.7.7 | Apache License 2.0 | CLI argument parsing. |
| `org.apache.cassandra:java-driver-core` (`cassandra` profile) | 4.19.3 | Apache License 2.0 | The former DataStax OSS Java driver, donated to and now maintained by the Apache Cassandra project. The legacy `com.datastax.oss:java-driver-core` coordinate is frozen at 4.17.0 and no longer receives updates. |
| `com.scylladb:java-driver-core` (`scylladb` profile) | 4.19.2.0 | Apache License 2.0 | ScyllaDB's fork of the DataStax OSS driver; same `com.datastax.oss.driver.*` Java package as the Cassandra driver above. |
| `org.apache.nifi:nifi-security-cert-builder` | 2.10.0 | Apache License 2.0 | Self-signed and CA-signed X.509 certificate builder, used for `start --with-ssl`. Same helper class (`StandardCertificateBuilder`) the nifi-cql-bundle integration tests use. |
| `org.apache.nifi:nifi-record` | 2.10.0 | Apache License 2.0 | The `Record`/`RecordSchema`/`RecordFieldType` model used both by the ported conversion code in `org.apache.nifi.testharness.cql.convert` (see "Code derived from nifi-cql-bundle" below) and by `register-type`/`RecordUdtGenerator` (original code - see "Code written for this project" below). |
| `org.apache.nifi:nifi-avro-record-utils` | 2.10.0 | Apache License 2.0 | Provides `AvroTypeUtil`, used by `CqlRecordSchemaMapper` (CQL -> Avro -> RecordSchema) and by `register-type` (Avro file -> RecordSchema) to convert between an Avro `Schema` and a `RecordSchema`. Transitively brings in Avro itself. |
| `net.datafaker:datafaker` | 2.7.0 | Apache License 2.0 | Fake data generation for `generate-test-data`. The original `com.github.javafaker:javafaker` is unmaintained (last release 2020); DataFaker is its actively maintained successor. |

## Code derived from nifi-cql-bundle

Unlike everything above (external dependencies pulled in as jars), the following source files in
`src/main/java/org/apache/nifi/testharness/cql/convert/` are **not** independent originals - they
are ported/adapted from nifi-cql-bundle's Record<->CQL type conversion logic
(`nifi-extension-bundles/nifi-cql-bundle/nifi-cassandra-session-provider-service/src/main/java/org/apache/nifi/service/cassandra/`
in the main `nifi` checkout). `nifi-cql-bundle` is never imported as a dependency here - the logic
was read and reimplemented in this project instead, per instruction. Both codebases are Apache
License 2.0 Apache NiFi code, so this is same-project internal reuse, not a third-party license
concern - listed here for provenance/audit purposes.

| This project | Derived from (nifi-cql-bundle) |
|---|---|
| `convert/codec/FlexibleBigIntCodec.java`, `FlexibleBooleanCodec.java`, `FlexibleCounterCodec.java`, `FlexibleDoubleCodec.java`, `FlexibleFloatCodec.java`, `FlexibleIntCodec.java`, `FlexibleSmallIntCodec.java`, `FlexibleTinyIntCodec.java`, `CharacterCodec.java`, `JavaSQLDateCodec.java`, `JavaSQLTimeCodec.java`, `JavaSQLTimestampCodec.java` | `mapping/` package, same class names, in `nifi-cassandra-session-provider-service` |
| `convert/CqlCodecs.java` | The `codecRegistry.register(...)` block in `CassandraCQLExecutionService#connectToCassandra` |
| `convert/CqlUdtSchemaMapper.java` | `mapping/CassandraUdtSchemaMapper.java` |
| `convert/CqlRecordSchemaMapper.java` | `CassandraCQLExecutionService#createSchema` (plus its `AvroTypeUtil.createSchema` call in `#query`) |
| `convert/CqlBindValueConverter.java` | `CassandraCQLExecutionService#convertForCqlType` / `#getBindValues` |

## Code written for this project

`RegisterTypeCommand.java`/`RecordUdtGenerator.java` (the `register-type` command, Avro schema file
-> CQL `CREATE TYPE` statement(s)), `GenerateTableCommand.java`/`SnakeCaseConverter.java` (the
`generate-table` command, Avro schema file -> CQL `CREATE TABLE` statement, reusing
`RecordUdtGenerator` for any nested UDTs), and `GenerateTestDataCommand.java`/`TestDataGenerator.java`/
`CqlLiteralFormatter.java` (the `generate-test-data` command, Avro schema file -> fake `Record`s
written live or dumped as literal CQL `INSERT` statements) are **original** code, not ported from
anywhere - nifi-cql-bundle only ever converts CQL to Avro/Record, never the reverse. They're listed
here only because they're additional consumers of the `nifi-record`/`nifi-avro-record-utils`
dependencies above (via `AvroTypeUtil.createSchema`, the same call `CqlRecordSchemaMapper` uses in
the opposite direction) and, for the `generate-test-data` files, of `net.datafaker:datafaker` above -
not because they're derived code like the table above.

## Transitive dependencies by license

Every dependency below is transitive (pulled in by a direct dependency, never declared in `pom.xml`
directly) and appears in `mvn dependency:tree` for the `cassandra` and/or `scylladb` profile. This
list is exhaustive for both profiles, not a "notable" sample, and every license here has been checked
against the Apache Software Foundation's Category X (prohibited) list at
<https://www.apache.org/legal/resolved.html> - none of them appear on it. Where the same artifact
resolves to a different version per profile, both are listed.

### Apache License 2.0

| Dependency | Version | Pulled in by |
|---|---|---|
| `org.apache.nifi:nifi-standard-record-utils` | 2.10.0 | `nifi-avro-record-utils` |
| `org.apache.nifi:nifi-security-utils-api` | 2.10.0 | `nifi-standard-record-utils` |
| `org.apache.nifi:nifi-schema-registry-service-api` | 2.10.0 | `nifi-avro-record-utils` |
| `org.apache.nifi:nifi-record-serialization-service-api` | 2.10.0 | `nifi-avro-record-utils` |
| `org.apache.commons:commons-csv` | 1.14.1 | `nifi-standard-record-utils` |
| `org.apache.commons:commons-text` | 1.15.0 | `nifi-standard-record-utils` |
| `org.apache.commons:commons-lang3` | 3.20.0 | `nifi-avro-record-utils` |
| `org.apache.avro:avro` | 1.12.1 | `nifi-avro-record-utils` |
| `commons-io:commons-io` | 2.22.0 | `nifi-avro-record-utils` |
| `org.apache.commons:commons-compress` | 1.28.0 | `nifi-avro-record-utils` |
| `commons-codec:commons-codec` | 1.19.0 | `commons-compress` |
| `com.fasterxml.jackson.core:jackson-databind` | 2.22.0 | `nifi-avro-record-utils` |
| `com.fasterxml.jackson.core:jackson-annotations` | 2.22 | `jackson-databind` |
| `com.fasterxml.jackson.core:jackson-core` | 2.20.1 (cassandra) / 2.21.3 (scylladb) | CQL driver |
| `com.github.ben-manes.caffeine:caffeine` | 3.2.4 | `nifi-avro-record-utils` |
| `org.jspecify:jspecify` | 1.0.0 | `caffeine` |
| `com.google.errorprone:error_prone_annotations` | 2.49.0 | `caffeine` |
| `org.yaml:snakeyaml` | 2.6 | `net.datafaker:datafaker` (locale data is stored as YAML) |
| `com.github.curious-odd-man:rgxgen` | 3.1 | `net.datafaker:datafaker` (regex-based generators) |
| `com.googlecode.libphonenumber:libphonenumber` | 9.0.33 | `net.datafaker:datafaker` (phone number generators) |
| `com.datastax.oss:native-protocol` (cassandra profile) | 1.5.2 | CQL driver |
| `com.scylladb:native-protocol` (scylladb profile) | 1.5.2.2 | CQL driver |
| `org.apache.cassandra:java-driver-guava-shaded` (cassandra profile) | 4.19.3 | CQL driver |
| `com.scylladb:java-driver-guava-shaded` (scylladb profile) | 4.19.2.0 | CQL driver |
| Netty (`io.netty:netty-common`, `-resolver`, `-buffer`, `-transport`, `-transport-native-unix-common`, `-handler`, `-codec`) | 4.1.130.Final (cassandra) / 4.1.135.Final (scylladb) | CQL driver |
| `com.typesafe:config` | 1.4.1 (cassandra) / 1.4.8 (scylladb) | CQL driver (driver configuration loading) |
| `io.dropwizard.metrics:metrics-core` | 4.1.18 (cassandra) / 4.2.38 (scylladb) | CQL driver |
| `com.github.jnr:jnr-ffi` | 2.2.11 (cassandra) / 2.2.19 (scylladb) | `jnr-posix` |
| `com.github.jnr:jnr-constants` | 0.10.3 (cassandra) / 0.10.4 (scylladb) | `jnr-posix` |
| `com.github.jnr:jffi` | 1.3.9 (cassandra) / 1.3.15 (scylladb) | `jnr-ffi` |
| `com.github.jnr:jnr-a64asm` | 1.0.0 | `jnr-ffi` |

### MIT License

| Dependency | Version | Pulled in by |
|---|---|---|
| `org.slf4j:slf4j-api` | 2.0.18 | `nifi-record` |
| `com.github.jnr:jnr-x86asm` | 1.0.2 | `jnr-ffi` |

### Bouncy Castle Licence (MIT-style)

| Dependency | Version | Pulled in by |
|---|---|---|
| `org.bouncycastle:bcpkix-jdk18on` | 1.84 | `nifi-security-cert-builder` |
| `org.bouncycastle:bcprov-jdk18on` | 1.84 | `nifi-security-cert-builder` |
| `org.bouncycastle:bcutil-jdk18on` | 1.84 | `nifi-security-cert-builder` (via `bcpkix-jdk18on`) |

### BSD-3-Clause

| Dependency | Version | Pulled in by |
|---|---|---|
| `org.ow2.asm:asm`, `asm-commons`, `asm-analysis`, `asm-tree`, `asm-util` | 9.2 (cassandra) / 9.7.1 (scylladb) | `jnr-ffi` |

### BSD-2-Clause / Public Domain (CC0 1.0) - dual-licensed

| Dependency | Version | Pulled in by |
|---|---|---|
| `org.hdrhistogram:HdrHistogram` | 2.1.12 (cassandra) / 2.2.2 (scylladb) | CQL driver |

### CC0 1.0 / MIT-0

| Dependency | Version | Pulled in by |
|---|---|---|
| `org.reactivestreams:reactive-streams` | 1.0.3 (cassandra) / 1.0.4 (scylladb) | CQL driver |

### Eclipse Public License 2.0

| Dependency | Version | Pulled in by |
|---|---|---|
| `com.github.jnr:jnr-posix` | 3.1.15 (cassandra) / 3.1.22 (scylladb) | CQL driver (POSIX bindings used by the driver's native-transport layer) |

EPL 2.0 is the one dependency in this project that isn't Category A (permissive) like everything else
above - the ASF classifies it as **Category B** (weak copyleft): permitted for inclusion, but its own
attribution/notice terms apply to `jnr-posix` specifically rather than it being interchangeable with
the Apache-licensed code around it. It is not Category X (prohibited) - GPL/LGPL/AGPL, SSPL,
BSD-4-Clause, and the other licenses actually banned by
<https://www.apache.org/legal/resolved.html> do not appear anywhere in this project's dependency tree
for either profile.

## Build-time only (not bundled in the produced jar/native image)

| Plugin | Version | License |
|---|---|---|
| `org.graalvm.buildtools:native-maven-plugin` | 1.1.6 | Universal Permissive License (UPL) 1.0 |

## Maintenance note

Re-run `mvn dependency:tree` (per active profile) and `mvn dependency:tree -P scylladb` after any
version bump, and update this file if a new direct or transitive dependency is introduced or a
license changes - including re-checking the new/changed license against
<https://www.apache.org/legal/resolved.html>'s Category X list.
