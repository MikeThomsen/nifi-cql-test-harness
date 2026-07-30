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
| `convert/CqlRowConverter.java` | The row-to-`MapRecord` loop in `CassandraCQLExecutionService#query` |

## Code written for this project

`RegisterTypeCommand.java` and `RecordUdtGenerator.java` (the `register-type` command, Avro schema
file -> CQL `CREATE TYPE` statement(s)) are **original** code, not ported from anywhere -
nifi-cql-bundle only ever converts CQL to Avro/Record, never the reverse. They're listed here only
because they're additional consumers of the `nifi-record`/`nifi-avro-record-utils` dependencies
above (via `AvroTypeUtil.createSchema`, the same call `CqlRecordSchemaMapper` uses in the opposite
direction), not because they're derived code like the table above.

## Notable transitive dependencies

| Dependency | Version | License | Pulled in by |
|---|---|---|---|
| `org.bouncycastle:bcpkix-jdk18on` | 1.84 | Bouncy Castle License (MIT-style) | `nifi-security-cert-builder` |
| `org.bouncycastle:bcprov-jdk18on` | 1.84 | Bouncy Castle License (MIT-style) | `nifi-security-cert-builder` |
| `org.bouncycastle:bcutil-jdk18on` | 1.84 | Bouncy Castle License (MIT-style) | `nifi-security-cert-builder` (via `bcpkix-jdk18on`) |
| Netty (`io.netty:netty-*`) | 4.1.x | Apache License 2.0 | CQL driver (both `cassandra` and `scylladb` profiles) |
| Jackson (`com.fasterxml.jackson.core:*`) | 2.x | Apache License 2.0 | CQL driver |
| `com.typesafe:config` | 1.4.1 | Apache License 2.0 | CQL driver (driver configuration loading) |
| `org.slf4j:slf4j-api` | 1.7.x | MIT License | CQL driver |
| `io.dropwizard.metrics:metrics-core` | 4.1.x | Apache License 2.0 | CQL driver |
| `org.apache.avro:avro` | 1.12.1 | Apache License 2.0 | `nifi-avro-record-utils` |
| `commons-io:commons-io` | 2.22.0 | Apache License 2.0 | `nifi-avro-record-utils` |
| `org.apache.commons:commons-compress` | 1.28.0 | Apache License 2.0 | `nifi-avro-record-utils` |
| `com.github.ben-manes.caffeine:caffeine` | 3.2.4 | Apache License 2.0 | `nifi-avro-record-utils` |

Not exhaustively enumerated below that tier; everything observed in `mvn dependency:tree` for both
the `cassandra` and `scylladb` profiles is Apache License 2.0 or MIT, with the one exception above
(Bouncy Castle's own license, also permissive/MIT-style).

## Build-time only (not bundled in the produced jar/native image)

| Plugin | Version | License |
|---|---|---|
| `org.graalvm.buildtools:native-maven-plugin` | 1.1.6 | Universal Permissive License (UPL) 1.0 |

## Maintenance note

Re-run `mvn dependency:tree` (per active profile) and `mvn dependency:tree -P scylladb` after any
version bump, and update this file if a new direct or notable transitive dependency is introduced
or a license changes.
