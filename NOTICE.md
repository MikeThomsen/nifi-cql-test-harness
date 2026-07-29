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
