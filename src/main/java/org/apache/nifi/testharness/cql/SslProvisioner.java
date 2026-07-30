package org.apache.nifi.testharness.cql;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Builds the TLS material for {@code start --with-ssl} and the {@code client_encryption_options} block each
 * engine's config needs to enable it. One-way mode generates a single self-signed server certificate. Two-way
 * mode additionally builds a pseudo certificate authority and uses it to sign both the server certificate and a
 * client certificate, which is delivered to the caller as a password-less PKCS12 file. In both modes, the
 * server's own certificate (not the CA) is what goes into the JKS truststore handed back for NiFi's
 * SSLContextService - trusting the leaf directly is sufficient for JSSE's trust-path check and keeps one-way
 * and two-way consistent with each other.
 *
 * <p>Cassandra's server-side TLS config is Java-keystore-based; ScyllaDB's is PEM-based (see
 * CassandraCQLExecutionServiceSslIT vs. ScyllaSslIT in nifi-cql-bundle) - each engine branch below mounts and
 * references the file format its own {@code client_encryption_options} schema expects.
 */
final class SslProvisioner {

    private static final String CASSANDRA_KEYSTORE_CONTAINER_PATH = "/etc/cassandra/harness-keystore.jks";
    private static final String CASSANDRA_SERVER_TRUSTSTORE_CONTAINER_PATH = "/etc/cassandra/harness-server-truststore.jks";

    private static final String SCYLLA_CERT_CONTAINER_PATH = "/etc/scylla/ssl/server.crt";
    private static final String SCYLLA_KEY_CONTAINER_PATH = "/etc/scylla/ssl/server.key";
    private static final String SCYLLA_TRUSTSTORE_CONTAINER_PATH = "/etc/scylla/ssl/client-truststore.pem";

    private static final String CA_DN = "CN=nifi-cql-test-harness-ca";
    private static final String SERVER_DN_FORMAT = "CN=%s-server";
    private static final String CLIENT_DN = "CN=nifi-cql-test-harness-client";

    record SslSetupResult(CertificateMaterial.Store trustStore, Path clientPkcs12, String blockKey, String blockBody,
                           SSLContext bootstrapSslContext, Path serverCertPem, Path clientKeyPem, Path clientCertPem,
                           Path caCertPem) {
        String applyTo(final String yaml) {
            return YamlPatcher.replaceBlock(yaml, blockKey, blockBody);
        }
    }

    private SslProvisioner() {
    }

    static SslSetupResult provision(final Engine engine, final SslMode sslMode, final Path outputDirectory, final List<String> mountArgs)
            throws GeneralSecurityException, IOException {
        final boolean twoWay = sslMode == SslMode.TWO_WAY;
        final String serverDn = String.format(SERVER_DN_FORMAT, engine.name().toLowerCase());

        // All generated TLS material lives under its own subfolder of the container's output directory, kept
        // separate from the extracted/patched engine config (cassandra.yaml/scylla.yaml) alongside it.
        final Path sslDirectory = outputDirectory.resolve("ssl");
        Files.createDirectories(sslDirectory);

        final CertificateMaterial.KeyAndCertificate ca = twoWay ? CertificateMaterial.selfSignedCertificateAuthority(CA_DN) : null;
        final CertificateMaterial.KeyAndCertificate server = twoWay
                ? CertificateMaterial.caSigned(ca, serverDn)
                : CertificateMaterial.selfSigned(serverDn);

        final CertificateMaterial.Store deliveredTrustStore = CertificateMaterial.writeJksTrustStore(
                server.certificate(), "server", sslDirectory.resolve("nifi-truststore.jks"));

        // PEM copy of the server certificate, kept regardless of engine: Cassandra's own client_encryption_options
        // block below still uses the JKS keystore, but `client` (docker exec cqlsh) only understands PEM. For
        // ScyllaDB this is the exact same certificate scyllaBlock mounts into the container as its live server
        // cert - reused as-is below rather than writing a duplicate.
        final Path serverCertPem = CertificateMaterial.writeCertificatePem(server.certificate(), sslDirectory.resolve("server-cert.pem"));

        Path clientPkcs12 = null;
        Path clientKeyPem = null;
        Path clientCertPem = null;
        Path caCertPem = null;
        CertificateMaterial.KeyAndCertificate client = null;
        if (twoWay) {
            client = CertificateMaterial.caSigned(ca, CLIENT_DN);
            clientPkcs12 = CertificateMaterial.writePkcs12NoPassword(
                    client.keyPair().getPrivate(), client.certificate(), "client", sslDirectory.resolve("nifi-client-identity.p12"));
            // PEM copies of the same client identity, for `client`'s cqlsh (userkey/usercert) - cqlsh only
            // understands PEM, never PKCS12.
            clientKeyPem = CertificateMaterial.writePrivateKeyPem(client.keyPair().getPrivate(), sslDirectory.resolve("client-key.pem"));
            clientCertPem = CertificateMaterial.writeCertificatePem(client.certificate(), sslDirectory.resolve("client-cert.pem"));
            // `client`'s cqlsh trust anchor in two-way mode: the CA, not the (CA-signed, non-self-signed) server
            // leaf. Pinning the leaf directly - which is what the JKS truststore above does, and what one-way
            // mode has to do since there's no CA there - works fine for the Java driver/JSSE (NiFi's use, and
            // Cassandra's own cqlsh) but not for ScyllaDB's separately-packaged cqlsh, which fails chain
            // validation ("unable to get local issuer certificate") without the actual issuing CA in the trust
            // store. Trusting the CA is the textbook-correct approach and works for both.
            caCertPem = CertificateMaterial.writeCertificatePem(ca.certificate(), sslDirectory.resolve("ca-cert.pem"));
        }

        final String blockBody = engine == Engine.CASSANDRA
                ? cassandraBlock(server, ca, twoWay, sslDirectory, mountArgs)
                : scyllaBlock(server, ca, twoWay, sslDirectory, mountArgs, serverCertPem);

        // The bootstrap CREATE ROLE connection (TestUserProvisioner) has to speak TLS to the same server, and
        // - in two-way mode - present a certificate the server's own truststore will accept, since
        // require_client_auth applies to every connection, not just the ones NiFi makes later.
        final SSLContext bootstrapSslContext = buildBootstrapSslContext(server.certificate(), client);

        return new SslSetupResult(deliveredTrustStore, clientPkcs12, "client_encryption_options", blockBody, bootstrapSslContext,
                serverCertPem, clientKeyPem, clientCertPem, caCertPem);
    }

    private static SSLContext buildBootstrapSslContext(final X509Certificate serverCertificate, final CertificateMaterial.KeyAndCertificate client)
            throws GeneralSecurityException, IOException {
        final KeyStore trustKeyStore = KeyStore.getInstance("JKS");
        trustKeyStore.load(null);
        trustKeyStore.setCertificateEntry("server", serverCertificate);
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustKeyStore);

        final KeyManager[] keyManagers;
        if (client == null) {
            keyManagers = null;
        } else {
            final char[] password = "bootstrap".toCharArray();
            final KeyStore clientKeyStore = KeyStore.getInstance("PKCS12");
            clientKeyStore.load(null);
            clientKeyStore.setKeyEntry("client", client.keyPair().getPrivate(), password, new X509Certificate[]{client.certificate()});
            final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(clientKeyStore, password);
            keyManagers = keyManagerFactory.getKeyManagers();
        }

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    private static String cassandraBlock(final CertificateMaterial.KeyAndCertificate server, final CertificateMaterial.KeyAndCertificate ca,
                                          final boolean requireClientAuth, final Path sslDirectory, final List<String> mountArgs)
            throws GeneralSecurityException, IOException {
        final CertificateMaterial.Store serverKeyStore = CertificateMaterial.writeJksKeyStore(
                server.keyPair().getPrivate(), server.certificate(), "server", sslDirectory.resolve("server-keystore.jks"));
        addMount(mountArgs, serverKeyStore.path(), CASSANDRA_KEYSTORE_CONTAINER_PATH);

        final StringBuilder block = new StringBuilder()
                .append("  enabled: true\n")
                .append("  optional: false\n")
                .append("  keystore: ").append(CASSANDRA_KEYSTORE_CONTAINER_PATH).append('\n')
                .append("  keystore_password: ").append(serverKeyStore.password()).append('\n')
                .append("  store_type: JKS\n")
                .append("  require_client_auth: ").append(requireClientAuth).append('\n');

        if (requireClientAuth) {
            final CertificateMaterial.Store serverTrustStore = CertificateMaterial.writeJksTrustStore(
                    ca.certificate(), "ca", sslDirectory.resolve("server-truststore.jks"));
            addMount(mountArgs, serverTrustStore.path(), CASSANDRA_SERVER_TRUSTSTORE_CONTAINER_PATH);
            block.append("  truststore: ").append(CASSANDRA_SERVER_TRUSTSTORE_CONTAINER_PATH).append('\n')
                    .append("  truststore_password: ").append(serverTrustStore.password()).append('\n');
        }
        return block.toString();
    }

    private static String scyllaBlock(final CertificateMaterial.KeyAndCertificate server, final CertificateMaterial.KeyAndCertificate ca,
                                       final boolean requireClientAuth, final Path sslDirectory, final List<String> mountArgs,
                                       final Path serverCertPem)
            throws GeneralSecurityException, IOException {
        final Path serverKeyPem = CertificateMaterial.writePrivateKeyPem(server.keyPair().getPrivate(), sslDirectory.resolve("server-key.pem"));
        addMount(mountArgs, serverCertPem, SCYLLA_CERT_CONTAINER_PATH);
        addMount(mountArgs, serverKeyPem, SCYLLA_KEY_CONTAINER_PATH);

        final StringBuilder block = new StringBuilder()
                .append("  enabled: true\n")
                .append("  certificate: ").append(SCYLLA_CERT_CONTAINER_PATH).append('\n')
                .append("  keyfile: ").append(SCYLLA_KEY_CONTAINER_PATH).append('\n')
                .append("  require_client_auth: ").append(requireClientAuth).append('\n');

        if (requireClientAuth) {
            final X509Certificate caCertificate = ca.certificate();
            final Path caCertPem = CertificateMaterial.writeCertificatePem(caCertificate, sslDirectory.resolve("client-truststore.pem"));
            addMount(mountArgs, caCertPem, SCYLLA_TRUSTSTORE_CONTAINER_PATH);
            block.append("  truststore: ").append(SCYLLA_TRUSTSTORE_CONTAINER_PATH).append('\n');
        }
        return block.toString();
    }

    private static void addMount(final List<String> mountArgs, final Path hostPath, final String containerPath) {
        mountArgs.add("-v");
        // Not mounted read-only: Cassandra's and ScyllaDB's entrypoint scripts chown files under /etc on
        // startup (to match the container's runtime user), which fails and aborts the container if the mount
        // is read-only.
        mountArgs.add(hostPath.toAbsolutePath() + ":" + containerPath);
    }
}
