package org.apache.nifi.testharness.cql;

import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * Certificate, keystore, and PEM generation for {@code start --with-ssl}, built on the same
 * {@link StandardCertificateBuilder} helper the nifi-cql-bundle integration tests use (see
 * ScyllaSslIT/CassandraCQLExecutionServiceSslIT). One-way mode produces a single self-signed server
 * certificate; two-way mode additionally builds a pseudo CA and uses it to sign both the server and client
 * certificates.
 */
final class CertificateMaterial {

    private static final Duration VALIDITY = Duration.ofDays(365);

    private static final String CERTIFICATE_PEM_FORMAT = "-----BEGIN CERTIFICATE-----%n%s%n-----END CERTIFICATE-----%n";

    private static final String PRIVATE_KEY_PEM_FORMAT = "-----BEGIN PRIVATE KEY-----%n%s%n-----END PRIVATE KEY-----%n";

    record KeyAndCertificate(KeyPair keyPair, X509Certificate certificate) {
    }

    record Store(Path path, String password) {
    }

    private CertificateMaterial() {
    }

    static KeyPair generateKeyPair() {
        try {
            final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(2048);
            return keyPairGenerator.generateKeyPair();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("RSA key pair generation is not available", e);
        }
    }

    static KeyAndCertificate selfSigned(final String distinguishedName) {
        final KeyPair keyPair = generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal(distinguishedName), VALIDITY)
                .setDnsSubjectAlternativeNames(List.of("localhost"))
                .build();
        return new KeyAndCertificate(keyPair, certificate);
    }

    /** A self-signed CA certificate (subject == issuer, so {@code StandardCertificateBuilder} marks it CA:true). */
    static KeyAndCertificate selfSignedCertificateAuthority(final String distinguishedName) {
        return selfSigned(distinguishedName);
    }

    /** A leaf certificate for {@code leafDistinguishedName}, signed by the given CA's key pair. */
    static KeyAndCertificate caSigned(final KeyAndCertificate certificateAuthority, final String leafDistinguishedName) {
        final KeyPair leafKeyPair = generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(
                certificateAuthority.keyPair(), certificateAuthority.certificate().getSubjectX500Principal(), VALIDITY)
                .setSubject(new X500Principal(leafDistinguishedName))
                .setSubjectPublicKey(leafKeyPair.getPublic())
                .setDnsSubjectAlternativeNames(List.of("localhost"))
                .build();
        return new KeyAndCertificate(leafKeyPair, certificate);
    }

    /** A JKS truststore containing a single trusted certificate entry, with a freshly generated password. */
    static Store writeJksTrustStore(final X509Certificate certificate, final String alias, final Path path) throws GeneralSecurityException, IOException {
        final String password = generatePassword();
        final KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null);
        trustStore.setCertificateEntry(alias, certificate);
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            trustStore.store(outputStream, password.toCharArray());
        }
        return new Store(path, password);
    }

    /** A JKS keystore containing a private key and its certificate, with a freshly generated password - used
     * server-side (the container never receives the client's own credential material). */
    static Store writeJksKeyStore(final PrivateKey privateKey, final X509Certificate certificate, final String alias, final Path path)
            throws GeneralSecurityException, IOException {
        final String password = generatePassword();
        final KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null);
        keyStore.setKeyEntry(alias, privateKey, password.toCharArray(), new X509Certificate[]{certificate});
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            keyStore.store(outputStream, password.toCharArray());
        }
        return new Store(path, password);
    }

    /** The two-way client identity, delivered to the user as a password-less PKCS12 file per spec. */
    static Path writePkcs12NoPassword(final PrivateKey privateKey, final X509Certificate certificate, final String alias, final Path path)
            throws GeneralSecurityException, IOException {
        final char[] noPassword = new char[0];
        final KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null);
        keyStore.setKeyEntry(alias, privateKey, noPassword, new X509Certificate[]{certificate});
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            keyStore.store(outputStream, noPassword);
        }
        return path;
    }

    /** PEM-encoded certificate, for ScyllaDB's {@code client_encryption_options.certificate}/{@code truststore}. */
    static Path writeCertificatePem(final X509Certificate certificate, final Path path) throws GeneralSecurityException, IOException {
        final String encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(certificate.getEncoded());
        Files.writeString(path, String.format(CERTIFICATE_PEM_FORMAT, encoded));
        return path;
    }

    /** PEM-encoded private key, for ScyllaDB's {@code client_encryption_options.keyfile}. */
    static Path writePrivateKeyPem(final PrivateKey privateKey, final Path path) throws IOException {
        final String encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(privateKey.getEncoded());
        Files.writeString(path, String.format(PRIVATE_KEY_PEM_FORMAT, encoded));
        return path;
    }

    private static String generatePassword() {
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] bytes = new byte[24];
        secureRandom.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }
}
