package com.tapdata.tm.sso.security;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Small helpers to turn PEM / Base64 strings (as stored in the SAML config) into
 * JCA objects for OpenSAML credentials. Kept dependency-light: only the JDK
 * {@link CertificateFactory} / {@link KeyFactory} are used.
 */
public final class SamlPemUtil {

    private SamlPemUtil() {
    }

    /**
     * Parse an X.509 certificate from a PEM block or bare Base64 DER.
     */
    public static X509Certificate parseCertificate(String pemOrBase64) {
        try {
            String pem = pemOrBase64.contains("-----BEGIN CERTIFICATE-----")
                    ? pemOrBase64.trim()
                    : "-----BEGIN CERTIFICATE-----\n"
                            + pemOrBase64.replaceAll("\\s", "")
                            + "\n-----END CERTIFICATE-----";
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(
                    new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid X.509 certificate", e);
        }
    }

    /**
     * Parse an RSA private key from a PKCS#8 PEM block ("BEGIN PRIVATE KEY").
     */
    public static PrivateKey parsePrivateKey(String pem) {
        try {
            String base64 = pem
                    .replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");
            byte[] der = Base64.getDecoder().decode(base64);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(der);
            return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid PKCS#8 private key", e);
        }
    }
}
