package com.tapdata.tm.sso.security;

import com.tapdata.tm.sso.dto.SpKeyPair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpKeyPairGeneratorTest {

    private final SpKeyPairGenerator generator = new SpKeyPairGenerator();

    @Test
    @DisplayName("generates a PEM private key and a parseable self-signed RSA-2048 cert")
    void generatesValidKeyPair() throws Exception {
        SpKeyPair keyPair = generator.generate("CN=TapData SP Test");

        assertNotNull(keyPair.getPrivateKeyPem());
        assertNotNull(keyPair.getCertificatePem());
        assertTrue(keyPair.getPrivateKeyPem().contains("-----BEGIN PRIVATE KEY-----"));
        assertTrue(keyPair.getCertificatePem().contains("-----BEGIN CERTIFICATE-----"));

        // private key parses as PKCS#8
        PrivateKey privateKey = parsePrivateKey(keyPair.getPrivateKeyPem());
        assertEquals("RSA", privateKey.getAlgorithm());

        // certificate parses and is RSA-2048, self-signed
        X509Certificate cert = parseCert(keyPair.getCertificatePem());
        RSAPublicKey publicKey = (RSAPublicKey) cert.getPublicKey();
        assertEquals(2048, publicKey.getModulus().bitLength());
        assertEquals(cert.getSubjectX500Principal(), cert.getIssuerX500Principal());
        cert.verify(cert.getPublicKey());
    }

    @Test
    @DisplayName("blank DN falls back to a default subject")
    void blankDnDefaults() throws Exception {
        SpKeyPair keyPair = generator.generate("  ");
        X509Certificate cert = parseCert(keyPair.getCertificatePem());
        assertTrue(cert.getSubjectX500Principal().getName().contains("TapData SAML SP"));
    }

    private PrivateKey parsePrivateKey(String pem) throws Exception {
        String base64 = pem.replaceAll("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        byte[] der = Base64.getDecoder().decode(base64);
        return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(der));
    }

    private X509Certificate parseCert(String pem) throws Exception {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        return (X509Certificate) factory.generateCertificate(
                new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
    }
}
