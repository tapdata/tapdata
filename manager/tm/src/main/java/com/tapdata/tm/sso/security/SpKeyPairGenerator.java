package com.tapdata.tm.sso.security;

import com.tapdata.tm.sso.dto.SpKeyPair;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.springframework.stereotype.Component;

import java.io.StringWriter;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

/**
 * Generates a fresh SP signing/decryption RSA key pair together with a self-signed
 * X.509 certificate, both PEM encoded. Uses BouncyCastle (already on the classpath)
 * so no JDK-internal APIs are required.
 */
@Component
public class SpKeyPairGenerator {

    private static final int KEY_SIZE = 2048;
    private static final int VALIDITY_YEARS = 10;
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";

    /**
     * Generate a new key pair and self-signed certificate.
     *
     * @param subjectDn the certificate subject/issuer DN, e.g. {@code CN=<sp-entity-id>}.
     * @return the PEM-encoded private key and certificate.
     */
    public SpKeyPair generate(String subjectDn) {
        try {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(KEY_SIZE);
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            String cn = (subjectDn == null || subjectDn.isBlank()) ? "CN=TapData SAML SP" : subjectDn;
            X500Name issuer = new X500Name(cn);
            BigInteger serial = BigInteger.valueOf(System.currentTimeMillis());
            Instant now = Instant.now();
            Date notBefore = Date.from(now.minus(1, ChronoUnit.DAYS));
            Date notAfter = Date.from(now.plus(365L * VALIDITY_YEARS, ChronoUnit.DAYS));

            JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                    issuer, serial, notBefore, notAfter, issuer, keyPair.getPublic());
            ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
                    .build(keyPair.getPrivate());
            X509Certificate certificate = new JcaX509CertificateConverter()
                    .getCertificate(certBuilder.build(signer));

            String privateKeyPem = toPem("PRIVATE KEY", keyPair.getPrivate().getEncoded());
            String certificatePem = toPem("CERTIFICATE", certificate.getEncoded());
            return new SpKeyPair(privateKeyPem, certificatePem);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate SP key pair", e);
        }
    }

    private String toPem(String type, byte[] der) throws Exception {
        StringWriter stringWriter = new StringWriter();
        try (PemWriter pemWriter = new PemWriter(stringWriter)) {
            pemWriter.writeObject(new PemObject(type, der));
        }
        return stringWriter.toString();
    }
}
