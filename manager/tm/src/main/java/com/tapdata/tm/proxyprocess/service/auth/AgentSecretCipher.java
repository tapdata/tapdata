package com.tapdata.tm.proxyprocess.service.auth;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Reversible at-rest protection for the client secret an agent reported.
 *
 * <p>Why encryption and not just a hash: TM has to hand the secret back when an
 * operator configures a caller for that agent, so the value must be recoverable.
 * A hash is kept alongside for change detection; neither alone is enough.
 *
 * <p>Key material comes from {@code agent-process.credential.secret-key} and
 * falls back to a per-installation value so a default deployment still works.
 * AES-GCM is used so a tampered row fails to decrypt instead of yielding a wrong
 * secret.
 */
@Component
@Slf4j
public class AgentSecretCipher {

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 12;
    private static final int TAG_BITS = 128;
    private static final String PREFIX = "gcm:";

    private final SecretKeySpec key;
    private final SecureRandom random = new SecureRandom();

    public AgentSecretCipher(
            @Value("${agent-process.credential.secret-key:}") String configuredKey) {
        byte[] material = resolveKey(configuredKey);
        /*
         * Validated here rather than at first use: a key of the wrong length
         * makes every encrypt and decrypt fail, and the failure would otherwise
         * surface as a 500 from the registration endpoint long after startup
         * reported success.
         */
        if (material.length != 16 && material.length != 24 && material.length != 32) {
            throw new IllegalStateException("agent-process.credential.secret-key must be"
                    + " 16, 24 or 32 bytes, got " + material.length);
        }
        this.key = new SecretKeySpec(material, "AES");
    }

    /**
     * Accept a Base64 or plain 16/24/32 byte value. The default keeps a fresh install working;
     * production deployments should set the property so the key can be managed and rotated.
     */
    private static byte[] resolveKey(String configured) {
        if (StringUtils.isNotBlank(configured)) {
            try {
                byte[] decoded = Base64.getDecoder().decode(configured);
                if (decoded.length == 16 || decoded.length == 24 || decoded.length == 32) {
                    return decoded;
                }
            } catch (IllegalArgumentException ignored) {
                // not Base64; fall through to the raw form
            }
            byte[] raw = configured.getBytes(StandardCharsets.UTF_8);
            if (raw.length == 16 || raw.length == 24 || raw.length == 32) {
                return raw;
            }
            log.warn("agent-process.credential.secret-key is not 16/24/32 bytes; using the default key");
        }
        /*
         * Default key. Public on purpose so a single-node dev install works out of the box, and
         * so the operator sees in the docs that it must be replaced: it protects nothing against
         * someone holding the database.
         *
         * Exactly 32 bytes: AES accepts 16, 24 or 32 only, and a default of any other length
         * would make every registration fail with "Invalid AES key length".
         */
        return "tapdata-agent-credential-key-000".getBytes(StandardCharsets.UTF_8);
    }

    /**
     * One-way hash of a secret, for detecting that an agent changed it.
     *
     * <p>Deliberately distinct from {@link #encrypt}: a hash cannot be read back,
     * so it is safe to compare against without handling plaintext, while the
     * ciphertext exists only for the explicit reveal.
     */
    public String hash(String plaintext) {
        if (plaintext == null) {
            return null;
        }
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] value = digest.digest(plaintext.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to hash agent client secret", e);
        }
    }

    /** Constant-time compare of a candidate secret against a stored {@link #hash}. */
    public boolean matches(String plaintext, String storedHash) {
        String candidate = hash(plaintext);
        if (candidate == null || storedHash == null) {
            return false;
        }
        return java.security.MessageDigest.isEqual(
                candidate.getBytes(StandardCharsets.UTF_8),
                storedHash.getBytes(StandardCharsets.UTF_8));
    }

    /** Encrypt a plaintext secret for storage; {@code null} in, {@code null} out. */
    public String encrypt(String plaintext) {
        if (plaintext == null) {
            return null;
        }
        try {
            byte[] iv = new byte[IV_LENGTH];
            random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] cipherText = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            byte[] combined = new byte[iv.length + cipherText.length];
            System.arraycopy(iv, 0, combined, 0, iv.length);
            System.arraycopy(cipherText, 0, combined, iv.length, cipherText.length);
            return PREFIX + Base64.getEncoder().encodeToString(combined);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to encrypt agent client secret: " + e.getMessage(), e);
        }
    }

    /**
     * Decrypt a stored secret.
     *
     * @return the plaintext, or {@code null} when the value is missing or cannot be decrypted
     *     (a rotated key, a tampered row); callers treat that as "the agent must re-register"
     */
    public String decrypt(String stored) {
        if (StringUtils.isBlank(stored) || !stored.startsWith(PREFIX)) {
            return null;
        }
        try {
            byte[] combined = Base64.getDecoder().decode(stored.substring(PREFIX.length()));
            if (combined.length <= IV_LENGTH) {
                return null;
            }
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, combined, 0, IV_LENGTH));
            byte[] plain = cipher.doFinal(combined, IV_LENGTH, combined.length - IV_LENGTH);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("Unable to decrypt a stored agent client secret: {}", e.getMessage());
            return null;
        }
    }
}
