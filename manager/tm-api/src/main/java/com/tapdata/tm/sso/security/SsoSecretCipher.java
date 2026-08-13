package com.tapdata.tm.sso.security;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

/**
 * At-rest encryption for SSO secrets (e.g. SP signing/decryption private keys).
 * <p>
 * Uses AES-256-GCM with a random 12-byte IV per operation. The IV is prepended to
 * the ciphertext (which already carries the GCM authentication tag) and the whole
 * blob is Base64 encoded for storage. The 256-bit master key is injected from the
 * {@code SSO_MASTER_KEY} environment variable / property and is never persisted or
 * logged. This intentionally replaces the legacy {@code AES256Util} (hardcoded key,
 * ECB mode) which is unsuitable for storing private keys.
 */
@Component
public class SsoSecretCipher {

    private static final Logger log = LoggerFactory.getLogger(SsoSecretCipher.class);

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String KEY_ALGORITHM = "AES";
    private static final int IV_LENGTH = 12;
    private static final int TAG_LENGTH_BITS = 128;
    private static final int KEY_LENGTH_BYTES = 32;

    private final SecureRandom secureRandom = new SecureRandom();
    private final byte[] masterKey;

    public SsoSecretCipher(@Value("${sso.master.key:${SSO_MASTER_KEY:}}") String configuredKey) {
        this.masterKey = deriveMasterKey(configuredKey);
    }

    private static byte[] deriveMasterKey(String configuredKey) {
        if (StringUtils.isBlank(configuredKey)) {
            log.warn("SSO_MASTER_KEY is not configured. SSO secret encryption is disabled until it is provided.");
            return null;
        }
        byte[] raw = decodeKey(configuredKey.trim());
        if (raw.length != KEY_LENGTH_BYTES) {
            throw new IllegalStateException(
                    "SSO_MASTER_KEY must decode to exactly 32 bytes (AES-256); got " + raw.length + " bytes");
        }
        return raw;
    }

    private static byte[] decodeKey(String key) {
        try {
            return Base64.getDecoder().decode(key);
        } catch (IllegalArgumentException notBase64) {
            return key.getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * @return {@code true} when a valid master key is configured and encryption is usable.
     */
    public boolean isEnabled() {
        return masterKey != null;
    }

    /**
     * Encrypt a plaintext secret. Output is Base64(iv || ciphertext+tag).
     */
    public String encrypt(String plaintext) {
        if (plaintext == null) {
            return null;
        }
        requireEnabled();
        try {
            byte[] iv = new byte[IV_LENGTH];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec(), new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            byte[] cipherText = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            byte[] out = new byte[iv.length + cipherText.length];
            System.arraycopy(iv, 0, out, 0, iv.length);
            System.arraycopy(cipherText, 0, out, iv.length, cipherText.length);
            return Base64.getEncoder().encodeToString(out);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encrypt SSO secret", e);
        }
    }

    /**
     * Decrypt a blob produced by {@link #encrypt(String)}.
     */
    public String decrypt(String encoded) {
        if (encoded == null) {
            return null;
        }
        requireEnabled();
        try {
            byte[] blob = Base64.getDecoder().decode(encoded);
            if (blob.length <= IV_LENGTH) {
                throw new IllegalArgumentException("Ciphertext too short");
            }
            byte[] iv = Arrays.copyOfRange(blob, 0, IV_LENGTH);
            byte[] cipherText = Arrays.copyOfRange(blob, IV_LENGTH, blob.length);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, keySpec(), new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            byte[] plain = cipher.doFinal(cipherText);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to decrypt SSO secret", e);
        }
    }

    private void requireEnabled() {
        if (!isEnabled()) {
            throw new IllegalStateException(
                    "SSO_MASTER_KEY is not configured; cannot encrypt/decrypt SSO secrets");
        }
    }

    private SecretKeySpec keySpec() {
        return new SecretKeySpec(masterKey, KEY_ALGORITHM);
    }
}
