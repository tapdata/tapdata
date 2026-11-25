package com.tapdata.tm.proxyprocess.service.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The at-rest cipher for agent client secrets.
 *
 * <p>The default key matters more than it looks: it is what a deployment with no
 * configured key uses, so if it is not a length AES accepts every reader
 * registration fails with "Invalid AES key length" - and it does so at the
 * registration endpoint, long after startup reported success.
 */
class AgentSecretCipherTest {

    private static final String SECRET = "0123456789abcdef0123456789abcdef"
            + "0123456789abcdef0123456789abcdef";

    @Test
    void defaultKeyEncryptsAndDecrypts() {
        /*
         * No configured key: the built-in default is used.  This is the case
         * that used to throw InvalidKeyException because the default was 31
         * bytes rather than a length AES accepts.
         */
        AgentSecretCipher cipher = new AgentSecretCipher("");

        String stored = cipher.encrypt(SECRET);

        assertNotNull(stored);
        assertTrue(stored.startsWith("gcm:"));
        assertEquals(SECRET, cipher.decrypt(stored));
    }

    @Test
    void defaultKeyRoundTripsRepeatedly() {
        AgentSecretCipher cipher = new AgentSecretCipher(null);

        for (int attempt = 0; attempt < 3; attempt++) {
            assertEquals(SECRET, cipher.decrypt(cipher.encrypt(SECRET)));
        }
    }

    @Test
    void configuredPlainKeyIsAccepted() {
        /* 32 characters, used verbatim. */
        AgentSecretCipher cipher = new AgentSecretCipher("0123456789abcdef0123456789abcdef");

        assertEquals(SECRET, cipher.decrypt(cipher.encrypt(SECRET)));
    }

    @Test
    void configuredBase64KeyIsAccepted() {
        /* 32 bytes once decoded. */
        String base64 = java.util.Base64.getEncoder().encodeToString(
                "abcdefghijklmnopqrstuvwxyz012345".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        AgentSecretCipher cipher = new AgentSecretCipher(base64);

        assertEquals(SECRET, cipher.decrypt(cipher.encrypt(SECRET)));
    }

    @Test
    void wrongLengthConfiguredKeyFallsBackToTheDefault() {
        /*
         * A configured key that is neither Base64 nor 16/24/32 bytes is not
         * silently used: it falls back to the built-in default, which is a
         * length AES accepts, so the bean still works.
         */
        AgentSecretCipher cipher = new AgentSecretCipher("too-short");

        assertEquals(SECRET, cipher.decrypt(cipher.encrypt(SECRET)));
    }

    @Test
    void aSecretChangedByTheAgentIsDetected() {
        AgentSecretCipher cipher = new AgentSecretCipher("");

        String storedHash = cipher.hash(SECRET);

        assertTrue(cipher.matches(SECRET, storedHash));
        assertTrue(!cipher.matches(SECRET + "x", storedHash));
    }

    @Test
    void tamperedCiphertextDoesNotDecrypt() {
        AgentSecretCipher cipher = new AgentSecretCipher("");
        String stored = cipher.encrypt(SECRET);

        /* AES-GCM authenticates, so a flipped character must not yield a secret. */
        char last = stored.charAt(stored.length() - 1);
        String tampered = stored.substring(0, stored.length() - 1)
                + (last == 'A' ? 'B' : 'A');

        assertNull(cipher.decrypt(tampered));
    }

    @Test
    void nullSecretIsHandled() {
        AgentSecretCipher cipher = new AgentSecretCipher("");

        assertNull(cipher.encrypt(null));
        assertNull(cipher.hash(null));
    }
}
