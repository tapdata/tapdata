package com.tapdata.tm.sso.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SsoSecretCipherTest {

    /** A valid 32-byte AES-256 key, Base64 encoded. */
    private static final String KEY_32 = Base64.getEncoder()
            .encodeToString("0123456789abcdef0123456789abcdef".getBytes());

    private SsoSecretCipher newCipher() {
        return new SsoSecretCipher(KEY_32);
    }

    @Nested
    @DisplayName("round-trip")
    class RoundTrip {
        @Test
        @DisplayName("encrypt then decrypt returns original plaintext")
        void roundTrip() {
            SsoSecretCipher cipher = newCipher();
            String secret = "-----BEGIN PRIVATE KEY-----\nMIIB...\n-----END PRIVATE KEY-----";

            String encrypted = cipher.encrypt(secret);
            assertNotEquals(secret, encrypted);
            assertEquals(secret, cipher.decrypt(encrypted));
        }

        @Test
        @DisplayName("same plaintext yields different ciphertext (random IV)")
        void randomIv() {
            SsoSecretCipher cipher = newCipher();
            String secret = "same-secret-value";

            String a = cipher.encrypt(secret);
            String b = cipher.encrypt(secret);
            assertNotEquals(a, b);
            assertEquals(secret, cipher.decrypt(a));
            assertEquals(secret, cipher.decrypt(b));
        }

        @Test
        @DisplayName("null in yields null out")
        void nullHandling() {
            SsoSecretCipher cipher = newCipher();
            assertNull(cipher.encrypt(null));
            assertNull(cipher.decrypt(null));
        }
    }

    @Nested
    @DisplayName("tamper detection")
    class Tamper {
        @Test
        @DisplayName("modifying ciphertext fails GCM authentication on decrypt")
        void tamperedCiphertext() {
            SsoSecretCipher cipher = newCipher();
            String encrypted = cipher.encrypt("sensitive");

            byte[] blob = Base64.getDecoder().decode(encrypted);
            blob[blob.length - 1] ^= 0x01; // flip a bit in the tag/ciphertext
            String tampered = Base64.getEncoder().encodeToString(blob);

            assertThrows(IllegalStateException.class, () -> cipher.decrypt(tampered));
        }

        @Test
        @DisplayName("ciphertext shorter than IV is rejected")
        void tooShort() {
            SsoSecretCipher cipher = newCipher();
            String tooShort = Base64.getEncoder().encodeToString(new byte[4]);
            assertThrows(IllegalStateException.class, () -> cipher.decrypt(tooShort));
        }

        @Test
        @DisplayName("ciphertext encrypted with a different key cannot be decrypted")
        void wrongKey() {
            String encrypted = newCipher().encrypt("secret");
            String otherKey = Base64.getEncoder()
                    .encodeToString("ffffffffffffffffffffffffffffffff".getBytes());
            SsoSecretCipher other = new SsoSecretCipher(otherKey);
            assertThrows(IllegalStateException.class, () -> other.decrypt(encrypted));
        }
    }

    @Nested
    @DisplayName("key configuration")
    class KeyConfig {
        @Test
        @DisplayName("blank key disables encryption")
        void blankKeyDisabled() {
            SsoSecretCipher cipher = new SsoSecretCipher("");
            assertFalse(cipher.isEnabled());
            assertThrows(IllegalStateException.class, () -> cipher.encrypt("x"));
            assertThrows(IllegalStateException.class, () -> cipher.decrypt("x"));
        }

        @Test
        @DisplayName("valid 32-byte key enables encryption")
        void validKeyEnabled() {
            assertTrue(newCipher().isEnabled());
        }

        @Test
        @DisplayName("key of wrong length is rejected at construction")
        void wrongLengthKey() {
            String shortKey = Base64.getEncoder().encodeToString("too-short".getBytes());
            assertThrows(IllegalStateException.class, () -> new SsoSecretCipher(shortKey));
        }

        @Test
        @DisplayName("raw (non-Base64) 32-byte key is accepted as UTF-8 bytes")
        void rawKeyAccepted() {
            // Contains '-' which is not in the standard Base64 alphabet, so it is
            // treated as raw UTF-8 bytes; exactly 32 bytes long.
            SsoSecretCipher cipher = new SsoSecretCipher("raw-master-key-0123456789-abcdef");
            assertTrue(cipher.isEnabled());
            assertEquals("v", cipher.decrypt(cipher.encrypt("v")));
        }
    }
}
