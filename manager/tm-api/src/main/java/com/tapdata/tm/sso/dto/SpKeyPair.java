package com.tapdata.tm.sso.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A freshly generated SP signing/decryption key pair.
 * <p>
 * Both values are PEM encoded. The {@link #privateKeyPem} is only handed to the
 * caller so it can be encrypted-at-rest immediately; it is never returned to
 * clients afterwards (AC-053).
 */
@Data
@AllArgsConstructor
public class SpKeyPair {

    /** PEM-encoded PKCS#8 private key. */
    private String privateKeyPem;
    /** PEM-encoded self-signed X.509 certificate. */
    private String certificatePem;
}
