package com.tapdata.tm.proxyprocess.service.auth;

import java.util.regex.Pattern;

/**
 * Validation rules for the credentials an agent reports about itself.
 *
 * <p>Kept separate from the registration service so the rules can be unit tested without
 * Mongo, and so every rule has one documented reason next to it.
 */
public final class AgentCredentialPolicy {

    /** {@code ObjectId} is a 24 character lower/upper hex string. */
    private static final Pattern OBJECT_ID_HEX = Pattern.compile("^[0-9a-fA-F]{24}$");

    private AgentCredentialPolicy() {
    }

    /**
     * {@code RegisteredClient} ids are stored as {@code ObjectId}, so only the
     * 24 character hex form is accepted. An agent is free to derive its client
     * id however it likes (the Db2 Reader drops the dashes from its agent id and
     * keeps 24 characters) as long as the result is unique and this shape.
     */
    public static boolean isObjectIdHex(String value) {
        return value != null && OBJECT_ID_HEX.matcher(value).matches();
    }

    /**
     * A secret must be long enough to be worth hashing and must not carry whitespace, which would
     * make the form-encoded token request ambiguous.
     */
    public static boolean isValidSecret(String secret) {
        return secret != null && !secret.isBlank() && secret.length() >= 16
                && secret.chars().noneMatch(Character::isWhitespace);
    }
}
