package com.tapdata.tm.sso.service;

import java.util.Date;

/**
 * Cluster-wide, one-time consumption guard for SAML message ids (Response and
 * Assertion ids). Backed by a MongoDB collection with a unique index so that
 * concurrent nodes cannot both accept the same assertion.
 */
public interface SamlReplayCacheService {

    /**
     * Atomically record the first use of {@code recordId} for the given {@code type}.
     *
     * @param type     the id category (e.g. "assertion" / "response").
     * @param recordId the SAML message/assertion id.
     * @return {@code true} if this is the first time the id is seen (accept the
     * message); {@code false} if it has already been consumed (replay -> reject).
     */
    boolean recordIfFirstUse(String type, String recordId);

    /**
     * Atomically record a message id until the supplied absolute expiry time.
     */
    default boolean recordIfFirstUse(String type, String recordId, Date expiresAt) {
        return recordIfFirstUse(type, recordId);
    }
}
