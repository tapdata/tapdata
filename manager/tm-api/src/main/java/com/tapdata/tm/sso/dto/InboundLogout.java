package com.tapdata.tm.sso.dto;

import lombok.Data;

/**
 * The trusted result of validating an inbound IdP-Initiated {@code LogoutRequest}: the
 * subject NameID plus the correlation data needed to terminate the matching
 * {@code SsoSession}(s) and to build the {@code LogoutResponse} back to the IdP.
 */
@Data
public class InboundLogout {

    /** The validated LogoutRequest message id (echoed as InResponseTo in the LogoutResponse). */
    private String requestId;

    /** The subject NameID whose sessions must be terminated. */
    private String nameId;

    /**
     * The optional SessionIndex to target a single session; when blank all sessions for
     * the NameID are terminated.
     */
    private String sessionIndex;

    /** The IdP entity ID (Issuer) that sent the LogoutRequest. */
    private String issuer;
}
