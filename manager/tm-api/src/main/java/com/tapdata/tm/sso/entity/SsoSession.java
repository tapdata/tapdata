package com.tapdata.tm.sso.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * A server-side record of an active SSO session, created when a SAML login
 * succeeds at the ACS endpoint. It correlates the IdP session (NameID +
 * SessionIndex) with the TapData AccessToken so that Single Logout (SP- or
 * IdP-initiated, M5) can terminate the exact token that the login issued, and so
 * session revocation is consistent across all cluster nodes.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Document("SsoSession")
public class SsoSession extends BaseEntity {

    /** SAML NameID of the authenticated subject. */
    @Indexed
    private String nameId;

    /** SAML SessionIndex issued by the IdP (used to target IdP-initiated SLO). */
    @Indexed
    private String sessionIndex;

    /** Entity ID of the IdP that authenticated the subject. */
    private String idpEntityId;

    /** The AccessToken id issued for this session (terminated on logout). */
    @Indexed
    private String accessTokenId;

    /** The resolved TapData user id (hex string). */
    private String userId;

    /** When the session was created. */
    private Date createdAt;

    /**
     * Optional session expiry advertised by the IdP (SessionNotOnOrAfter). When
     * set and passed, the session is considered invalid (AC-049).
     */
    private Date sessionNotOnOrAfter;
}
