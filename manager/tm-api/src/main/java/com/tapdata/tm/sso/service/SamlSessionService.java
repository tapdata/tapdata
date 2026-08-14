package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.entity.SsoSession;

/**
 * Terminates the server-side SSO sessions recorded at login (see {@link SsoSession}).
 * <p>
 * Because the {@code SsoSession} store is shared MongoDB, revocation performed by any
 * cluster node is immediately visible to all nodes (AC-048). Termination revokes the
 * TapData AccessToken issued for the session and deletes the session record so a
 * subsequent Single Logout is idempotent.
 */
public interface SamlSessionService {

    /**
     * Terminate the session identified by the AccessToken it issued (SP-Initiated SLO,
     * where the browser presents its {@code access_token}).
     *
     * @param accessTokenId the AccessToken id stored on the session.
     * @return the number of sessions terminated (0 if none matched).
     */
    long terminateByAccessToken(String accessTokenId);

    /**
     * Terminate the session(s) for the given NameID (IdP-Initiated SLO). When
     * {@code sessionIndex} is provided only the matching session is terminated;
     * otherwise all sessions for the NameID are terminated.
     *
     * @param nameId       the subject NameID (required).
     * @param sessionIndex the IdP SessionIndex, or {@code null}/blank for all.
     * @return the number of sessions terminated.
     */
    long terminate(String nameId, String sessionIndex);

    /**
     * Whether the session has passed the IdP-advertised {@code SessionNotOnOrAfter}
     * and is therefore already invalid (AC-049).
     */
    boolean isExpired(SsoSession session);
}
