package com.tapdata.tm.proxyprocess.constant;

/**
 * Constants of the agent credential chain.
 *
 * <p>An <em>agent</em> is any proxied process TM reaches through
 * {@code /api/agent-process}: the Db2 for z CDC Reader today, and whatever proxy
 * service is added next. The chain is generic on purpose, so a new agent type
 * reuses it instead of re-deriving its own scheme.
 *
 * <p>Kept inside {@code proxyprocess} deliberately: an agent issuing its own
 * OAuth2 client is an agent-process concern and must not leak into the general
 * OAuth2 surface ({@code Application} console, API Server client management,
 * SAML, IDaaS).
 *
 * @see com.tapdata.tm.proxyprocess.service.auth.AgentOAuth2ClientRegistrar
 */
public final class AgentCredentialConstants {
    /**
     * Request header carrying an agent's own access token. The agent is both the
     * subject of the token (it authenticated with its access code) and the actor
     * reporting its client credentials, so one header serves both.
     */
    public static final String AGENT_TOKEN_HEADER = "Authorization";

    /** Mongo collection backing {@code RegisteredClientRepository}. */
    public static final String OAUTH2_CLIENT_COLLECTION = "Application";

    /**
     * Marks an {@code Application} row as owned by an agent rather than a
     * console user. The agent name is appended, so rows stay distinguishable
     * per agent type in the OAuth2 collection.
     */
    public static final String CLIENT_NAME_PREFIX = "AgentProcess ";

    /**
     * Access-token lifetime for agent clients. Short on purpose: the token is
     * self-contained, so it cannot be revoked, and the caller refreshes it on
     * expiry.
     */
    public static final java.time.Duration ACCESS_TOKEN_TTL = java.time.Duration.ofMinutes(30);

    private AgentCredentialConstants() {
    }
}
