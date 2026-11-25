package com.tapdata.tm.proxyprocess.service.auth;

import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The standard credential chain for a proxied agent (an "AgentProcess").
 *
 * <p>Every proxied service TM manages - the Db2 for z CDC Reader today, any
 * later proxy service - needs the same four things, and this is where they
 * live together so a new agent type reuses them instead of re-deriving a
 * scheme:
 *
 * <ol>
 *   <li><b>Inbound from the agent.</b> The agent authenticates with the access
 *       code it was deployed with and presents the resulting bearer token on
 *       every call to TM. {@link #authenticateAgent} resolves it.</li>
 *   <li><b>The agent's own client.</b> The agent generates a client id/secret,
 *       reports them, and TM records and publishes them so external callers can
 *       exchange them for an access token at the agent's token URL.
 *       {@link #acceptAgentCredentials} records them; {@link #revokeAgent}
 *       removes the client with the registration.</li>
 *   <li><b>Inbound from a caller.</b> Callers (TM's own command forwarding, and
 *       any connector) obtain an access token from the token URL.
 *       {@link #tokenForAgent} supplies the one TM itself presents.</li>
 *   <li><b>At-rest handling.</b> The secret is stored hashed for change
 *       detection and encrypted for the deliberate reveal an operator uses when
 *       configuring a caller. {@link #revealSecret} reads it back.</li>
 * </ol>
 *
 * <p>The credential belongs to the agent, not to TM. See
 * {@link AgentOAuth2ClientRegistrar} for why that direction matters.
 */
@Service
@Slf4j
public class AgentCredentialService {

    /** Role an agent's access token needs for the read-only endpoints. */
    public static final String DEFAULT_SCOPE_ROLE = "viewer";

    private final AgentTokenValidator tokenValidator;
    private final AgentOAuth2ClientRegistrar clientRegistrar;
    private final AgentSecretCipher secretCipher;
    private final AgentAccessTokenIssuer tokenIssuer;

    @Autowired
    public AgentCredentialService(AgentTokenValidator tokenValidator,
                                  AgentOAuth2ClientRegistrar clientRegistrar,
                                  AgentSecretCipher secretCipher,
                                  AgentAccessTokenIssuer tokenIssuer) {
        this.tokenValidator = tokenValidator;
        this.clientRegistrar = clientRegistrar;
        this.secretCipher = secretCipher;
        this.tokenIssuer = tokenIssuer;
    }

    /**
     * Resolve the owning user behind an agent's inbound bearer token.
     *
     * @return the user, or {@code null} when the token is missing or invalid
     */
    public ObjectId authenticateAgent(String authorization) {
        return tokenValidator.authenticate(authorization);
    }

    /**
     * Record the client credentials an agent reported about itself.
     *
     * <p>Validates the three values as a group, keeps a hash for change detection
     * and an encrypted copy for the reveal endpoint, and publishes the client to
     * the authorization server when the secret is new.
     *
     * @param agent       the registration being updated
     * @param clientId    24-hex client id the agent generated
     * @param secret      plaintext secret the agent generated
     * @param scopeRole   role whose id becomes the client scope, or {@code null} for the default
     * @return true when the stored secret changed and the client was re-published
     * @throws IllegalArgumentException when the credentials are incomplete or malformed
     */
    public boolean acceptAgentCredentials(AgentProcessEntity agent, String clientId,
                                          String secret, String scopeRole) {
        if (!AgentCredentialPolicy.isValidSecret(secret)) {
            throw new IllegalArgumentException("Agent client secret is invalid");
        }
        if (!AgentCredentialPolicy.isObjectIdHex(clientId)) {
            throw new IllegalArgumentException(
                    "Agent client id must be a 24 character hex string");
        }
        boolean changed = agent.getClientSecretHash() == null
                || !secretCipher.matches(secret, agent.getClientSecretHash());
        agent.setClientId(clientId);
        if (changed) {
            agent.setClientSecretHash(secretCipher.hash(secret));
            agent.setClientSecretCipher(secretCipher.encrypt(secret));
            publishClient(clientId, secret, scopeRole);
        }
        return changed;
    }

    /**
     * Publish the agent's client so {@code /oauth/token} accepts it.
     *
     * <p>Failure must not fail the registration: an agent that cannot be
     * published can still heartbeat, and the next register retries.
     */
    public void publishClient(String clientId, String secret, String scopeRole) {
        String role = scopeRole == null ? DEFAULT_SCOPE_ROLE : scopeRole;
        List<ObjectId> scopes = new ArrayList<>();
        clientRegistrar.findRoleId(role).ifPresent(scopes::add);
        if (scopes.isEmpty()) {
            log.warn("Role '{}' not found; agent client {} will only carry $everyone", role, clientId);
        }
        try {
            clientRegistrar.register(clientId, secret, scopes);
        } catch (RuntimeException e) {
            log.warn("Unable to publish agent OAuth2 client {}: {}", clientId, e.getMessage());
        }
    }

    /** Remove the OAuth2 client along with the registration. */
    public void revokeAgent(String clientId) {
        if (clientId == null || clientId.isEmpty()) {
            return;
        }
        try {
            clientRegistrar.unregister(clientId);
        } catch (RuntimeException e) {
            log.warn("Unable to revoke agent OAuth2 client {}: {}", clientId, e.getMessage());
        }
    }

    /** Token TM presents when calling into the agent. */
    public String tokenForAgent(AgentProcessEntity agent) {
        return tokenIssuer.tokenFor(agent);
    }

    /** Drop a cached token after the agent rejected it. */
    public void invalidateAgentToken(AgentProcessEntity agent) {
        tokenIssuer.invalidate(agent);
    }

    /**
     * Read back the secret an agent reported.
     *
     * @throws IllegalStateException when the stored value cannot be decrypted,
     *     so the operator is told to read it off the agent rather than being
     *     handed a wrong value
     */
    public String revealSecret(AgentProcessEntity agent) {
        String secret = secretCipher.decrypt(agent.getClientSecretCipher());
        if (secret == null) {
            throw new IllegalStateException(
                    "Stored client secret cannot be decrypted; read it from the agent configuration");
        }
        return secret;
    }

    /** Whether this client id is already owned by an agent. */
    public boolean isClientIdInUse(String clientId) {
        return clientRegistrar.exists(clientId);
    }

    /** Resolve a role name to its id, for callers that need a different scope. */
    public Optional<ObjectId> roleId(String roleName) {
        return clientRegistrar.findRoleId(roleName);
    }
}
