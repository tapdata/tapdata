package com.tapdata.tm.proxyprocess.service.auth;

import com.tapdata.tm.accessToken.dto.AccessTokenDto;
import com.tapdata.tm.accessToken.dto.AuthType;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.user.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Supplies the token TM presents when it calls <em>into</em> an agent.
 *
 * <p>An agent protects its own endpoints with the same {@code access_token} model
 * every other external caller uses, so TM needs a token of its own instead of
 * the registration token it used to mint for the agent and then reuse backwards.
 *
 * <p>Tokens are cached per agent for most of their lifetime. The lifetime is short (minutes),
 * so a cached token is cheap to replace and a stale one is never reused for long.
 */
@Service
@Slf4j
public class AgentAccessTokenIssuer {

    /** Refresh this far ahead of expiry so an in-flight request cannot straddle it. */
    static final long REFRESH_MARGIN_MILLIS = 60_000L;

    private final AccessTokenService accessTokenService;

    private final Map<String, CachedToken> cache = new ConcurrentHashMap<>();

    @Autowired
    public AgentAccessTokenIssuer(AccessTokenService accessTokenService) {
        this.accessTokenService = accessTokenService;
    }

    /**
     * Return a live access token owned by the agent's user, minting one when the cache is cold
     * or close to expiry.
     *
     * @throws IllegalStateException when the agent has no owning user, so nothing can be issued
     */
    public String tokenFor(AgentProcessEntity agent) {
        if (agent == null || agent.getUserId() == null) {
            throw new IllegalStateException(
                    "Agent has no owning user; cannot issue an access token for it");
        }
        String key = agent.getId() == null
                ? String.valueOf(agent.getAgentId()) : agent.getId().toHexString();
        CachedToken cached = cache.get(key);
        long now = System.currentTimeMillis();
        if (cached != null && cached.expiresAt - REFRESH_MARGIN_MILLIS > now) {
            return cached.token;
        }
        synchronized (cache.computeIfAbsent(key, ignored -> new CachedToken())) {
            CachedToken entry = cache.get(key);
            if (entry.token != null && entry.expiresAt - REFRESH_MARGIN_MILLIS > now) {
                return entry.token;
            }
            AccessTokenDto issued = issue(agent.getUserId());
            /*
             * The DTO reports ttl in seconds. Treat a missing or non-positive ttl as "do not
             * cache" rather than "never expires", so a misconfiguration degrades to re-issuing
             * instead of pinning a token that may already be dead.
             */
            long ttlSeconds = issued.getTtl() == null ? 0L : issued.getTtl();
            entry.token = issued.getId();
            entry.expiresAt = ttlSeconds > 0
                    ? now + ttlSeconds * 1000L : now;
            return entry.token;
        }
    }

    /** Drop a cached token, called after an agent answers 401 so the next call mints a new one. */
    public void invalidate(AgentProcessEntity agent) {
        if (agent == null) {
            return;
        }
        String key = agent.getId() == null
                ? String.valueOf(agent.getAgentId()) : agent.getId().toHexString();
        cache.remove(key);
    }

    private AccessTokenDto issue(String userId) {
        /*
         * AccessTokenService writes a token row keyed by the user rather than an OAuth2 client;
         * ACCESS_CODE is the closest auth type because this is a machine credential, and it keeps
         * the token on the absolute-lifetime path instead of the interactive sliding window.
         */
        /*
         * AgentProcessEntity.userId is the base entity's String form; the User entity keys by
         * ObjectId. Converting here keeps the caller's contract unchanged.
         */
        ObjectId ownerId = ObjectId.isValid(userId) ? new ObjectId(userId) : null;
        if (ownerId == null) {
            throw new IllegalStateException("Agent owner id is not a valid ObjectId: " + userId);
        }
        User owner = new User();
        owner.setId(ownerId);
        AccessTokenDto dto = accessTokenService.save(owner, AuthType.ACCESS_CODE.getValue());
        if (dto == null || StringUtils.isBlank(dto.getId())) {
            throw new IllegalStateException("Unable to issue an access token for the agent owner");
        }
        log.debug("Issued access token for agent owner {} (ttl={}s)", ownerId, dto.getTtl());
        return dto;
    }

    private static final class CachedToken {
        private String token;
        private long expiresAt;
    }
}
