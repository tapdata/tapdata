package com.tapdata.tm.proxyprocess.service.auth;

import com.tapdata.tm.accessToken.entity.AccessTokenEntity;
import com.tapdata.tm.accessToken.service.AccessTokenService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Authenticates an agent on its outbound calls to TM.
 *
 * <p>An agent authenticates with the access code it was deployed with
 * ({@code POST /api/users/generatetoken}) and then sends the resulting token as
 * {@code Authorization: Bearer <token>} on every later call, exactly as the API
 * Server does.
 *
 * <p>Before this existed an agent had no credential of its own: it announced an
 * {@code agentId} and TM handed it back a token it then also used to authorise
 * TM's calls <em>into</em> it. One value proved both directions, so a leak of
 * either was fatal to both.
 *
 * <p>Generic by design: any proxied service registered through
 * {@code /api/agent-process} authenticates the same way.
 */
@Service
@Slf4j
public class AgentTokenValidator {

    private final AccessTokenService accessTokenService;

    @Autowired
    public AgentTokenValidator(AccessTokenService accessTokenService) {
        this.accessTokenService = accessTokenService;
    }

    /**
     * Resolve the user behind an agent's bearer token.
     *
     * @param authorization raw {@code Authorization} header value, with or without {@code Bearer }
     * @return the owning user, or {@code null} when the token is missing or invalid
     */
    public ObjectId authenticate(String authorization) {
        String token = extractBearer(authorization);
        if (StringUtils.isBlank(token)) {
            return null;
        }
        /*
         * countAsActivity=false: an agent heartbeating every interval must not keep extending a
         * user's interactive session. Access-code tokens are absolute-lifetime anyway, but the
         * flag also keeps the write path out of the heartbeat.
         */
        AccessTokenEntity entity = accessTokenService.validateEntity(token, false);
        if (entity == null || entity.getUserId() == null) {
            log.debug("Agent bearer token rejected: no live access token entry");
            return null;
        }
        return entity.getUserId();
    }

    /** Strip an optional {@code Bearer } prefix; TM accepts both forms for compatibility. */
    public static String extractBearer(String authorization) {
        if (StringUtils.isBlank(authorization)) {
            return null;
        }
        String value = authorization.trim();
        if (value.regionMatches(true, 0, "Bearer ", 0, 7)) {
            value = value.substring(7).trim();
        }
        return StringUtils.isBlank(value) ? null : value;
    }
}
