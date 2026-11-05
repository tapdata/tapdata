package com.tapdata.tm.sso.service;

import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.sso.entity.SsoSession;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Date;
import java.util.List;

/**
 * MongoDB-backed {@link SamlSessionService}. Looks up {@link SsoSession} records on
 * their indexed fields, revokes the associated TapData AccessToken and deletes the
 * session record. All state lives in shared MongoDB so a logout on any node revokes
 * the session cluster-wide (AC-048).
 */
@Slf4j
@Service
public class SamlSessionServiceImpl implements SamlSessionService {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private AccessTokenService accessTokenService;

    @Override
    public long terminateByAccessToken(String accessTokenId) {
        if (StringUtils.isBlank(accessTokenId)) {
            return 0;
        }
        SsoSession session = mongoTemplate.findOne(
                Query.query(Criteria.where("accessTokenId").is(accessTokenId)), SsoSession.class);
        // Always revoke the token even if no session record exists, so SP-Initiated
        // logout is effective for locally-issued tokens.
        long revoked = revokeToken(accessTokenId);
        if (session != null) {
            deleteSession(session);
        }
        return session != null ? 1 : (revoked > 0 ? 1 : 0);
    }

    @Override
    public long terminate(String nameId, String sessionIndex) {
        if (StringUtils.isBlank(nameId)) {
            return 0;
        }
        Criteria criteria = Criteria.where("nameId").is(nameId);
        if (StringUtils.isNotBlank(sessionIndex)) {
            criteria = criteria.and("sessionIndex").is(sessionIndex);
        }
        List<SsoSession> sessions = mongoTemplate.find(Query.query(criteria), SsoSession.class);
        long terminated = 0;
        for (SsoSession session : sessions) {
            revokeToken(session.getAccessTokenId());
            deleteSession(session);
            terminated++;
        }
        return terminated;
    }

    @Override
    public boolean isExpired(SsoSession session) {
        if (session == null) {
            return true;
        }
        Date notOnOrAfter = session.getSessionNotOnOrAfter();
        return notOnOrAfter != null && !new Date().before(notOnOrAfter);
    }

    /** Revoke expired IdP sessions even when no subsequent API request touches them. */
    @Scheduled(fixedDelay = 300_000L)
    public void cleanupExpiredSessions() {
        List<SsoSession> expired = mongoTemplate.find(
                Query.query(Criteria.where("sessionNotOnOrAfter").lte(new Date())), SsoSession.class);
        for (SsoSession session : expired) {
            revokeToken(session.getAccessTokenId());
            deleteSession(session);
        }
    }

    private long revokeToken(String accessTokenId) {
        if (StringUtils.isBlank(accessTokenId)) {
            return 0;
        }
        try {
            return accessTokenService.removeAccessToken(accessTokenId, null);
        } catch (Exception e) {
            log.warn("Failed to revoke access token during SLO: {}", e.getMessage());
            return 0;
        }
    }

    private void deleteSession(SsoSession session) {
        try {
            mongoTemplate.remove(Query.query(Criteria.where("accessTokenId").is(session.getAccessTokenId())),
                    SsoSession.class);
        } catch (Exception e) {
            log.warn("Failed to delete SsoSession during SLO: {}", e.getMessage());
        }
    }
}
