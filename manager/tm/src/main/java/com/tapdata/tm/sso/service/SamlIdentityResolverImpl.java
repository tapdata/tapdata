package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlAuthenticatedSubject;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.entity.SsoExternalIdentity;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Default {@link SamlIdentityResolver}. Resolves via an existing binding first, then
 * by email. JIT provisioning of new users is disabled by default (secure default);
 * when disabled and no user matches, login is refused.
 */
@Service
public class SamlIdentityResolverImpl implements SamlIdentityResolver {

    @Autowired
    private SamlConfigService samlConfigService;

    @Autowired
    private UserService userService;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public User resolve(SamlAuthenticatedSubject subject) {
        if (subject == null || StringUtils.isBlank(subject.getNameId())) {
            throw new SamlValidationException("No subject to resolve");
        }
        SamlConfig config = samlConfigService.getConfig();

        // 1. Existing binding wins.
        SsoExternalIdentity binding = findBinding(subject.getIdpEntityId(), subject.getNameId());
        if (binding != null && StringUtils.isNotBlank(binding.getTapdataUserId())) {
            User user = findUserById(binding.getTapdataUserId());
            if (user != null) {
                return ensureActive(user);
            }
        }

        // 2. Match by email (NameID or mapped email claim).
        String email = resolveEmail(subject, config);
        if (StringUtils.isBlank(email)) {
            throw new SamlValidationException("Unable to determine email from SAML subject");
        }
        User user = userService.findOneByEmail(email);
        if (user == null) {
            if (!config.isJitProvisioningEnabled()) {
                throw new SamlValidationException("No matching TapData user and JIT provisioning is disabled");
            }
            throw new SamlValidationException("JIT provisioning is enabled but not implemented in this milestone");
        }
        user = ensureActive(user);
        bind(subject, user);
        return user;
    }

    private String resolveEmail(SamlAuthenticatedSubject subject, SamlConfig config) {
        Map<String, List<String>> attributes = subject.getAttributes();
        if (StringUtils.isNotBlank(config.getClaimEmail()) && attributes != null) {
            List<String> values = attributes.get(config.getClaimEmail());
            if (values != null && !values.isEmpty() && StringUtils.isNotBlank(values.get(0))) {
                return values.get(0).trim();
            }
        }
        String nameId = subject.getNameId();
        return nameId == null ? null : nameId.trim();
    }

    private SsoExternalIdentity findBinding(String idpEntityId, String nameId) {
        Query query = Query.query(Criteria.where("idpEntityId").is(idpEntityId).and("nameId").is(nameId));
        return mongoTemplate.findOne(query, SsoExternalIdentity.class);
    }

    private void bind(SamlAuthenticatedSubject subject, User user) {
        SsoExternalIdentity identity = new SsoExternalIdentity();
        identity.setNameId(subject.getNameId());
        identity.setNameIdFormat(subject.getNameIdFormat());
        identity.setIdpEntityId(subject.getIdpEntityId());
        identity.setTapdataUserId(user.getId().toHexString());
        try {
            mongoTemplate.insert(identity);
        } catch (org.springframework.dao.DuplicateKeyException ignored) {
            // Concurrent first login created the binding; safe to ignore.
        }
    }

    private User findUserById(String userId) {
        if (!org.bson.types.ObjectId.isValid(userId)) {
            return null;
        }
        Query query = Query.query(Criteria.where("_id").is(new org.bson.types.ObjectId(userId)));
        return mongoTemplate.findOne(query, User.class);
    }

    private User ensureActive(User user) {
        if (user.getAccountStatus() == 0) {
            throw new SamlValidationException("User account is disabled");
        }
        if (user.getAccountStatus() == 2) {
            throw new SamlValidationException("User account is pending approval");
        }
        return user;
    }
}
