package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
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

import java.util.ArrayList;
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
    private SamlProvisioningService samlProvisioningService;

    @Autowired
    private MongoTemplate mongoTemplate;

    /** System actor used when JIT-creating a user during login (no interactive admin). */
    private static final String SYSTEM_ACTOR = "admin@admin.com";

    @Override
    public User resolve(SamlAuthenticatedSubject subject) {
        if (subject == null || StringUtils.isBlank(subject.getNameId())) {
            throw new SamlValidationException("No subject to resolve");
        }
        SamlConfig config = samlConfigService.getConfig();

        // 1. Existing binding wins. If a binding exists but the bound user is gone
        //    (deleted/frozen by an admin), refuse login with a clear reason rather
        //    than silently falling through to an email re-match.
        SsoExternalIdentity binding = findBinding(subject.getIdpEntityId(), subject.getNameId());
        if (binding != null && StringUtils.isNotBlank(binding.getTapdataUserId())) {
            User user = findUserById(binding.getTapdataUserId());
            if (user == null) {
                throw new SamlLoginException(SamlLoginError.USER_NOT_FOUND,
                        "Bound TapData user no longer exists");
            }
            return ensureActive(user);
        }

        // 2. Match by email (NameID or mapped email claim).
        String email = resolveEmail(subject, config);
        if (StringUtils.isBlank(email)) {
            throw new SamlValidationException("Unable to determine email from SAML subject");
        }
        User user = findUserByEmail(email);
        if (user == null) {
            if (!config.isJitProvisioningEnabled()) {
                throw new SamlLoginException(SamlLoginError.USER_NOT_FOUND,
                        "No matching TapData user and JIT provisioning is disabled");
            }
            // 3. JIT provisioning: create the user (roles resolved from claimGroups, if any),
            //    then bind. Roles are set only here, at first creation.
            user = provisionUser(subject, config, email);
        }
        user = ensureActive(user);
        bind(subject, user);
        return user;
    }

    private User provisionUser(SamlAuthenticatedSubject subject, SamlConfig config, String email) {
        UserDetail actor = userService.loadUserByUsername(SYSTEM_ACTOR);
        String username = resolveUsername(subject, config);
        List<String> roleNames = resolveRoleNames(subject, config);
        User created = samlProvisioningService.provisionUser(email, username, roleNames, actor);
        if (created == null) {
            throw new SamlValidationException("Failed to provision user for SAML login");
        }
        return created;
    }

    private String resolveUsername(SamlAuthenticatedSubject subject, SamlConfig config) {
        Map<String, List<String>> attributes = subject.getAttributes();
        if (StringUtils.isNotBlank(config.getClaimUsername()) && attributes != null) {
            List<String> values = attributes.get(config.getClaimUsername());
            if (values != null && !values.isEmpty() && StringUtils.isNotBlank(values.get(0))) {
                return values.get(0).trim();
            }
        }
        return null;
    }

    private List<String> resolveRoleNames(SamlAuthenticatedSubject subject, SamlConfig config) {
        List<String> roleNames = new ArrayList<>();
        Map<String, List<String>> attributes = subject.getAttributes();
        if (StringUtils.isNotBlank(config.getClaimGroups()) && attributes != null) {
            List<String> values = attributes.get(config.getClaimGroups());
            if (values != null) {
                for (String value : values) {
                    if (StringUtils.isNotBlank(value)) {
                        roleNames.add(value.trim());
                    }
                }
            }
        }
        return roleNames;
    }

    private User findUserByEmail(String email) {
        Query query = Query.query(Criteria.where("email").is(email)
                .orOperator(Criteria.where("isDeleted").is(false), Criteria.where("isDeleted").exists(false)));
        return mongoTemplate.findOne(query, User.class);
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
            throw new SamlLoginException(SamlLoginError.USER_DISABLED, "User account is disabled");
        }
        if (user.getAccountStatus() == 2) {
            throw new SamlLoginException(SamlLoginError.USER_PENDING, "User account is pending approval");
        }
        return user;
    }
}
