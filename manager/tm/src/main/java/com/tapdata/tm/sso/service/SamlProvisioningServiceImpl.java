package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import com.tapdata.tm.user.dto.CreateUserRequest;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.user.service.UserService;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Default {@link SamlProvisioningService}. Roles are resolved by exact name and created
 * empty (no permissions) when missing; users are created active, email-verified and
 * password-less. Roles are attached at creation time only.
 */
@Service
public class SamlProvisioningServiceImpl implements SamlProvisioningService {

    @Autowired
    private RoleService roleService;

    @Autowired
    private UserService userService;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public String resolveOrCreateRoleId(String roleName, UserDetail actor) {
        if (StringUtils.isBlank(roleName)) {
            throw new IllegalArgumentException("roleName must not be blank");
        }
        String name = roleName.trim();
        RoleDto existing = roleService.findOne(Query.query(Criteria.where("name").is(name)));
        if (existing != null && existing.getId() != null) {
            return existing.getId().toHexString();
        }
        RoleDto dto = new RoleDto();
        dto.setName(name);
        dto.setDescription(AUTO_ROLE_DESCRIPTION);
        dto.setRegisterUserDefault(false);
        RoleDto saved = roleService.save(dto, actor);
        return saved.getId().toHexString();
    }

    @Override
    public User provisionUser(String email, String username, List<String> roleNames, UserDetail actor) {
        if (StringUtils.isBlank(email)) {
            throw new IllegalArgumentException("email must not be blank");
        }
        String normalizedEmail = email.trim().toLowerCase(Locale.ROOT);

        CreateUserRequest request = new CreateUserRequest();
        request.setEmail(normalizedEmail);
        request.setUsername(StringUtils.isNotBlank(username) ? username.trim() : localPart(normalizedEmail));
        request.setSource(SOURCE_SAML);
        request.setAccountStatus(1);
        request.setEmailVerified(true);
        request.setRoleusers(resolveRoleIds(roleNames, actor));

        userService.save(request, actor);
        return findByEmail(normalizedEmail);
    }

    private List<Object> resolveRoleIds(List<String> roleNames, UserDetail actor) {
        List<Object> roleIds = new ArrayList<>();
        if (roleNames != null) {
            for (String roleName : roleNames) {
                if (StringUtils.isNotBlank(roleName)) {
                    roleIds.add(resolveOrCreateRoleId(roleName, actor));
                }
            }
        }
        if (roleIds.isEmpty()) {
            List<RoleDto> defaultRoles = roleService.findAll(
                    Query.query(Criteria.where("register_user_default").is(true)));
            if (defaultRoles != null) {
                defaultRoles.stream()
                        .map(RoleDto::getId)
                        .filter(java.util.Objects::nonNull)
                        .map(ObjectId::toHexString)
                        .forEach(roleIds::add);
            }
        }
        return roleIds;
    }

    private User findByEmail(String email) {
        Query query = Query.query(Criteria.where("email").is(email)
                .orOperator(Criteria.where("isDeleted").is(false), Criteria.where("isDeleted").exists(false)));
        return mongoTemplate.findOne(query, User.class);
    }

    private String localPart(String email) {
        int at = email.indexOf('@');
        return at > 0 ? email.substring(0, at) : email;
    }
}
