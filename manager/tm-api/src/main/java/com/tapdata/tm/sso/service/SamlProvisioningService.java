package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.user.entity.User;

import java.util.List;

/**
 * Shared provisioning helper for SAML M4: resolves (or auto-creates) roles by name and
 * creates pre-provisioned TapData users. Used by both batch import (admin-driven) and
 * JIT provisioning (first-login-driven).
 *
 * <p>Roles are only ever set at user-creation time here; there is no per-login role
 * re-sync, no SCIM, and no directory pull-back.
 */
public interface SamlProvisioningService {

    /** Marker written to {@code User.source} for SAML-provisioned accounts. */
    String SOURCE_SAML = "createSaml";

    /** Description stamped on roles auto-created by SAML provisioning. */
    String AUTO_ROLE_DESCRIPTION = "SAML 自动创建";

    /**
     * Return the id (hex) of an existing role matched by exact name, creating an empty
     * role (name + description only, no permissions) when none exists.
     *
     * @param roleName role name to resolve; blank names are rejected
     * @param actor    the acting admin/system user
     * @return the role id as a hex string
     */
    String resolveOrCreateRoleId(String roleName, UserDetail actor);

    /**
     * Create a single SAML-provisioned user. The account is active, email-verified and
     * password-less (the user authenticates through the IdP). Roles are resolved/created
     * by name and attached at creation time only.
     *
     * @param email       required, unique login email
     * @param username    optional display username (defaults to the email local-part)
     * @param roleNames   optional role names; missing roles are auto-created
     * @param actor       the acting admin/system user
     * @return the created {@link User}
     */
    User provisionUser(String email, String username, List<String> roleNames, UserDetail actor);
}
