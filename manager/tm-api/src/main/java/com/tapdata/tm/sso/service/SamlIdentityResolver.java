package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlAuthenticatedSubject;
import com.tapdata.tm.user.entity.User;

/**
 * Resolves a validated SAML subject to a TapData {@link User}.
 * <p>
 * Resolution order: existing {@code SsoExternalIdentity} binding, then match by
 * email. Just-In-Time provisioning of a brand-new user is only performed when
 * explicitly enabled in the configuration (off by default, security closed).
 */
public interface SamlIdentityResolver {

    /**
     * Resolve (and bind) the TapData user for the given validated subject.
     *
     * @param subject the trusted subject extracted from a validated assertion.
     * @return the resolved, active user.
     * @throws com.tapdata.tm.sso.service.SamlValidationException if no user can be
     *         resolved (and JIT is disabled), or the user is disabled.
     */
    User resolve(SamlAuthenticatedSubject subject);
}
