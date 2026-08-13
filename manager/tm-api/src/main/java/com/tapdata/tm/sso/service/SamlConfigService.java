package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlConfig;

/**
 * Reads and writes the single, IdP-agnostic SAML SSO configuration that is stored
 * in the system Settings collection (mirroring how LDAP configuration is handled).
 */
public interface SamlConfigService {

    /**
     * Assemble the current SAML configuration from Settings.
     * <p>
     * The returned config has the SP private key decrypted for in-memory use. It is
     * the caller's responsibility never to echo the private key back to clients.
     *
     * @return the assembled configuration (never {@code null}; {@code enabled=false}
     * when SAML is not configured/enabled).
     */
    SamlConfig getConfig();

    /**
     * @return {@code true} when SAML login is enabled in Settings.
     */
    boolean isEnabled();
}
