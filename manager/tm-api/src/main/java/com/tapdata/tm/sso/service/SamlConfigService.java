package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.dto.SamlConfigForm;
import com.tapdata.tm.sso.dto.SamlConfigView;

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

    /**
     * Read the current configuration as a masked view safe to return to clients.
     * <p>
     * The SP private key value is never included; only a boolean flag indicates
     * whether one is stored (AC-053).
     */
    SamlConfigView getMaskedConfig();

    /**
     * Persist the supplied configuration to Settings.
     * <p>
     * The SP private key, when present, is encrypted at rest via the SSO cipher
     * before storage. A blank {@code spPrivateKey} preserves the existing stored
     * key (so clients that can never read the key back can still save other fields).
     *
     * @param form the configuration to save (must not be {@code null}).
     */
    void saveConfig(SamlConfigForm form);
}
