package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlConfigForm;
import com.tapdata.tm.sso.dto.SamlValidationResult;

/**
 * Validates a SAML configuration before it is saved/enabled.
 */
public interface SamlValidationService {

    /**
     * Validate the supplied configuration.
     * <p>
     * Checks required fields (SP entity ID / ACS URL, IdP entity ID / SSO URL,
     * IdP signing certificate), that supplied certificates are parseable X.509,
     * and that the configured signature algorithm is not a weak/deprecated one
     * (e.g. SHA-1 / MD5). When {@code enabled} is false only structural checks
     * that do not block saving a draft are applied.
     *
     * @param form the configuration to validate.
     * @return the validation result (errors + warnings).
     */
    SamlValidationResult validate(SamlConfigForm form);

}
