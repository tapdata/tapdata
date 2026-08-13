package com.tapdata.tm.sso.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.security.SsoSecretCipher;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Settings-backed implementation of {@link SamlConfigService}. Assembles a generic,
 * IdP-agnostic {@link SamlConfig} from the {@code saml.*} keys under the SAML category
 * and decrypts the SP private key via {@link SsoSecretCipher}.
 */
@Service
public class SamlConfigServiceImpl implements SamlConfigService {

    @Autowired
    private SettingsService settingsService;

    @Autowired
    private SsoSecretCipher ssoSecretCipher;

    @Override
    public SamlConfig getConfig() {
        SamlConfig.SamlConfigBuilder builder = SamlConfig.builder()
                .enabled(readBoolean(KeyEnum.SAML_LOGIN_ENABLE, false))
                .spEntityId(readString(KeyEnum.SAML_SP_ENTITY_ID))
                .spAcsUrl(readString(KeyEnum.SAML_SP_ACS_URL))
                .spPrivateKey(decryptPrivateKey(readString(KeyEnum.SAML_SP_PRIVATE_KEY)))
                .spCertificate(readString(KeyEnum.SAML_SP_CERTIFICATE))
                .idpEntityId(readString(KeyEnum.SAML_IDP_ENTITY_ID))
                .idpSsoUrl(readString(KeyEnum.SAML_IDP_SSO_URL))
                .idpSloUrl(readString(KeyEnum.SAML_IDP_SLO_URL))
                .idpSigningCertificate(readString(KeyEnum.SAML_IDP_SIGNING_CERTIFICATE))
                .wantAssertionsSigned(readBoolean(KeyEnum.SAML_WANT_ASSERTIONS_SIGNED, true))
                .signAuthnRequest(readBoolean(KeyEnum.SAML_SIGN_AUTHN_REQUEST, false))
                .clockSkewSeconds(readInt(KeyEnum.SAML_CLOCK_SKEW_SECONDS, 120))
                .claimUsername(readString(KeyEnum.SAML_CLAIM_USERNAME))
                .claimEmail(readString(KeyEnum.SAML_CLAIM_EMAIL))
                .claimDisplayName(readString(KeyEnum.SAML_CLAIM_DISPLAY_NAME))
                .claimGroups(readString(KeyEnum.SAML_CLAIM_GROUPS));

        String nameIdFormat = readString(KeyEnum.SAML_NAME_ID_FORMAT);
        if (StringUtils.isNotBlank(nameIdFormat)) {
            builder.nameIdFormat(nameIdFormat);
        }
        String signatureAlgorithm = readString(KeyEnum.SAML_SIGNATURE_ALGORITHM);
        if (StringUtils.isNotBlank(signatureAlgorithm)) {
            builder.signatureAlgorithm(signatureAlgorithm);
        }
        return builder.build();
    }

    @Override
    public boolean isEnabled() {
        return readBoolean(KeyEnum.SAML_LOGIN_ENABLE, false);
    }

    private String decryptPrivateKey(String stored) {
        if (StringUtils.isBlank(stored)) {
            return null;
        }
        if (!ssoSecretCipher.isEnabled()) {
            // Master key not configured; do not attempt to use the stored value.
            return null;
        }
        return ssoSecretCipher.decrypt(stored);
    }

    private String readString(KeyEnum key) {
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.SAML, key);
        if (settings == null) {
            return null;
        }
        Object value = settings.getValue() != null ? settings.getValue() : settings.getDefault_value();
        return value == null ? null : value.toString();
    }

    private boolean readBoolean(KeyEnum key, boolean defaultValue) {
        String raw = readString(key);
        if (StringUtils.isBlank(raw)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }

    private int readInt(KeyEnum key, int defaultValue) {
        String raw = readString(key);
        if (StringUtils.isBlank(raw)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
