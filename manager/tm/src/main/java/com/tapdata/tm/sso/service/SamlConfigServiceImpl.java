package com.tapdata.tm.sso.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.dto.SamlConfigForm;
import com.tapdata.tm.sso.dto.SamlConfigView;
import com.tapdata.tm.sso.security.SsoSecretCipher;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
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

    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public SamlConfig getConfig() {
        SamlConfig.SamlConfigBuilder builder = SamlConfig.builder()
                .enabled(readBoolean(KeyEnum.SAML_LOGIN_ENABLE, false))
                .spEntityId(readString(KeyEnum.SAML_SP_ENTITY_ID))
                .spAcsUrl(readString(KeyEnum.SAML_SP_ACS_URL))
                .spSloUrl(readString(KeyEnum.SAML_SP_SLO_URL))
                .spPrivateKey(decryptPrivateKey(readString(KeyEnum.SAML_SP_PRIVATE_KEY)))
                .spCertificate(readString(KeyEnum.SAML_SP_CERTIFICATE))
                .idpEntityId(readString(KeyEnum.SAML_IDP_ENTITY_ID))
                .idpSsoUrl(readString(KeyEnum.SAML_IDP_SSO_URL))
                .idpSloUrl(readString(KeyEnum.SAML_IDP_SLO_URL))
                .idpSigningCertificate(readString(KeyEnum.SAML_IDP_SIGNING_CERTIFICATE))
                .wantAssertionsSigned(readBoolean(KeyEnum.SAML_WANT_ASSERTIONS_SIGNED, true))
                .signAuthnRequest(readBoolean(KeyEnum.SAML_SIGN_AUTHN_REQUEST, false))
                .clockSkewSeconds(readInt(KeyEnum.SAML_CLOCK_SKEW_SECONDS, 120))
                .idpInitiatedEnabled(readBoolean(KeyEnum.SAML_IDP_INITIATED_ENABLED, false))
                .jitProvisioningEnabled(readBoolean(KeyEnum.SAML_JIT_PROVISIONING_ENABLED, false))
                .loginRedirectUrl(readString(KeyEnum.SAML_LOGIN_REDIRECT_URL))
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

    @Override
    public SamlConfigView getMaskedConfig() {
        SamlConfig config = getConfig();
        boolean keyConfigured = StringUtils.isNotBlank(readString(KeyEnum.SAML_SP_PRIVATE_KEY));
        return SamlConfigView.builder()
                .enabled(config.isEnabled())
                .spEntityId(config.getSpEntityId())
                .spAcsUrl(config.getSpAcsUrl())
                .spSloUrl(config.getSpSloUrl())
                .spPrivateKeyConfigured(keyConfigured)
                .spCertificate(config.getSpCertificate())
                .idpEntityId(config.getIdpEntityId())
                .idpSsoUrl(config.getIdpSsoUrl())
                .idpSloUrl(config.getIdpSloUrl())
                .idpSigningCertificate(config.getIdpSigningCertificate())
                .nameIdFormat(config.getNameIdFormat())
                .wantAssertionsSigned(config.isWantAssertionsSigned())
                .signAuthnRequest(config.isSignAuthnRequest())
                .signatureAlgorithm(config.getSignatureAlgorithm())
                .clockSkewSeconds(config.getClockSkewSeconds())
                .idpInitiatedEnabled(config.isIdpInitiatedEnabled())
                .jitProvisioningEnabled(config.isJitProvisioningEnabled())
                .loginRedirectUrl(config.getLoginRedirectUrl())
                .claimUsername(config.getClaimUsername())
                .claimEmail(config.getClaimEmail())
                .claimDisplayName(config.getClaimDisplayName())
                .claimGroups(config.getClaimGroups())
                .build();
    }

    @Override
    public void saveConfig(SamlConfigForm form) {
        if (form == null) {
            return;
        }
        writeBoolean(KeyEnum.SAML_LOGIN_ENABLE, form.getEnabled());
        writeString(KeyEnum.SAML_SP_ENTITY_ID, form.getSpEntityId());
        writeString(KeyEnum.SAML_SP_ACS_URL, form.getSpAcsUrl());
        writeString(KeyEnum.SAML_SP_SLO_URL, form.getSpSloUrl());
        writeSpPrivateKey(form.getSpPrivateKey());
        writeString(KeyEnum.SAML_SP_CERTIFICATE, form.getSpCertificate());
        writeString(KeyEnum.SAML_IDP_ENTITY_ID, form.getIdpEntityId());
        writeString(KeyEnum.SAML_IDP_SSO_URL, form.getIdpSsoUrl());
        writeString(KeyEnum.SAML_IDP_SLO_URL, form.getIdpSloUrl());
        writeString(KeyEnum.SAML_IDP_SIGNING_CERTIFICATE, form.getIdpSigningCertificate());
        writeString(KeyEnum.SAML_NAME_ID_FORMAT, form.getNameIdFormat());
        writeBoolean(KeyEnum.SAML_WANT_ASSERTIONS_SIGNED, form.getWantAssertionsSigned());
        writeBoolean(KeyEnum.SAML_SIGN_AUTHN_REQUEST, form.getSignAuthnRequest());
        writeString(KeyEnum.SAML_SIGNATURE_ALGORITHM, form.getSignatureAlgorithm());
        writeInt(KeyEnum.SAML_CLOCK_SKEW_SECONDS, form.getClockSkewSeconds());
        writeBoolean(KeyEnum.SAML_IDP_INITIATED_ENABLED, form.getIdpInitiatedEnabled());
        writeBoolean(KeyEnum.SAML_JIT_PROVISIONING_ENABLED, form.getJitProvisioningEnabled());
        writeString(KeyEnum.SAML_LOGIN_REDIRECT_URL, form.getLoginRedirectUrl());
        writeString(KeyEnum.SAML_CLAIM_USERNAME, form.getClaimUsername());
        writeString(KeyEnum.SAML_CLAIM_EMAIL, form.getClaimEmail());
        writeString(KeyEnum.SAML_CLAIM_DISPLAY_NAME, form.getClaimDisplayName());
        writeString(KeyEnum.SAML_CLAIM_GROUPS, form.getClaimGroups());
    }

    /**
     * Encrypt and store the SP private key. A blank value preserves the existing
     * stored key so clients that can never read the key back can still re-save
     * other fields. Requires the master key to be configured.
     */
    private void writeSpPrivateKey(String plaintextPem) {
        if (StringUtils.isBlank(plaintextPem)) {
            return;
        }
        if (!ssoSecretCipher.isEnabled()) {
            throw new IllegalStateException(
                    "Cannot store SP private key: SSO master key (SSO_MASTER_KEY) is not configured");
        }
        writeRaw(KeyEnum.SAML_SP_PRIVATE_KEY, ssoSecretCipher.encrypt(plaintextPem));
    }

    private void writeString(KeyEnum key, String value) {
        if (value == null) {
            return;
        }
        writeRaw(key, value);
    }

    private void writeBoolean(KeyEnum key, Boolean value) {
        if (value == null) {
            return;
        }
        writeBooleanRaw(key, value);
    }

    private void writeInt(KeyEnum key, Integer value) {
        if (value == null) {
            return;
        }
        writeRaw(key, Integer.toString(value));
    }

    /**
     * Upsert a single {@code saml.*} setting row under the SAML category so the
     * configuration is self-contained and does not depend on pre-seeded rows.
     */
    private void writeRaw(KeyEnum key, String value) {
        Query query = Query.query(Criteria.where("category").is(CategoryEnum.SAML.getValue())
                .and("key").is(key.getValue()));
        Update update = new Update()
                .set("category", CategoryEnum.SAML.getValue())
                .set("key", key.getValue())
                .set("value", value);
        mongoTemplate.upsert(query, update, Settings.class);
    }

    /**
     * Upsert a boolean {@code saml.*} toggle into the {@code open} field, consistent
     * with how {@link #readBoolean} reads it and how the UI toggle is stored.
     */
    private void writeBooleanRaw(KeyEnum key, boolean value) {
        Query query = Query.query(Criteria.where("category").is(CategoryEnum.SAML.getValue())
                .and("key").is(key.getValue()));
        Update update = new Update()
                .set("category", CategoryEnum.SAML.getValue())
                .set("key", key.getValue())
                .set("open", value);
        mongoTemplate.upsert(query, update, Settings.class);
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
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.SAML, key);
        if (settings == null || settings.getOpen() == null) {
            return defaultValue;
        }
        return settings.getOpen();
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
