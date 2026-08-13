package com.tapdata.tm.sso.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.security.SsoSecretCipher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SamlConfigServiceImplTest {

    private SettingsService settingsService;
    private SsoSecretCipher ssoSecretCipher;
    private SamlConfigServiceImpl service;

    @BeforeEach
    void setUp() {
        settingsService = mock(SettingsService.class);
        ssoSecretCipher = mock(SsoSecretCipher.class);
        service = new SamlConfigServiceImpl();
        ReflectionTestUtils.setField(service, "settingsService", settingsService);
        ReflectionTestUtils.setField(service, "ssoSecretCipher", ssoSecretCipher);
    }

    private void stub(KeyEnum key, String value) {
        Settings s = new Settings();
        s.setCategory(CategoryEnum.SAML.getValue());
        s.setKey(key.getValue());
        s.setValue(value);
        when(settingsService.getByCategoryAndKey(eq(CategoryEnum.SAML), eq(key))).thenReturn(s);
    }

    @Test
    @DisplayName("defaults apply when nothing is configured")
    void defaultsWhenEmpty() {
        SamlConfig config = service.getConfig();
        assertFalse(config.isEnabled());
        assertTrue(config.isWantAssertionsSigned());
        assertFalse(config.isSignAuthnRequest());
        assertEquals(120, config.getClockSkewSeconds());
        assertEquals("urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress", config.getNameIdFormat());
        assertEquals("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256", config.getSignatureAlgorithm());
        assertNull(config.getIdpSsoUrl());
    }

    @Test
    @DisplayName("reads generic fields from Settings")
    void readsFields() {
        stub(KeyEnum.SAML_LOGIN_ENABLE, "true");
        stub(KeyEnum.SAML_SP_ENTITY_ID, "https://tapdata/sp");
        stub(KeyEnum.SAML_IDP_SSO_URL, "https://idp/sso");
        stub(KeyEnum.SAML_WANT_ASSERTIONS_SIGNED, "false");
        stub(KeyEnum.SAML_CLOCK_SKEW_SECONDS, "300");
        stub(KeyEnum.SAML_CLAIM_EMAIL, "email");

        SamlConfig config = service.getConfig();
        assertTrue(config.isEnabled());
        assertEquals("https://tapdata/sp", config.getSpEntityId());
        assertEquals("https://idp/sso", config.getIdpSsoUrl());
        assertFalse(config.isWantAssertionsSigned());
        assertEquals(300, config.getClockSkewSeconds());
        assertEquals("email", config.getClaimEmail());
    }

    @Test
    @DisplayName("private key is decrypted when cipher is enabled")
    void decryptsPrivateKey() {
        stub(KeyEnum.SAML_SP_PRIVATE_KEY, "ENC-BLOB");
        when(ssoSecretCipher.isEnabled()).thenReturn(true);
        when(ssoSecretCipher.decrypt("ENC-BLOB")).thenReturn("PLAIN-KEY");

        SamlConfig config = service.getConfig();
        assertEquals("PLAIN-KEY", config.getSpPrivateKey());
    }

    @Test
    @DisplayName("private key is not exposed when master key is absent")
    void privateKeyHiddenWhenCipherDisabled() {
        stub(KeyEnum.SAML_SP_PRIVATE_KEY, "ENC-BLOB");
        when(ssoSecretCipher.isEnabled()).thenReturn(false);

        SamlConfig config = service.getConfig();
        assertNull(config.getSpPrivateKey());
    }

    @Test
    @DisplayName("invalid integer falls back to default")
    void invalidIntFallsBack() {
        stub(KeyEnum.SAML_CLOCK_SKEW_SECONDS, "not-a-number");
        assertEquals(120, service.getConfig().getClockSkewSeconds());
    }

    @Test
    @DisplayName("isEnabled reflects the enable flag")
    void isEnabledFlag() {
        assertFalse(service.isEnabled());
        stub(KeyEnum.SAML_LOGIN_ENABLE, "true");
        assertTrue(service.isEnabled());
    }
}
