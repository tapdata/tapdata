package com.tapdata.tm.sso.service;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.dto.SamlConfigForm;
import com.tapdata.tm.sso.dto.SamlConfigView;
import com.tapdata.tm.sso.security.SsoSecretCipher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SamlConfigServiceImplTest {

    private SettingsService settingsService;
    private SsoSecretCipher ssoSecretCipher;
    private MongoTemplate mongoTemplate;
    private SamlConfigServiceImpl service;

    @BeforeEach
    void setUp() {
        settingsService = mock(SettingsService.class);
        ssoSecretCipher = mock(SsoSecretCipher.class);
        mongoTemplate = mock(MongoTemplate.class);
        service = new SamlConfigServiceImpl();
        ReflectionTestUtils.setField(service, "settingsService", settingsService);
        ReflectionTestUtils.setField(service, "ssoSecretCipher", ssoSecretCipher);
        ReflectionTestUtils.setField(service, "mongoTemplate", mongoTemplate);
    }

    private void stub(KeyEnum key, String value) {
        Settings s = new Settings();
        s.setCategory(CategoryEnum.SAML.getValue());
        s.setKey(key.getValue());
        s.setValue(value);
        when(settingsService.getByCategoryAndKey(eq(CategoryEnum.SAML), eq(key))).thenReturn(s);
    }

    /** Boolean SAML toggles are stored in the {@code open} field, not {@code value}. */
    private void stubBoolean(KeyEnum key, boolean value) {
        Settings s = new Settings();
        s.setCategory(CategoryEnum.SAML.getValue());
        s.setKey(key.getValue());
        s.setOpen(value);
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
        stubBoolean(KeyEnum.SAML_LOGIN_ENABLE, true);
        stub(KeyEnum.SAML_SP_ENTITY_ID, "https://tapdata/sp");
        stub(KeyEnum.SAML_IDP_SSO_URL, "https://idp/sso");
        stubBoolean(KeyEnum.SAML_WANT_ASSERTIONS_SIGNED, false);
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
        stubBoolean(KeyEnum.SAML_LOGIN_ENABLE, true);
        assertTrue(service.isEnabled());
    }

    @Test
    @DisplayName("masked view never exposes the private key but flags its presence")
    void maskedViewHidesPrivateKey() {
        stub(KeyEnum.SAML_SP_PRIVATE_KEY, "ENC-BLOB");
        stub(KeyEnum.SAML_SP_ENTITY_ID, "https://tapdata/sp");
        when(ssoSecretCipher.isEnabled()).thenReturn(true);
        when(ssoSecretCipher.decrypt("ENC-BLOB")).thenReturn("PLAIN-KEY");

        SamlConfigView view = service.getMaskedConfig();
        assertTrue(view.isSpPrivateKeyConfigured());
        assertEquals("https://tapdata/sp", view.getSpEntityId());
        // SamlConfigView has no field that can carry the private key value at all.
    }

    @Test
    @DisplayName("masked view reports no key when none is stored")
    void maskedViewNoKey() {
        SamlConfigView view = service.getMaskedConfig();
        assertFalse(view.isSpPrivateKeyConfigured());
    }

    @Test
    @DisplayName("saveConfig upserts settings and encrypts the private key")
    void saveEncryptsPrivateKey() {
        when(ssoSecretCipher.isEnabled()).thenReturn(true);
        when(ssoSecretCipher.encrypt("PLAIN-KEY")).thenReturn("ENC-BLOB");

        SamlConfigForm form = new SamlConfigForm();
        form.setEnabled(true);
        form.setSpEntityId("https://tapdata/sp");
        form.setSpPrivateKey("PLAIN-KEY");

        service.saveConfig(form);

        ArgumentCaptor<Update> updateCaptor = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate, times(3)).upsert(any(Query.class), updateCaptor.capture(), eq(Settings.class));
        boolean storedEncrypted = updateCaptor.getAllValues().stream()
                .anyMatch(u -> u.getUpdateObject().toJson().contains("ENC-BLOB"));
        boolean storedPlain = updateCaptor.getAllValues().stream()
                .anyMatch(u -> u.getUpdateObject().toJson().contains("PLAIN-KEY"));
        assertTrue(storedEncrypted, "private key should be stored encrypted");
        assertFalse(storedPlain, "plaintext private key must never be stored");
    }

    @Test
    @DisplayName("blank private key on save preserves the existing key (no write)")
    void saveBlankKeyPreservesExisting() {
        SamlConfigForm form = new SamlConfigForm();
        form.setSpEntityId("https://tapdata/sp");
        // spPrivateKey left blank

        service.saveConfig(form);

        // one upsert for spEntityId only; encrypt never called
        verify(ssoSecretCipher, never()).encrypt(any());
        verify(mongoTemplate, times(1)).upsert(any(Query.class), any(Update.class), eq(Settings.class));
    }

    @Test
    @DisplayName("saving a private key without master key configured is rejected")
    void saveKeyWithoutMasterKeyRejected() {
        when(ssoSecretCipher.isEnabled()).thenReturn(false);
        SamlConfigForm form = new SamlConfigForm();
        form.setSpPrivateKey("PLAIN-KEY");

        assertThrows(IllegalStateException.class, () -> service.saveConfig(form));
    }

    @Test
    @DisplayName("null form is a no-op")
    void saveNullIsNoOp() {
        service.saveConfig(null);
        verify(mongoTemplate, never()).upsert(any(Query.class), any(Update.class), eq(Settings.class));
    }
}
