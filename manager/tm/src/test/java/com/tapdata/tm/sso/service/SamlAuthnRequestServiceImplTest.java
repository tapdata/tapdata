package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.AuthnRequestResult;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.dto.SpKeyPair;
import com.tapdata.tm.sso.security.SpKeyPairGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SamlAuthnRequestServiceImplTest {

    private final SamlAuthnRequestServiceImpl service = new SamlAuthnRequestServiceImpl();

    private SamlConfig baseConfig() {
        return SamlConfig.builder()
                .enabled(true)
                .spEntityId("https://tapdata/sp")
                .spAcsUrl("https://tapdata/api/sso/saml/acs")
                .idpEntityId("https://idp/entity")
                .idpSsoUrl("https://idp/sso")
                .nameIdFormat("urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress")
                .build();
    }

    @Test
    @DisplayName("builds a redirect URL carrying SAMLRequest and a request id")
    void buildsRedirect() {
        AuthnRequestResult result = service.buildRedirect(baseConfig(), "state-123");
        assertNotNull(result.getRequestId());
        assertTrue(result.getRequestId().startsWith("_"));
        assertTrue(result.getRedirectUrl().startsWith("https://idp/sso?"));
        assertTrue(result.getRedirectUrl().contains("SAMLRequest="));
        assertTrue(result.getRedirectUrl().contains("RelayState=state-123"));
    }

    @Test
    @DisplayName("signed AuthnRequest includes SigAlg and Signature params")
    void signedRedirect() {
        SpKeyPair keyPair = new SpKeyPairGenerator().generate("CN=TapData SP");
        SamlConfig config = baseConfig();
        config.setSignAuthnRequest(true);
        config.setSpPrivateKey(keyPair.getPrivateKeyPem());

        AuthnRequestResult result = service.buildRedirect(config, null);
        assertTrue(result.getRedirectUrl().contains("SigAlg="));
        assertTrue(result.getRedirectUrl().contains("Signature="));
    }

    @Test
    @DisplayName("missing IdP SSO URL is rejected")
    void missingSsoUrl() {
        SamlConfig config = baseConfig();
        config.setIdpSsoUrl(null);
        assertThrows(IllegalStateException.class, () -> service.buildRedirect(config, null));
    }

    @Test
    @DisplayName("signing enabled but no private key is rejected")
    void signingWithoutKey() {
        SamlConfig config = baseConfig();
        config.setSignAuthnRequest(true);
        assertThrows(IllegalStateException.class, () -> service.buildRedirect(config, null));
    }
}
