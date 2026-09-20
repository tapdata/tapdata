package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlConfigForm;
import com.tapdata.tm.sso.dto.SamlValidationResult;
import com.tapdata.tm.sso.security.SpKeyPairGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SamlValidationServiceImplTest {

    private final SamlValidationServiceImpl service = new SamlValidationServiceImpl();
    private static final String VALID_CERT =
            new SpKeyPairGenerator().generate("CN=Test IdP").getCertificatePem();

    private SamlConfigForm minimalValid() {
        SamlConfigForm form = new SamlConfigForm();
        form.setSpEntityId("https://tapdata/sp");
        form.setSpAcsUrl("https://tapdata/acs");
        form.setIdpEntityId("https://idp/entity");
        form.setIdpSsoUrl("https://idp/sso");
        form.setIdpSigningCertificate(VALID_CERT);
        return form;
    }

    @Test
    @DisplayName("null form is invalid")
    void nullInvalid() {
        SamlValidationResult result = service.validate(null);
        assertFalse(result.isValid());
    }

    @Test
    @DisplayName("missing required fields produce errors")
    void missingRequired() {
        SamlValidationResult result = service.validate(new SamlConfigForm());
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("spEntityId")));
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("idpSsoUrl")));
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("idpSigningCertificate")));
    }

    @Test
    @DisplayName("weak signature algorithm (SHA-1) is rejected")
    void weakAlgorithmRejected() {
        SamlConfigForm form = minimalValid();
        form.setSignatureAlgorithm("http://www.w3.org/2000/09/xmldsig#rsa-sha1");
        SamlValidationResult result = service.validate(form);
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Weak signature algorithm")));
    }

    @Test
    @DisplayName("strong algorithm passes")
    void strongAlgorithmPasses() {
        SamlConfigForm form = minimalValid();
        form.setSignatureAlgorithm("http://www.w3.org/2001/04/xmldsig-more#rsa-sha256");
        SamlValidationResult result = service.validate(form);
        assertTrue(result.isValid());
    }

    @Test
    @DisplayName("invalid X.509 certificate content is rejected")
    void invalidCertRejected() {
        SamlConfigForm form = minimalValid();
        form.setSpCertificate("-----BEGIN CERTIFICATE-----\nNOT-BASE64!!!\n-----END CERTIFICATE-----");
        SamlValidationResult result = service.validate(form);
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("SP certificate is not a valid")));
    }

    @Test
    @DisplayName("disabling assertion signing produces a warning but stays valid")
    void assertionSigningWarning() {
        SamlConfigForm form = minimalValid();
        form.setWantAssertionsSigned(false);
        SamlValidationResult result = service.validate(form);
        assertTrue(result.isValid());
        assertTrue(result.getWarnings().stream().anyMatch(w -> w.contains("Assertion signing")));
    }
}
