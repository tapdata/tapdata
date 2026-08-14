package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlAuthenticatedSubject;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.dto.SpKeyPair;
import com.tapdata.tm.sso.security.SpKeyPairGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SamlResponseValidatorImplTest {

    private static final String SP_ENTITY = "https://tapdata/sp";
    private static final String ACS_URL = "https://tapdata/api/sso/saml/acs";
    private static final String IDP_ENTITY = "https://idp/entity";
    private static final String REQUEST_ID = "_req-123";

    private SamlResponseValidatorImpl validator;
    private SpKeyPair idpKeyPair;
    private SamlConfig config;

    @BeforeEach
    void setUp() {
        validator = new SamlResponseValidatorImpl();
        // Replay cache that accepts the first use and rejects duplicates.
        java.util.Set<String> seen = new java.util.HashSet<>();
        SamlReplayCacheService replayCache = (type, recordId) -> seen.add(type + ":" + recordId);
        org.springframework.test.util.ReflectionTestUtils.setField(validator, "replayCacheService", replayCache);

        idpKeyPair = new SpKeyPairGenerator().generate("CN=Test IdP");
        config = SamlConfig.builder()
                .enabled(true)
                .spEntityId(SP_ENTITY)
                .spAcsUrl(ACS_URL)
                .idpEntityId(IDP_ENTITY)
                .idpSigningCertificate(idpKeyPair.getCertificatePem())
                .wantAssertionsSigned(true)
                .clockSkewSeconds(120)
                .build();
    }

    private String signedResponse(String nameId, String inResponseTo, Instant notOnOrAfter) throws Exception {
        return SamlTestAssertions.buildSignedResponseBase64(idpKeyPair.getCertificatePem(),
                idpKeyPair.getPrivateKeyPem(), SP_ENTITY, ACS_URL, IDP_ENTITY, nameId, inResponseTo, notOnOrAfter);
    }

    @Test
    @DisplayName("valid signed response is accepted and subject extracted")
    void validResponse() throws Exception {
        String response = signedResponse("user@corp.com", REQUEST_ID, Instant.now().plusSeconds(300));
        SamlAuthenticatedSubject subject = validator.validate(config, response, REQUEST_ID);
        assertEquals("user@corp.com", subject.getNameId());
        assertEquals(IDP_ENTITY, subject.getIdpEntityId());
        assertEquals("session-index-1", subject.getSessionIndex());
    }

    @Test
    @DisplayName("replayed assertion is rejected")
    void replayRejected() throws Exception {
        String response = signedResponse("user@corp.com", REQUEST_ID, Instant.now().plusSeconds(300));
        validator.validate(config, response, REQUEST_ID);
        assertThrows(SamlValidationException.class, () -> validator.validate(config, response, REQUEST_ID));
    }

    @Test
    @DisplayName("expired assertion (NotOnOrAfter in past) is rejected")
    void expiredRejected() throws Exception {
        String response = signedResponse("user@corp.com", REQUEST_ID, Instant.now().minusSeconds(600));
        assertThrows(SamlValidationException.class, () -> validator.validate(config, response, REQUEST_ID));
    }

    @Test
    @DisplayName("wrong audience is rejected")
    void wrongAudienceRejected() throws Exception {
        String response = signedResponse("user@corp.com", REQUEST_ID, Instant.now().plusSeconds(300));
        config.setSpEntityId("https://other/sp");
        assertThrows(SamlValidationException.class, () -> validator.validate(config, response, REQUEST_ID));
    }

    @Test
    @DisplayName("mismatched InResponseTo is rejected")
    void wrongInResponseToRejected() throws Exception {
        String response = signedResponse("user@corp.com", REQUEST_ID, Instant.now().plusSeconds(300));
        assertThrows(SamlValidationException.class, () -> validator.validate(config, response, "_different"));
    }

    @Test
    @DisplayName("signature from a different key is rejected")
    void wrongSignatureRejected() throws Exception {
        String response = signedResponse("user@corp.com", REQUEST_ID, Instant.now().plusSeconds(300));
        // Point config at a different IdP cert so signature verification fails.
        config.setIdpSigningCertificate(new SpKeyPairGenerator().generate("CN=Other").getCertificatePem());
        assertThrows(SamlValidationException.class, () -> validator.validate(config, response, REQUEST_ID));
    }

    @Test
    @DisplayName("XSW: forged assertion wrapped alongside the signed one is rejected")
    void signatureWrappingRejected() throws Exception {
        String wrapped = SamlTestAssertions.buildSignatureWrappingResponseBase64(
                idpKeyPair.getCertificatePem(), idpKeyPair.getPrivateKeyPem(),
                SP_ENTITY, ACS_URL, IDP_ENTITY, "victim@corp.com", "attacker@evil.com",
                REQUEST_ID, Instant.now().plusSeconds(300));
        assertThrows(SamlValidationException.class, () -> validator.validate(config, wrapped, REQUEST_ID));
    }

    @Test
    @DisplayName("XXE: SAMLResponse with a DOCTYPE / external entity is rejected (AC-054)")
    void xxeDoctypeRejected() {
        String xxe = "<?xml version=\"1.0\"?>"
                + "<!DOCTYPE Response [ <!ENTITY xxe SYSTEM \"file:///etc/passwd\"> ]>"
                + "<samlp:Response xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\">&xxe;</samlp:Response>";
        String base64 = java.util.Base64.getEncoder().encodeToString(xxe.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(SamlValidationException.class, () -> validator.validate(config, base64, REQUEST_ID));
    }
}
