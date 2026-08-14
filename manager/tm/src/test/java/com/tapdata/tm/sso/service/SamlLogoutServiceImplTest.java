package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.InboundLogout;
import com.tapdata.tm.sso.dto.LogoutRedirectResult;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.dto.SpKeyPair;
import com.tapdata.tm.sso.security.SpKeyPairGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SamlLogoutServiceImplTest {

    private SamlLogoutServiceImpl service;

    @Mock
    private SamlReplayCacheService replayCacheService;

    private SpKeyPair keyPair;

    @BeforeEach
    void setUp() {
        service = new SamlLogoutServiceImpl();
        ReflectionTestUtils.setField(service, "replayCacheService", replayCacheService);
        keyPair = new SpKeyPairGenerator().generate("CN=TapData SP");
    }

    private SamlConfig baseConfig() {
        return SamlConfig.builder()
                .enabled(true)
                .spEntityId("https://tapdata/sp")
                .idpEntityId("https://idp/entity")
                .idpSloUrl("https://idp/slo")
                .idpSigningCertificate(keyPair.getCertificatePem())
                .nameIdFormat("urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress")
                .build();
    }

    private Map<String, String> queryParams(String url) throws Exception {
        Map<String, String> params = new HashMap<>();
        String query = url.substring(url.indexOf('?') + 1);
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            params.put(pair.substring(0, eq), URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return params;
    }

    @Test
    @DisplayName("builds a LogoutRequest redirect carrying SAMLRequest and a request id")
    void buildsLogoutRequest() {
        LogoutRedirectResult result = service.buildLogoutRequest(baseConfig(), "user@x", "idx-1", "state-1");
        assertNotNull(result.getRequestId());
        assertTrue(result.getRequestId().startsWith("_"));
        assertTrue(result.getRedirectUrl().startsWith("https://idp/slo?"));
        assertTrue(result.getRedirectUrl().contains("SAMLRequest="));
        assertTrue(result.getRedirectUrl().contains("RelayState=state-1"));
    }

    @Test
    @DisplayName("signed LogoutRequest includes SigAlg and Signature and round-trips through parse")
    void signedRoundTrip() throws Exception {
        SamlConfig config = baseConfig();
        config.setSignAuthnRequest(true);
        config.setSpPrivateKey(keyPair.getPrivateKeyPem());

        LogoutRedirectResult result = service.buildLogoutRequest(config, "user@x", "idx-1", null);
        assertTrue(result.getRedirectUrl().contains("SigAlg="));
        assertTrue(result.getRedirectUrl().contains("Signature="));

        Map<String, String> params = queryParams(result.getRedirectUrl());
        // Reconstruct the exact signed portion (SAMLRequest[&RelayState]&SigAlg).
        String signedQuery = "SAMLRequest=" + enc(params.get("SAMLRequest"))
                + "&SigAlg=" + enc(params.get("SigAlg"));

        when(replayCacheService.recordIfFirstUse(org.mockito.ArgumentMatchers.eq("logoutrequest"),
                org.mockito.ArgumentMatchers.anyString())).thenReturn(true);

        InboundLogout inbound = service.parseLogoutRequest(config, params.get("SAMLRequest"),
                params.get("SigAlg"), params.get("Signature"), signedQuery);
        assertEquals("user@x", inbound.getNameId());
        assertEquals("idx-1", inbound.getSessionIndex());
        assertEquals("https://tapdata/sp", inbound.getIssuer());
    }

    private String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("unsigned LogoutRequest is accepted when signing is not required")
    void unsignedAcceptedWhenNotRequired() {
        SamlConfig config = baseConfig();
        LogoutRedirectResult result = service.buildLogoutRequest(config, "user@x", null, null);
        when(replayCacheService.recordIfFirstUse(org.mockito.ArgumentMatchers.eq("logoutrequest"),
                org.mockito.ArgumentMatchers.anyString())).thenReturn(true);
        Map<String, String> params;
        try {
            params = queryParams(result.getRedirectUrl());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        InboundLogout inbound = service.parseLogoutRequest(config, params.get("SAMLRequest"), null, null, null);
        assertEquals("user@x", inbound.getNameId());
    }

    @Test
    @DisplayName("unsigned LogoutRequest is rejected when signing is required")
    void unsignedRejectedWhenRequired() {
        SamlConfig config = baseConfig();
        config.setSignAuthnRequest(true);
        config.setSpPrivateKey(keyPair.getPrivateKeyPem());
        LogoutRedirectResult built = service.buildLogoutRequest(config, "user@x", null, null);
        Map<String, String> params;
        try {
            params = queryParams(built.getRedirectUrl());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Present the message but drop the signature.
        assertThrows(SamlValidationException.class, () ->
                service.parseLogoutRequest(config, params.get("SAMLRequest"), null, null, null));
    }

    @Test
    @DisplayName("replayed LogoutRequest id is rejected")
    void replayRejected() {
        SamlConfig config = baseConfig();
        LogoutRedirectResult built = service.buildLogoutRequest(config, "user@x", null, null);
        Map<String, String> params;
        try {
            params = queryParams(built.getRedirectUrl());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        when(replayCacheService.recordIfFirstUse(org.mockito.ArgumentMatchers.eq("logoutrequest"),
                org.mockito.ArgumentMatchers.anyString())).thenReturn(false);
        assertThrows(SamlValidationException.class, () ->
                service.parseLogoutRequest(config, params.get("SAMLRequest"), null, null, null));
    }

    @Test
    @DisplayName("builds a Success LogoutResponse redirect")
    void buildsLogoutResponse() {
        LogoutRedirectResult result = service.buildLogoutResponse(baseConfig(), "_req-1", "state-2");
        assertTrue(result.getRedirectUrl().startsWith("https://idp/slo?"));
        assertTrue(result.getRedirectUrl().contains("SAMLResponse="));
        assertTrue(result.getRedirectUrl().contains("RelayState=state-2"));
    }

    @Test
    @DisplayName("missing IdP SLO URL is rejected for both request and response")
    void missingSloUrl() {
        SamlConfig config = baseConfig();
        config.setIdpSloUrl(null);
        assertThrows(IllegalStateException.class, () -> service.buildLogoutRequest(config, "user@x", null, null));
        assertThrows(IllegalStateException.class, () -> service.buildLogoutResponse(config, "_r", null));
    }

    @Test
    @DisplayName("XXE: inbound LogoutRequest with a DOCTYPE / external entity is rejected (AC-054)")
    void xxeDoctypeRejected() throws Exception {
        SamlConfig config = baseConfig();
        String xxe = "<?xml version=\"1.0\"?>"
                + "<!DOCTYPE LogoutRequest [ <!ENTITY xxe SYSTEM \"file:///etc/passwd\"> ]>"
                + "<samlp:LogoutRequest xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\">&xxe;</samlp:LogoutRequest>";
        String samlRequest = deflateBase64(xxe);
        assertThrows(SamlValidationException.class, () ->
                service.parseLogoutRequest(config, samlRequest, null, null, null));
    }

    private String deflateBase64(String xml) throws Exception {
        java.util.zip.Deflater deflater = new java.util.zip.Deflater(java.util.zip.Deflater.DEFLATED, true);
        deflater.setInput(xml.getBytes(StandardCharsets.UTF_8));
        deflater.finish();
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int n = deflater.deflate(buffer);
            out.write(buffer, 0, n);
        }
        deflater.end();
        return java.util.Base64.getEncoder().encodeToString(out.toByteArray());
    }
}
