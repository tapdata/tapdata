package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.IdpMetadata;
import com.tapdata.tm.sso.dto.SamlConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SamlMetadataServiceImplTest {

    private final SamlMetadataServiceImpl service = new SamlMetadataServiceImpl();

    private static final String IDP_METADATA = "<?xml version=\"1.0\"?>"
            + "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\""
            + " xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\" entityID=\"https://idp.example.com/entity\">"
            + "<md:IDPSSODescriptor protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">"
            + "<md:KeyDescriptor use=\"signing\"><ds:KeyInfo><ds:X509Data>"
            + "<ds:X509Certificate>MIICERT_SIGNING</ds:X509Certificate>"
            + "</ds:X509Data></ds:KeyInfo></md:KeyDescriptor>"
            + "<md:SingleLogoutService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
            + " Location=\"https://idp.example.com/slo\"/>"
            + "<md:SingleSignOnService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\""
            + " Location=\"https://idp.example.com/sso/post\"/>"
            + "<md:SingleSignOnService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
            + " Location=\"https://idp.example.com/sso/redirect\"/>"
            + "</md:IDPSSODescriptor></md:EntityDescriptor>";

    @Test
    @DisplayName("parses entity ID, SSO/SLO URLs and signing certificate")
    void parsesFields() {
        IdpMetadata md = service.parseIdpMetadata(IDP_METADATA);
        assertEquals("https://idp.example.com/entity", md.getIdpEntityId());
        // prefers HTTP-Redirect binding for SSO
        assertEquals("https://idp.example.com/sso/redirect", md.getIdpSsoUrl());
        assertEquals("https://idp.example.com/slo", md.getIdpSloUrl());
        assertEquals("MIICERT_SIGNING", md.getIdpSigningCertificate());
    }

    @Test
    @DisplayName("blank input is rejected")
    void blankRejected() {
        assertThrows(IllegalArgumentException.class, () -> service.parseIdpMetadata("  "));
    }

    @Test
    @DisplayName("non-metadata XML is rejected")
    void nonMetadataRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> service.parseIdpMetadata("<root><child/></root>"));
    }

    @Test
    @DisplayName("SP-only metadata (no IDPSSODescriptor) is rejected")
    void spOnlyRejected() {
        String spOnly = "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\""
                + " entityID=\"sp\"><md:SPSSODescriptor"
                + " protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\"/></md:EntityDescriptor>";
        assertThrows(IllegalArgumentException.class, () -> service.parseIdpMetadata(spOnly));
    }

    @Test
    @DisplayName("XXE: DOCTYPE with external entity is rejected (AC-054)")
    void xxeRejected() {
        String xxe = "<?xml version=\"1.0\"?>"
                + "<!DOCTYPE foo [ <!ENTITY xxe SYSTEM \"file:///etc/passwd\"> ]>"
                + "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\""
                + " entityID=\"&xxe;\"><md:IDPSSODescriptor"
                + " protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\"/></md:EntityDescriptor>";
        // disallow-doctype-decl causes parsing to fail before any entity resolution.
        assertThrows(IllegalArgumentException.class, () -> service.parseIdpMetadata(xxe));
    }

    @Test
    @DisplayName("builds SP metadata containing entity ID, ACS URL, SLO URL and certificate")
    void buildsSpMetadata() {
        SamlConfig config = SamlConfig.builder()
                .spEntityId("https://tapdata.example.com/sp")
                .spAcsUrl("https://tapdata.example.com/api/sso/saml/acs")
                .spSloUrl("https://tapdata.example.com/api/sso/saml/slo")
                .spCertificate("-----BEGIN CERTIFICATE-----\nMIISPCERT\n-----END CERTIFICATE-----")
                .build();
        String xml = service.buildSpMetadata(config);
        assertTrue(xml.contains("entityID=\"https://tapdata.example.com/sp\""));
        assertTrue(xml.contains("Location=\"https://tapdata.example.com/api/sso/saml/acs\""));
        // SLO endpoint must be advertised with the HTTP-Redirect binding (matches /slo)
        assertTrue(xml.contains("<md:SingleLogoutService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
                + " Location=\"https://tapdata.example.com/api/sso/saml/slo\"/>"));
        assertTrue(xml.contains("MIISPCERT"));
        assertTrue(xml.contains("SPSSODescriptor"));
        // private key must never appear in metadata
        assertTrue(!xml.contains("PRIVATE"));
    }

    @Test
    @DisplayName("SP metadata omits SingleLogoutService when spSloUrl is not configured")
    void buildsSpMetadataWithoutSlo() {
        SamlConfig config = SamlConfig.builder()
                .spEntityId("https://tapdata.example.com/sp")
                .spAcsUrl("https://tapdata.example.com/api/sso/saml/acs")
                .build();
        String xml = service.buildSpMetadata(config);
        assertTrue(!xml.contains("SingleLogoutService"));
    }

    @Test
    @DisplayName("SP metadata build requires entity ID and ACS URL")
    void buildRequiresFields() {
        assertThrows(IllegalStateException.class,
                () -> service.buildSpMetadata(SamlConfig.builder().spEntityId("only-entity").build()));
    }

    @Test
    @DisplayName("SLO is optional; absent SLO yields null")
    void sloOptional() {
        String noSlo = "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\""
                + " entityID=\"idp\"><md:IDPSSODescriptor"
                + " protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">"
                + "<md:SingleSignOnService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\""
                + " Location=\"https://idp/sso\"/></md:IDPSSODescriptor></md:EntityDescriptor>";
        IdpMetadata md = service.parseIdpMetadata(noSlo);
        assertNull(md.getIdpSloUrl());
        assertEquals("https://idp/sso", md.getIdpSsoUrl());
    }
}
