package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.IdpMetadata;
import com.tapdata.tm.sso.dto.SamlConfig;

/**
 * Parses IdP SAML metadata and generates the TapData SP metadata document.
 */
public interface SamlMetadataService {

    /**
     * Parse an IdP SAML 2.0 metadata XML document and extract the values needed to
     * configure the IdP side of the connection.
     * <p>
     * The XML is parsed with a hardened parser: DTDs, external general/parameter
     * entities and external schema access are all disabled to prevent XXE (AC-054).
     *
     * @param metadataXml the raw metadata XML.
     * @return the extracted values (fields may be {@code null} when absent).
     * @throws IllegalArgumentException when the input is blank or not parseable as
     *                                  SAML metadata.
     */
    IdpMetadata parseIdpMetadata(String metadataXml);

    /**
     * Build the TapData SP SAML 2.0 metadata XML from the current configuration.
     * <p>
     * Contains the SP entity ID, the Assertion Consumer Service (HTTP-POST) URL and,
     * when present, the SP certificate for signing/encryption use descriptors. The
     * SP private key is never included.
     *
     * @param config the current configuration.
     * @return the SP metadata XML.
     * @throws IllegalStateException when required SP fields (entity ID, ACS URL) are
     *                               missing.
     */
    String buildSpMetadata(SamlConfig config);
}
