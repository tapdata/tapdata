package com.tapdata.tm.sso.dto;

import lombok.Data;

/**
 * Request payload carrying a raw IdP metadata XML document to be parsed.
 */
@Data
public class MetadataImportRequest {

    /** The raw IdP SAML metadata XML. */
    private String metadataXml;
}
