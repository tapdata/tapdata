package com.tapdata.tm.sso.dto;

import lombok.Data;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * The trusted result of successfully validating a SAML Response/Assertion: the
 * subject NameID plus the mapped attributes and session correlation data needed to
 * resolve a TapData user, issue a token and record an {@code SsoSession}.
 */
@Data
public class SamlAuthenticatedSubject {

    /** The validated NameID value. */
    private String nameId;

    /** The NameID format URI. */
    private String nameIdFormat;

    /** The IdP entity ID (Issuer) that asserted this subject. */
    private String idpEntityId;

    /** IdP SessionIndex (used to target IdP-initiated SLO). */
    private String sessionIndex;

    /** Optional session expiry advertised by the IdP (SessionNotOnOrAfter). */
    private Date sessionNotOnOrAfter;

    /** All SAML attributes (name -> values), used for claim/role mapping. */
    private Map<String, List<String>> attributes;
}
