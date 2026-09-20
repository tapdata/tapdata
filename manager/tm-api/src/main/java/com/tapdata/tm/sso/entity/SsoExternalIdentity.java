package com.tapdata.tm.sso.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Immutable binding between an external SAML identity (NameID issued by an IdP) and a
 * TapData user account. Created on first successful SSO login and used on subsequent
 * logins to resolve the same TapData user without re-provisioning.
 * <p>
 * Uniqueness is enforced on {@code (idpEntityId, nameId)} so that the same NameID from
 * the same IdP always maps to exactly one binding. A TapData user may have multiple
 * bindings (e.g. from different IdPs), so {@code tapdataUserId} is indexed but not unique.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Document("SsoExternalIdentity")
@CompoundIndex(name = "unq_sso_idp_nameId", def = "{'idpEntityId': 1, 'nameId': 1}", unique = true)
public class SsoExternalIdentity extends BaseEntity {

    /** The SAML NameID value that uniquely identifies the subject at the IdP. */
    private String nameId;

    /** The NameID format URI (e.g. emailAddress / persistent). */
    private String nameIdFormat;

    /** Entity ID of the IdP that issued this identity (traceability of the source). */
    private String idpEntityId;

    /** The linked TapData user's id (User._id as hex string). */
    @Indexed
    private String tapdataUserId;
}
