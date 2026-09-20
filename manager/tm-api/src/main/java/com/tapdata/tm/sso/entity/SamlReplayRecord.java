package com.tapdata.tm.sso.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * A one-time record used to detect replay of SAML protocol messages across all
 * cluster nodes. A record is inserted the first time a given message id is seen;
 * a duplicate insert (enforced by the unique {@code (type, recordId)} index) means
 * the message is being replayed and must be rejected.
 * <p>
 * Records expire automatically via a TTL index on {@link #expiresAt} so the
 * collection never grows unbounded. The TTL is created by a startup patch.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Document("SamlReplayCache")
@CompoundIndex(name = "unq_saml_replay_type_record", def = "{'type': 1, 'recordId': 1}", unique = true)
public class SamlReplayRecord extends BaseEntity {

    /** The kind of id being tracked (e.g. "assertion" or "response"). */
    private String type;

    /** The SAML message/assertion id that must only be consumed once. */
    private String recordId;

    /** Creation time, retained for audit and diagnostics. */
    private Date createdAt;

    /**
     * Absolute expiry time for this replay guard. For assertions this is their
     * validated NotOnOrAfter time, preventing a still-valid assertion from
     * becoming replayable merely because a fixed cache window elapsed.
     */
    private Date expiresAt;
}
