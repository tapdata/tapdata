package com.tapdata.tm.proxyprocess.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@EqualsAndHashCode(callSuper = true)
@Document("AgentProcess")
public class AgentProcessEntity extends BaseEntity {
    private String agentName;
    @Indexed
    private String agentType;
    @Indexed(unique = true)
    private String agentId;
    private String advertiseIp;
    private Integer advertisePort;
    /** Process start timestamp in milliseconds. */
    private Long startTime;

    @Indexed
    private String status;
    /** Latest accepted status report timestamp in milliseconds. */
    @Indexed
    private Long statusReportTime;
    private Long pid;
    private Double cpuUsage;
    private Double memoryUsage;
    private String errorReason;

    /**
     * OAuth2 client id the Reader generated for itself and reported in {@code register}.
     *
     * <p>External callers (TM command forwarding, db2z-connector) use this with
     * {@link #clientSecretHash} against {@code /oauth/token} to obtain an access token for
     * {@code /api/db2-z/cdc/**}. The Reader owns this value; TM only records it.
     */
    private String clientId;

    /**
     * Hash of the client secret the Reader generated, used only to detect a changed secret on
     * re-register without decrypting.
     */
    private String clientSecretHash;

    /**
     * The client secret, reversible-encrypted, because TM must hand it back when an operator
     * configures a connection to this Reader. Never returned by the listing endpoints; only
     * {@code GET /api/agent-process/{id}/client-secret} reveals it.
     */
    private String clientSecretCipher;


    /** Latest registration timestamp in milliseconds. */
    private Long lastRegisterTime;

    public String getProxyProcessId() {
        ObjectId id = getId();
        return id == null ? null : id.toHexString();
    }
}
