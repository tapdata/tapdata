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

    private String token;
    /** Latest registration timestamp in milliseconds. */
    private Long lastRegisterTime;

    public String getProxyProcessId() {
        ObjectId id = getId();
        return id == null ? null : id.toHexString();
    }
}
