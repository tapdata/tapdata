package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

import java.util.Date;

@Data
public class AgentProcessDto {
    private String proxyProcessId;
    private String agentName;
    private String agentType;
    private String agentId;
    private String advertiseIp;
    private Integer advertisePort;
    private Date startTime;
    private String status;
    private Date statusReportTime;
    private Long pid;
    private Double cpuUsage;
    private Double memoryUsage;
    private String errorReason;
    private Date lastRegisterTime;
    /** OAuth2 client id the Reader reported. */
    private String clientId;
    /**
     * OAuth2 client secret the Reader reported.
     *
     * <p>It travels with the listing because that is how a connection learns
     * everything it needs to obtain an access token: the selection UI copies the
     * whole object into `proxyProcessInfo`, so one field left out means the
     * connection is configured without it. It is the Reader's own credential,
     * handed to whoever may already manage the Reader.
     */
    private String clientSecret;
}
