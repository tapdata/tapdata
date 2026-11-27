package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

/**
 * The client secret a CDC Reader generated, returned only by the explicit reveal endpoint.
 *
 * <p>Not part of {@link AgentProcessDto} on purpose: a listing must never carry secrets.
 */
@Data
public class AgentProcessClientSecretDto {
    private String proxyProcessId;
    private String clientSecret;
}
