package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

/** OracleLogParser-specific response fields. */
@Data
@EqualsAndHashCode(callSuper = true)
public class OracleLogParserAgentProcessDto extends AgentProcessDto {
    private String sourceServiceId;
    private String sourceState;
}
