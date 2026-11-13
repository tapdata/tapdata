package com.tapdata.tm.proxyprocess.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;

/** OracleLogParser-specific fields persisted in the shared AgentProcess collection. */
@Data
@EqualsAndHashCode(callSuper = true)
@Document("AgentProcess")
@TypeAlias("oracleLogParserAgentProcess")
public class OracleLogParserAgentProcessEntity extends AgentProcessEntity {
    private String sourceServiceId;
    private String sourceState;
}
