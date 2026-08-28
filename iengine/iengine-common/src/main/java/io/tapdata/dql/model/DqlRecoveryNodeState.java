package io.tapdata.dql.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

/** Engine/TM contract describing a node temporarily hidden for DQL replay. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DqlRecoveryNodeState(String nodeId,
                                   String nodeName,
                                   boolean disabledBefore,
                                   boolean disabledDuring,
                                   boolean restored,
                                   String restoreMessage) implements Serializable {
}
