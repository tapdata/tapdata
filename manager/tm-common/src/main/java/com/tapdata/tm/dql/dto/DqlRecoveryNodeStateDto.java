package com.tapdata.tm.dql.dto;

import lombok.Data;

import java.io.Serializable;

/** TM representation of a node temporarily hidden during DQL replay. */
@Data
public class DqlRecoveryNodeStateDto implements Serializable {
    private String nodeId;
    private String nodeName;
    private boolean disabledBefore;
    private boolean disabledDuring;
    private boolean restored;
    private String restoreMessage;
}
