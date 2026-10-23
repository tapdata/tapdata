package com.tapdata.tm.ds.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ConnectionAgentCheckRequest extends BatchUpdateAgentSettingsRequest {
    private int skip;
    private int limit = 20;
}
