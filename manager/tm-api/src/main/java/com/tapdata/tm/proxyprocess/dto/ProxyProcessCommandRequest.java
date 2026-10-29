package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

@Data
public class ProxyProcessCommandRequest {
    private String proxyProcessId;
    private String type;
    private Object value;
}
