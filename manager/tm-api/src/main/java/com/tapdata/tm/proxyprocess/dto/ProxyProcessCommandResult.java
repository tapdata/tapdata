package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ProxyProcessCommandResult {
    private int statusCode;
    private Map<String, List<String>> headers;
    private String body;
}
