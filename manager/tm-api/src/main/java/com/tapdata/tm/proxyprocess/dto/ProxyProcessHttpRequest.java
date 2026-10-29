package com.tapdata.tm.proxyprocess.dto;

import lombok.Data;

import java.util.Map;

@Data
public class ProxyProcessHttpRequest {
    private String method;
    private String path;
    private Map<String, Object> query;
    private Object body;
}
