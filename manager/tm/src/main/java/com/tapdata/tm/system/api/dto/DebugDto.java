package com.tapdata.tm.system.api.dto;

import lombok.Data;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/11 17:13 Create
 * @description
 */
@Data
public class DebugDto {
    String apiId;
    String url;
    String method;
    Map<String, Object> body;
    Map<String, String> headers;
    Map<String, Object> params;
}
