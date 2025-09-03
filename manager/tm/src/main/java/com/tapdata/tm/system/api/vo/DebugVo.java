package com.tapdata.tm.system.api.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/11 17:39 Create
 * @description
 */
@Data
public class DebugVo {
    Integer httpCode;

    List<Map<String, Object>> data;

    Integer count;

    @JsonProperty("api_monit_info")
    Map<String, Object> apiMonitInfo;

    Map<String, Object> error;

    public static DebugVo error(String msg) {
        DebugVo error = new DebugVo();
        error.setError(new HashMap<>());
        error.getError().put("statusCode", "400");
        error.getError().put("name", "BadRequestError");
        error.getError().put("message", msg);
        error.getError().put("code", "INVALID_BODY_VALUE");
        return error;
    }
}
