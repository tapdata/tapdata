package com.tapdata.tm.webhook.vo;

import lombok.Data;

import java.util.Map;

@Data
public class WebHookHistoryInfoVo {
    String hookId;
    String url;
    String eventType;
    int status;

    String requestId;
    String requestHeard;
    String requestBody;
    String requestParams;
    long requestAt;

    String responseHeard;
    String responseResult;
    String responseStatus;
    int responseCode;
    long responseAt;
    Map<String,Object> httpResponse;
}
