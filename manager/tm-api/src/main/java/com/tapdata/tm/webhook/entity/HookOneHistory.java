package com.tapdata.tm.webhook.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class HookOneHistory {
    String eventType;
    Date sendAt;
    Date responseAt;

    String requestId;
    String requestContent;
    String requestHeaders;

    String responseBody;
    String responseHeaders;
    int responseStatus;
    Map<String,Object> httpResponse;
    int status;
}
