package com.tapdata.tm.webhook.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class HookOneHistory extends BaseEntity {
    String url;
    String requestHeard;
    String requestBody;
    String requestParams;
    long requestAt;

    String responseHeard;
    String responseResult;
    String responseStatus;
    int responseCode;
    long responseAt;


    String eventType;
    Map<String,Object> httpResponse;
    int status;
}
