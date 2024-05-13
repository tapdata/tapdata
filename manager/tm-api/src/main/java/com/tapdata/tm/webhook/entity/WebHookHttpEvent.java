package com.tapdata.tm.webhook.entity;

import lombok.Data;

@Data
public class WebHookHttpEvent {
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

    public WebHookHttpEvent withUrl(String url) {
        this.url = url;
        return this;
    }

    public WebHookHttpEvent withRequestHeard(String head) {
        this.requestHeard = head;
        return this;
    }

    public WebHookHttpEvent withRequestBody(String requestBody) {
        this.requestBody = requestBody;
        return this;
    }

    public WebHookHttpEvent withRequestParams(String requestParams) {
        this.requestParams = requestParams;
        return this;
    }

    public WebHookHttpEvent withRequestAt(long requestAt) {
        this.requestAt = requestAt;
        return this;
    }

    public WebHookHttpEvent withResponseHeard(String responseHeard) {
        this.responseHeard = responseHeard;
        return this;
    }

    public WebHookHttpEvent withResponseResult(String responseResult) {
        this.responseResult = responseResult;
        return this;
    }

    public WebHookHttpEvent withResponseStatus(String responseStatus) {
        this.responseStatus = responseStatus;
        return this;
    }

    public WebHookHttpEvent withResponseAt(long responseAt) {
        this.responseAt = responseAt;
        return this;
    }

    public WebHookHttpEvent withResponseCode(int responseCode) {
        this.responseCode = responseCode;
        return this;
    }

    public static WebHookHttpEvent of() {
        return new WebHookHttpEvent();
    }
}
