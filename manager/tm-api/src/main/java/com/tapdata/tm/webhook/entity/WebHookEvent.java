package com.tapdata.tm.webhook.entity;

import java.util.List;

public class WebHookEvent {
    String type;
    String metric;
    Object event;
    List<String> userId;
    private String title;
    private String content;

    public static WebHookEvent of() {
        return new WebHookEvent();
    }

    public WebHookEvent withType(String type) {
        this.type = type;
        return this;
    }
    public WebHookEvent withMetric(String metric) {
        this.metric = metric;
        return this;
    }

    public WebHookEvent withEvent(Object event) {
        this.event = event;
        return this;
    }

    public WebHookEvent withUserId(List<String> userIds) {
        this.userId = userIds;
        return this;
    }

    public WebHookEvent withTitle(String title) {
        this.title = title;
        return this;
    }

    public WebHookEvent withContent(String content) {
        this.content = content;
        return this;
    }

    public String getMetric() {
        return metric;
    }

    public String getType() {
        return type;
    }

    public Object getEvent() {
        return event;
    }

    public List<String> getUserId() {
        return userId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
