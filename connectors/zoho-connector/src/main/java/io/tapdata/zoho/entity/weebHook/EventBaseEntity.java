package io.tapdata.zoho.entity.weebHook;

import io.tapdata.zoho.enums.WebHookEvent;

import java.util.Map;

public class EventBaseEntity {
    private Long eventTime;
    private String eventType;
    private WebHookEvent event;
    private Long orgId;
    private Map<String,Object> payload;
    public EventBaseEntity payload(Map<String,Object> payload){
        this.payload = payload;
        return this;
    }
    public Map<String,Object> payload(){
        return this.payload;
    }
    public EventBaseEntity orgId(Long orgId){
        this.orgId = orgId;
        return this;
    }
    public Long orgId(){
        return this.orgId;
    }
    public EventBaseEntity eventTime(Long eventTime){
        this.eventTime = eventTime;
        return this;
    }
    public Long eventTime(){
        return this.eventTime;
    }
    public EventBaseEntity eventType(String eventType){
        this.event = WebHookEvent.event(eventType);
        this.eventType = eventType;
        return this;
    }
    public String eventType(){
        return this.eventType;
    }
    public WebHookEvent event(){
        return this.event;
    }
}
