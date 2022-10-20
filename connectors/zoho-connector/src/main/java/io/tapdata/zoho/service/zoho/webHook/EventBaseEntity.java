package io.tapdata.zoho.service.zoho.webHook;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.utils.Checker;

import java.util.Map;

public abstract class EventBaseEntity<T> {
    private static final String TAG = EventBaseEntity.class.getSimpleName();
    private Long eventTime;
    private String eventType;
    private WebHookEvent event;
    private Long orgId;
    private Map<String,Object> payload;

    protected void config(Map<String, Object> issueEventData){
        if (Checker.isEmpty(issueEventData)) return;
        Object eventTime = issueEventData.get("eventTime");
        this.eventTime(Checker.isEmpty(eventTime)?0:Long.parseLong(String.valueOf(eventTime)));

        Object eventType = issueEventData.get("eventType");
        this.eventType(null == eventType?null:(String)eventType);

        Object orgId = issueEventData.get("orgId");
        this.orgId(null == orgId?0:Long.parseLong(String.valueOf(orgId)));

        Object payload = issueEventData.get("payload");
        this.payload(null == payload?null:(Map<String,Object>)payload);
    }

    /**
     * 外部调用者使用此方法根据事件类型获取子类
     * */
    public static EventBaseEntity getInstanceByEventType(Map<String, Object> issueEventData){
        Class clz = null;
        try {
            if (Checker.isEmpty(issueEventData)) return null;
            Object typeObj = issueEventData.get("eventType");
            if (Checker.isEmpty(typeObj)) return null;
            WebHookEvent event = WebHookEvent.event((String) typeObj);
            if (Checker.isEmpty(event)) return null;
            clz = Class.forName(EventBaseEntity.class.getPackage().getName()+".doMain."+event.getTapEvent()+"Entity");
            return (EventBaseEntity) ((EventBaseEntity)clz.newInstance()).event(issueEventData);
        } catch (ClassNotFoundException e) {
            TapLogger.debug(TAG,"Class not found | EventBaseEntity");
        } catch (InstantiationException e1) {
            TapLogger.debug(TAG,"Instantiation | EventBaseEntity");
        } catch (IllegalAccessException e2) {
            TapLogger.debug(TAG,"Illegal access | EventBaseEntity");
        }
        return null;
    }
    /**
     * 子类实现了此方法就按自己的类型返回自己
     * */
    protected abstract T event(Map<String, Object> issueEventData);
    /**
     * 子类实现了此方法按自己类型输出对应得TapEvent事件
     * */
    public abstract TapEvent outputTapEvent(String table, ConnectionMode instance);

    public abstract String tapEventType();


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
    public Long getEventTime() {
        return eventTime;
    }
    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
    public String getEventType() {
        return eventType;
    }
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    public WebHookEvent getEvent() {
        return event;
    }
    public void setEvent(WebHookEvent event) {
        this.event = event;
    }
    public Long getOrgId() {
        return orgId;
    }
    public void setOrgId(Long orgId) {
        this.orgId = orgId;
    }
    public Map<String, Object> getPayload() {
        return payload;
    }
    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }
}
