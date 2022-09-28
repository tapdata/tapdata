package io.tapdata.zoho.entity.weebHook;

import java.util.Map;

public class UpdateEventEntity extends EventBaseEntity {
    private Map<String,Object> prevState;
    public static UpdateEventEntity create(){
        return new UpdateEventEntity();
    }
    public UpdateEventEntity prevState(Map<String,Object> prevState){
        this.prevState = prevState;
        return this;
    }
    public Map<String,Object> prevState(){
        return this.prevState;
    }
}
