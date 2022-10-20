package io.tapdata.zoho.service.zoho.webHook.doMain;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.zoho.service.zoho.webHook.EventBaseEntity;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.connectionMode.impl.CSVMode;

import java.util.Map;

public class UpdateEventEntity extends EventBaseEntity<UpdateEventEntity> {
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

    @Override
    protected UpdateEventEntity event(Map<String, Object> issueEventData) {
        super.config(issueEventData);
        Object prevState = issueEventData.get("prevState");
        this.prevState(null == prevState?null:(Map<String,Object>)prevState);
        return this;
    }

    @Override
    public TapEvent outputTapEvent(String table, ConnectionMode instance) {
        return TapSimplify.updateDMLEvent(
                instance instanceof CSVMode ? instance.attributeAssignmentSelf(this.prevState,table):this.prevState,
                instance instanceof CSVMode ? instance.attributeAssignmentSelf(this.payload(),table):this.payload(),
                table)
                .referenceTime(this.eventTime());
    }


    @Override
    public String tapEventType() {
        return "UpdateEvent";
    }

    public Map<String, Object> getPrevState() {
        return prevState;
    }

    public void setPrevState(Map<String, Object> prevState) {
        this.prevState = prevState;
    }
}
