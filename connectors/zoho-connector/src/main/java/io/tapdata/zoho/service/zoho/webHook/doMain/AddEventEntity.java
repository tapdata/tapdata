package io.tapdata.zoho.service.zoho.webHook.doMain;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.zoho.service.zoho.webHook.EventBaseEntity;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.connectionMode.impl.CSVMode;

import java.util.Map;

public class AddEventEntity extends EventBaseEntity<AddEventEntity> {
    public AddEventEntity create(){
        return new AddEventEntity();
    }

    @Override
    protected AddEventEntity event(Map<String, Object> issueEventData) {
        super.config(issueEventData);
        return this;
    }

    @Override
    public TapEvent outputTapEvent(String table, ConnectionMode instance) {
        return TapSimplify.insertRecordEvent(
                instance instanceof CSVMode ? instance.attributeAssignmentSelf(this.payload(),table):this.payload()
                ,table)
                .referenceTime(this.eventTime());
    }

    @Override
    public String tapEventType() {
        return "AddEvent";
    }
}
