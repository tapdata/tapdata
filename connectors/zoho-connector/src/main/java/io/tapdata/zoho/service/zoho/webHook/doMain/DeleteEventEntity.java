package io.tapdata.zoho.service.zoho.webHook.doMain;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.zoho.service.zoho.webHook.EventBaseEntity;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.connectionMode.impl.CSVMode;

import java.util.Map;

public class DeleteEventEntity extends EventBaseEntity<DeleteEventEntity> {
    public static DeleteEventEntity create(){
        return new DeleteEventEntity();
    }

    @Override
    protected DeleteEventEntity event(Map<String, Object> issueEventData) {
        super.config(issueEventData);
        return this;
    }

    @Override
    public TapEvent outputTapEvent(String table, ConnectionMode instance) {
        return TapSimplify.deleteDMLEvent(
                instance instanceof CSVMode ? instance.attributeAssignmentSelf(this.payload(),table):this.payload()
                , table)
                .referenceTime(this.eventTime());
    }

    @Override
    public String tapEventType() {
        return "DeleteEvent";
    }
}
