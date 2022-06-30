package io.tapdata.pdk.core.workflow.engine.driver;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.event.control.PatrolEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.core.api.TargetNode;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class TableTapEventHandler {
    private static final String TAG = TableTapEventHandler.class.getSimpleName();
    private List<TapRecordEvent> recordEvents = new ArrayList<>();
    private List<ControlEvent> controlEvents = new ArrayList<>();
    private String tableId;
    private TargetNode targetNode;

    public TableTapEventHandler(TargetNode targetNode) {
        this.targetNode = targetNode;
    }

    private Consumer<List<TapRecordEvent>> recordEventsConsumer;
    public TableTapEventHandler recordEventsConsumer(Consumer<List<TapRecordEvent>> recordEventsConsumer) {
        this.recordEventsConsumer = recordEventsConsumer;
        return this;
    }
    private Consumer<List<ControlEvent>> controlEventsConsumer;
    public TableTapEventHandler controlEventsConsumer(Consumer<List<ControlEvent>> controlEventsConsumer) {
        this.controlEventsConsumer = controlEventsConsumer;
        return this;
    }

    private Consumer<TapDDLEvent> ddlEventsConsumer;
    public TableTapEventHandler ddlEventsConsumer(Consumer<TapDDLEvent> ddlEventsConsumer) {
        this.ddlEventsConsumer = ddlEventsConsumer;
        return this;
    }

    private Function<TapRecordEvent, TapRecordEvent> filterEventFunction;
    public TableTapEventHandler filterEventConsumer(Function<TapRecordEvent, TapRecordEvent> filterEventConsumer) {
        this.filterEventFunction = filterEventConsumer;
        return this;
    }

    public void handle(TapEvent event) {
        if(event instanceof TapDDLEvent) {
            //force to handle DML before handle DDL.
            recordEventsConsumer.accept(recordEvents);
            controlEventsConsumer.accept(controlEvents);
            //handle ddl events
            ddlEventsConsumer.accept((TapDDLEvent) event);
        } else if(event instanceof TapRecordEvent) {
            recordEvents.add(filterEventFunction.apply((TapRecordEvent) event));
        } else if(event instanceof ControlEvent) {
            if(event instanceof PatrolEvent) {
                PatrolEvent patrolEvent = (PatrolEvent) event;
                if(patrolEvent.applyState(targetNode.getAssociateId(), PatrolEvent.STATE_ENTER)) {
                    if(patrolEvent.getPatrolListener() != null) {
                        CommonUtils.ignoreAnyError(() -> patrolEvent.getPatrolListener().patrol(targetNode.getAssociateId(), PatrolEvent.STATE_ENTER), TAG);
                    }
                }
            }
            controlEvents.add((ControlEvent) event);
        }

        recordEventsConsumer.accept(recordEvents);
        controlEventsConsumer.accept(controlEvents);
    }

}
