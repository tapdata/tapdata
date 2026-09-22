package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.Map;
import java.util.Optional;

/** Shared recovery marker detection used by processor and target capture. */
public final class DqlRecoveryCaptureGuard {
    /** Internal marker for the temporary task namespace used by replay. */
    public static final String TASK_ATTR_RECOVERY_RUNTIME = "__dql_recovery_runtime";
    private static final String STATE_MAP_NAMESPACE_PREFIX = "DQL_RECOVERY";

    private DqlRecoveryCaptureGuard() {
    }

    public static boolean isRecovery(TapdataEvent event) {
        return event != null && isRecoveryRecord(event.getTapEvent());
    }

    public static boolean isRecoveryTask(TaskDto task) {
        return task != null && isRecoveryTask(task.getAttrs());
    }

    public static boolean isRecoveryTask(Map<String, Object> attrs) {
        return attrs != null && Boolean.TRUE.equals(attrs.get(TASK_ATTR_RECOVERY_RUNTIME));
    }

    /**
     * Returns the physical state-map namespace for a temporary replay task.
     * It is derived from the temporary task id so concurrent replays cannot
     * share connector state.
     */
    public static String stateMapNamespace(TaskDto task) {
        if (task == null || task.getId() == null) {
            throw new IllegalArgumentException("Recovery task id is required");
        }
        return String.join("_", STATE_MAP_NAMESPACE_PREFIX, task.getId().toHexString());
    }

    public static boolean isRecoveryRecord(TapEvent event) {
        return TapdataDqlRecoveryEvent.isRecoveryEvent(event);
    }

    public static Optional<String> eventId(TapEvent event) {
        if (event == null || event.getInfo() == null) {
            return Optional.empty();
        }
        Object eventId = event.getInfo().get(TapdataDqlRecoveryEvent.INFO_KEY_DQL_EVENT_ID);
        return eventId == null ? Optional.empty() : Optional.of(String.valueOf(eventId));
    }

    public static boolean notifyFailure(TapdataEvent event, Throwable failure) {
        return event != null && notifyFailure(event.getTapEvent(), failure);
    }

    public static boolean notifyFailure(TapRecordEvent event, Throwable failure) {
        return notifyFailure((TapEvent) event, failure);
    }

    private static boolean notifyFailure(TapEvent event, Throwable failure) {
        if (!isRecoveryRecord(event)) {
            return false;
        }
        return eventId(event).map(id -> DqlRecoveryFailureRegistry.fail(id, failure)).orElse(false);
    }
}
