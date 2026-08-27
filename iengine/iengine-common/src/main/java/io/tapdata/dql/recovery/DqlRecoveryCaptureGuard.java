package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;

import java.util.Optional;

/** Shared recovery marker detection used by processor and target capture. */
public final class DqlRecoveryCaptureGuard {
    private DqlRecoveryCaptureGuard() {
    }

    public static boolean isRecovery(TapdataEvent event) {
        return event != null && isRecoveryRecord(event.getTapEvent());
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
