package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.ddl.index.*;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.Data;

import java.util.List;

/**
 * @author Dexter
 */
public class HandlerUtil {
    public static EventTypeRecorder countTapdataEvent(List<TapdataEvent> events) {
        long now = System.currentTimeMillis();

        EventTypeRecorder recorder = new EventTypeRecorder();
        for (TapdataEvent tapdataEvent : events) {
            // skip events like heartbeat
            if (null == tapdataEvent.getTapEvent()) {
                if (tapdataEvent instanceof TapdataHeartbeatEvent) {
                    setEventTimestamp(recorder, tapdataEvent.getSourceTime());
                }
                continue;
            }
            Long referenceTime = countEventTypeAndGetReferenceTime(tapdataEvent.getTapEvent(), recorder);
            CommonUtils.ignoreAnyError(() -> recorder.incrReplicateLagTotal(now, referenceTime), "HandlerUtil-countTapdataEvent");
        }

        return recorder;
    }

    public static EventTypeRecorder countTapEvent(List<? extends TapEvent> events) {
        long now = System.currentTimeMillis();

        Long referenceTime;
        EventTypeRecorder recorder = new EventTypeRecorder();
        for (TapEvent tapEvent : events) {
            referenceTime = countEventTypeAndGetReferenceTime(tapEvent, recorder);
            recorder.incrProcessTimeTotal(now, tapEvent.getTime());
            recorder.incrReplicateLagTotal(now, referenceTime);
        }

        return recorder;
    }

    private static Long countEventTypeAndGetReferenceTime(TapEvent event, EventTypeRecorder recorder) {
        Long ts;
        if (event instanceof HeartbeatEvent) {
            ts = ((HeartbeatEvent) event).getReferenceTime();
        } else {
            ts = ((TapBaseEvent) event).getReferenceTime();

            switch (event.getType()) {
                case TapInsertRecordEvent.TYPE:
                    recorder.incrInsertTotal();
                    break;
                case TapDeleteRecordEvent.TYPE:
                    recorder.incrDeleteTotal();
                    break;
                case TapUpdateRecordEvent.TYPE:
                    recorder.incrUpdateTotal();
                    break;
                case TapDeleteIndexEvent.TYPE:
                case TapCreateIndexEvent.TYPE:
                case TapAlterDatabaseTimezoneEvent.TYPE:
                case TapAlterFieldAttributesEvent.TYPE:
                case TapAlterFieldNameEvent.TYPE:
                case TapAlterFieldPrimaryKeyEvent.TYPE:
                case TapAlterTableCharsetEvent.TYPE:
                case TapClearTableEvent.TYPE:
                case TapCreateTableEvent.TYPE:
                case TapDropFieldEvent.TYPE:
                case TapDropTableEvent.TYPE:
                case TapNewFieldEvent.TYPE:
                case TapRenameTableEvent.TYPE:
                    recorder.incrDdlTotal();
                    break;
                default:
                    recorder.incrOthersTotal();
            }
        }
        setEventTimestamp(recorder, ts);
        return ts;
    }

    private static void setEventTimestamp(EventTypeRecorder recorder, Long ts) {
        if (null != ts) {
            if (null == recorder.getNewestEventTimestamp() || ts > recorder.getNewestEventTimestamp()) {
                recorder.setNewestEventTimestamp(ts);
            }
            if (null == recorder.getOldestEventTimestamp() || ts < recorder.getOldestEventTimestamp()) {
                recorder.setOldestEventTimestamp(ts);
            }
        }
    }

    @Data
    public static class EventTypeRecorder {
        private long ddlTotal;
        private long insertTotal;
        private long updateTotal;
        private long deleteTotal;
        private long othersTotal;
        private Long processTimeTotal;
        private Long replicateLagTotal;
        private Long oldestEventTimestamp;
        private Long newestEventTimestamp;

        public void incrDdlTotal() {
            this.ddlTotal += 1;
        }

        public void incrInsertTotal() {
            this.insertTotal += 1;
        }

        public void incrUpdateTotal() {
            this.updateTotal+= 1;
        }

        public void incrDeleteTotal() {
            this.deleteTotal += 1;
        }
        public void incrOthersTotal() {
            this.othersTotal += 1;
        }

        public void incrProcessTimeTotal(Long now, Long time) {
            if (null == time) return;
            if (null == processTimeTotal) {
                processTimeTotal = 0L;
            }
            processTimeTotal += (now - time);
        }

        public void incrReplicateLagTotal(Long now, Long replicateLag) {
            if (null == replicateLag) return;
            if (null == replicateLagTotal) {
                replicateLagTotal = 0L;
            }
            replicateLagTotal += (now - replicateLag);
        }



        public long getTotal() {
            return ddlTotal + insertTotal + updateTotal + deleteTotal + othersTotal;
        }
    }
}
