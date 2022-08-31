package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.ddl.index.*;
import lombok.Data;

import java.util.List;

/**
 * @author Dexter
 */
public class HandlerUtil {
    public static EventTypeRecorder countTapdataEvent(List<TapdataEvent> events) {
        EventTypeRecorder recorder = new EventTypeRecorder();
        for (TapdataEvent tapdataEvent : events) {
            // skip events like heartbeat
            if (null == tapdataEvent.getTapEvent()) {
                continue;
            }
            countEventType(tapdataEvent.getTapEvent(), recorder);
        }

        return recorder;
    }

    public static EventTypeRecorder countTapEvent(List<? extends TapEvent> events) {
        EventTypeRecorder recorder = new EventTypeRecorder();
        long timeCostTotal = 0, recordTotal = 0;
        for (TapEvent event : events) {
            countEventType(event, recorder);
            if (null != event.getTime()) {
                timeCostTotal += System.currentTimeMillis() - event.getTime();
                recordTotal += 1;
            }
        }

        long timeCostAvg = 0;
        if (recordTotal != 0) {
            timeCostAvg = timeCostTotal / recordTotal;
        }
        recorder.setAvgProcessTime(timeCostAvg);

        return recorder;
    }

    private static void countEventType(TapEvent event, EventTypeRecorder recorder) {
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

    @Data
    public static class EventTypeRecorder {
        private long ddlTotal;
        private long insertTotal;
        private long updateTotal;
        private long deleteTotal;
        private long othersTotal;
        private long avgProcessTime;

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

        public long getTotal() {
            return ddlTotal + insertTotal + updateTotal + deleteTotal + othersTotal;
        }
    }
}
