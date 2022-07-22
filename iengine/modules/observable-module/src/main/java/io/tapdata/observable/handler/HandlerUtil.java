package io.tapdata.observable.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.ddl.index.*;
import lombok.Data;

import java.util.List;

/**
 * @author Dexter
 */
public class HandlerUtil {
    public static EventTypeRecorder countEventType(List<TapdataEvent> events) {
        EventTypeRecorder recorder = new EventTypeRecorder();
        for (TapdataEvent tapdataEvent : events) {
            int type = 0;
            if (tapdataEvent.getTapEvent() != null) {
                type = tapdataEvent.getTapEvent().getType();
            }
            switch (type) {
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

        return recorder;
    }

    @Data
    public static class EventTypeRecorder {
        private long ddlTotal;
        private long insertTotal;
        private long updateTotal;
        private long deleteTotal;
        private long othersTotal;

        public void incrDdlTotal() {
            this.ddlTotal += 1;
        }

        public void incrInsertTotal() {
            this.ddlTotal += 1;
        }

        public void incrUpdateTotal() {
            this.ddlTotal += 1;
        }

        public void incrDeleteTotal() {
            this.ddlTotal += 1;
        }
        public void incrOthersTotal() {
            this.ddlTotal += 1;
        }

        public long getTotal() {
            return ddlTotal + insertTotal + updateTotal + deleteTotal + othersTotal;
        }
    }
}
