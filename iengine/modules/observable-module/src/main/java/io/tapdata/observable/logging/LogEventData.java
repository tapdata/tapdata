package io.tapdata.observable.logging;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import lombok.*;
import lombok.extern.jackson.Jacksonized;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Dexter
 */
@Jacksonized
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class LogEventData {
    public static final String LOG_EVENT_TYPE_RECEIVE = "receive";
    public static final String LOG_EVENT_TYPE_PROCESS = "process";
    public static final String LOG_EVENT_TYPE_SEND = "send";

    public static final String LOG_EVENT_STATUS_OK = "OK";
    public static final String LOG_EVENT_STATUS_WARN = "WARN";
    public static final String LOG_EVENT_STATUS_ERROR = "ERROR";


    private String eventId;
    private String eventType;
    private Long time;
    private Long cost;
    private Long delay;
    private String status;
    private String message;

    @Singular("property")
    private Map<String, Object> data;

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        if (eventId != null) {
            map.put("eventId", eventId);
        }
        if (eventType != null) {
            map.put("eventType", eventType);
        }
        if (time != null) {
            map.put("time", time);
        }
        if (cost != null) {
            map.put("cost", cost);
        }
        if (delay != null) {
            map.put("delay", delay);
        }
        if (status != null) {
            map.put("status", status);
        }
        if (message != null) {
            map.put("message", message);
        }
        if (data != null) {
            map.put("data", data);
        }

        return map;
    }

    public static class LogEventDataBuilder {

        public LogEventDataBuilder withNode(Node<?> node) {
            if (node instanceof DataParentNode) {
                DataParentNode<?> dataParentNode = (DataParentNode<?>) node;
                this.property("connectionId", dataParentNode.getConnectionId());
                this.property("databaseType", dataParentNode.getDatabaseType());
                this.property("connectionName", dataParentNode.getName());
            }

            return this;
        }

        public LogEventDataBuilder withTapEvent(TapEvent event) {
            if (null == event) {
                return this;
            }

            Map<String, Object> before = null;
            Map<String, Object> after = null;
            String table, message = null;
            Long delay = null;
            Object ddl = null;
            switch (event.getType()) {
                case TapInsertRecordEvent.TYPE:
                    TapInsertRecordEvent insert = (TapInsertRecordEvent) event;

                    after = insert.getAfter();
                    table = insert.getTableId();
                    message = "insert record event";
                    if (null != insert.getReferenceTime()) {
                        delay = System.currentTimeMillis() - insert.getReferenceTime();
                    }
                    break;
                case TapDeleteRecordEvent.TYPE:
                    TapDeleteRecordEvent delete = (TapDeleteRecordEvent) event;

                    before = delete.getBefore();
                    table = delete.getTableId();
                    message = "delete record event";
                    if (null != delete.getReferenceTime()) {
                        delay = System.currentTimeMillis() - delete.getReferenceTime();
                    }
                    break;
                case TapUpdateRecordEvent.TYPE:
                    TapUpdateRecordEvent update = (TapUpdateRecordEvent) event;

                    after = update.getAfter();
                    before = update.getBefore();
                    table = update.getTableId();
                    message = "update record event";
                    if (null != update.getReferenceTime()) {
                        delay = System.currentTimeMillis() - update.getReferenceTime();
                    }
                    break;
                case TapCreateTableEvent.TYPE:
                    TapCreateTableEvent createTable = (TapCreateTableEvent) event;

                    ddl = createTable.getOriginDDL();
                    table = createTable.getTableId();
                    message = "create table event";
                    if (null != createTable.getReferenceTime()) {
                        delay = System.currentTimeMillis() - createTable.getReferenceTime();
                    }
                    break;
                case TapCreateIndexEvent.TYPE:
                    TapCreateIndexEvent createIndex = (TapCreateIndexEvent) event;

                    ddl = createIndex.getOriginDDL();
                    table = createIndex.getTableId();
                    message = "create index event";
                    if (null != createIndex.getReferenceTime()) {
                        delay = System.currentTimeMillis() - createIndex.getReferenceTime();
                    }
                    break;
                default:
                    return this;
            }

            this.eventId((String) event.getInfo().get("eventId")).message(message);
            if (null != delay) {
                this.delay(delay);
            }

            this.property("table", table);
            if (before != null) {
                this.property("before", before);
            }
            if (after != null) {
                this.property("after", after);
            }
            if (ddl != null) {
                this.property("ddl", ddl);
            }
            return this;
        }
    }
}
