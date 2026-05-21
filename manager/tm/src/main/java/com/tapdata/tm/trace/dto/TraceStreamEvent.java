package com.tapdata.tm.trace.dto;

import com.tapdata.tm.trace.enums.TraceEventType;
import lombok.Data;

@Data
public class TraceStreamEvent {

    private TraceEventType type;
    private String requestId;
    private String nodeId;
    private String connectionId;
    private String connectionName;
    private String table;
    private TraceValue traceValue;
    private TraceNodeError error;

    public static TraceStreamEvent traceValue(String requestId, String nodeId, String connectionId,
                                              String connectionName, String table, TraceValue traceValue) {
        TraceStreamEvent event = new TraceStreamEvent();
        event.setType(TraceEventType.TRACE_VALUE);
        event.setRequestId(requestId);
        event.setNodeId(nodeId);
        event.setConnectionId(connectionId);
        event.setConnectionName(connectionName);
        event.setTable(table);
        event.setTraceValue(traceValue);
        return event;
    }

    public static TraceStreamEvent nodeError(String requestId, String nodeId, String connectionId,
                                             String connectionName, String table, TraceNodeError error) {
        TraceStreamEvent event = new TraceStreamEvent();
        event.setType(TraceEventType.NODE_ERROR);
        event.setRequestId(requestId);
        event.setNodeId(nodeId);
        event.setConnectionId(connectionId);
        event.setConnectionName(connectionName);
        event.setTable(table);
        event.setError(error);
        return event;
    }

    public static TraceStreamEvent complete(String requestId) {
        TraceStreamEvent event = new TraceStreamEvent();
        event.setType(TraceEventType.COMPLETE);
        event.setRequestId(requestId);
        return event;
    }
}
