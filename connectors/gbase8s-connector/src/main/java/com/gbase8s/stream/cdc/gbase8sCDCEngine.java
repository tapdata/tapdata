package com.gbase8s.stream.cdc;

import com.informix.jdbc.IfxSmartBlob;
import com.informix.stream.cdc.IfxCDCRecordBuilder;
import io.tapdata.entity.logger.Log;

import java.sql.Connection;
import java.util.List;

/**
 * Author:Skeet
 * Date: 2023/6/25
 **/
public class gbase8sCDCEngine implements AutoCloseable {
    private Log logger;
    private IfxSmartBlob smartBlob;
    private final Connection con;
    private int sessionID;
    private final int bufferSize;
    private final byte[] buffer;
    private final IfxCDCRecordBuilder recordBuilder;
    private final int timeout;
    private final List<IfxCDCEngine.IfmxWatchedTable> capturedTables;
    private final boolean stopLoggingOnClose;
    private boolean inlineLOB;
    private boolean isClosed;
    private final long startingSequencePosition;
    private int fetchSize;

    public gbase8sCDCEngine(Log logger, IfxSmartBlob smartBlob, Connection con, int sessionID, int bufferSize, byte[] buffer, IfxCDCRecordBuilder recordBuilder, int timeout, List<IfxCDCEngine.IfmxWatchedTable> capturedTables, boolean stopLoggingOnClose, boolean inlineLOB, boolean isClosed, long startingSequencePosition, int fetchSize) {
        this.logger = logger;
        this.smartBlob = smartBlob;
        this.con = con;
        this.sessionID = sessionID;
        this.bufferSize = bufferSize;
        this.buffer = buffer;
        this.recordBuilder = recordBuilder;
        this.timeout = timeout;
        this.capturedTables = capturedTables;
        this.stopLoggingOnClose = stopLoggingOnClose;
        this.inlineLOB = inlineLOB;
        this.isClosed = isClosed;
        this.startingSequencePosition = startingSequencePosition;
        this.fetchSize = fetchSize;
    }


    public void close() throws Exception {

    }
}
