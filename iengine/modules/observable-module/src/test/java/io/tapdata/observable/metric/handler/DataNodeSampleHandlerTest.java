package io.tapdata.observable.metric.handler;

import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.NumberSampler;
import io.tapdata.common.sample.sampler.ResetSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.utils.UnitTestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DataNodeSampleHandlerTest {
    DataNodeSampleHandler handler;
    HandlerUtil.EventTypeRecorder recorder;
    Logger logger;

    @BeforeEach
    void init() {
        handler = mock(DataNodeSampleHandler.class);
        logger = LogManager.getLogger(DataNodeSampleHandler.class);
        UnitTestUtils.injectField(DataNodeSampleHandler.class, handler, "logger", logger);
        recorder = new HandlerUtil.EventTypeRecorder();


        handler.inputSizeSpeed = mock(SpeedSampler.class);
        handler.inputSpeed = mock(SpeedSampler.class);
        handler.inputDdlCounter = mock(CounterSampler.class);
        handler.inputInsertCounter = mock(CounterSampler.class);
        handler.inputUpdateCounter = mock(CounterSampler.class);
        handler.inputDeleteCounter = mock(CounterSampler.class);
        handler.inputOthersCounter = mock(CounterSampler.class);

        doNothing().when(handler.inputSizeSpeed).add(anyLong());
        doNothing().when(handler.inputSpeed).add(anyLong());
        doNothing().when(handler.inputDdlCounter).inc(anyLong());
        doNothing().when(handler.inputInsertCounter).inc(anyLong());
        doNothing().when(handler.inputUpdateCounter).inc(anyLong());
        doNothing().when(handler.inputDeleteCounter).inc(anyLong());
        doNothing().when(handler.inputOthersCounter).inc(anyLong());


        handler.outputSizeSpeed = mock(SpeedSampler.class);
        handler.outputSpeed = mock(SpeedSampler.class);
        handler.outputDdlCounter = mock(CounterSampler.class);
        handler.outputInsertCounter = mock(CounterSampler.class);
        handler.outputUpdateCounter = mock(CounterSampler.class);
        handler.outputDeleteCounter = mock(CounterSampler.class);
        handler.outputOthersCounter = mock(CounterSampler.class);
        handler.currentEventTimestamp = mock(NumberSampler.class);
        handler.replicateLag = mock(ResetSampler.class);
        handler.lastCapturedIncrementalEventAt = mock(NumberSampler.class);
        handler.lastEnqueuedIncrementalEventAt = mock(NumberSampler.class);
        handler.pendingIncrementalEvent = mock(NumberSampler.class);
        handler.sourceIncrementalMonitorStartAt = mock(NumberSampler.class);
        ReflectionTestUtils.setField(handler, "incrementalSourceReadTimeCostAvg", mock(AverageSampler.class));

        doNothing().when(handler.outputSizeSpeed).add(anyLong());
        doNothing().when(handler.outputSpeed).add(anyLong());
        doNothing().when(handler.outputDdlCounter).inc(anyLong());
        doNothing().when(handler.outputInsertCounter).inc(anyLong());
        doNothing().when(handler.outputUpdateCounter).inc(anyLong());
        doNothing().when(handler.outputDeleteCounter).inc(anyLong());
        doNothing().when(handler.outputOthersCounter).inc(anyLong());
        doNothing().when(handler.currentEventTimestamp).setValue(anyLong());
        doNothing().when(handler.replicateLag).setValue(anyLong());
        doNothing().when(handler.lastCapturedIncrementalEventAt).setValue(anyLong());
        doNothing().when(handler.lastEnqueuedIncrementalEventAt).setValue(anyLong());
        doNothing().when(handler.pendingIncrementalEvent).setValue(anyInt());
        doNothing().when(handler.sourceIncrementalMonitorStartAt).setValue(anyLong());
        doNothing().when(((AverageSampler) ReflectionTestUtils.getField(handler, "incrementalSourceReadTimeCostAvg"))).add(anyLong(), anyLong());
    }

    @Nested
    class HandleStreamReadProcessCompleteTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(handler).handleStreamReadProcessComplete(anyLong(), any(HandlerUtil.EventTypeRecorder.class));
        }

        @Test
        void handleStreamReadProcessCompleteNormal() {
            long time = System.currentTimeMillis();
            handler.handleStreamReadProcessComplete(time, recorder);

            verify(handler.outputSizeSpeed, times(1)).add(recorder.getMemorySize());
            verify(handler.outputSpeed, times(1)).add(recorder.getTotal());
            verify(handler.outputDdlCounter, times(1)).inc(recorder.getDdlTotal());
            verify(handler.outputInsertCounter, times(1)).inc(recorder.getInsertTotal());
            verify(handler.outputUpdateCounter, times(1)).inc(recorder.getUpdateTotal());
            verify(handler.outputDeleteCounter, times(1)).inc(recorder.getDdlTotal());
            verify(handler.outputOthersCounter, times(1)).inc(recorder.getOthersTotal());
            verify(handler.currentEventTimestamp, times(1)).setValue(recorder.getNewestEventTimestamp());
            verify(handler.replicateLag, times(1)).setValue(recorder.getReplicateLagTotal());
        }
    }

    @Nested
    class HandleStreamReadReadCompleteTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(handler).handleStreamReadReadComplete(anyLong(), any(HandlerUtil.EventTypeRecorder.class));
        }

        @Test
        void handleStreamReadProcessCompleteNormal() {
            recorder.incrInsertTotal();
            recorder.setNewestEventTimestamp(System.currentTimeMillis());
            long time = System.currentTimeMillis();
            handler.handleStreamReadReadComplete(time, recorder);

            verify(handler.inputSizeSpeed, times(1)).add(recorder.getMemorySize());
            verify(handler.inputSpeed, times(1)).add(recorder.getTotal());
            verify(handler.inputDdlCounter, times(1)).inc(recorder.getDdlTotal());
            verify(handler.inputInsertCounter, times(1)).inc(recorder.getInsertTotal());
            verify(handler.inputUpdateCounter, times(1)).inc(recorder.getUpdateTotal());
            verify(handler.inputDeleteCounter, times(1)).inc(recorder.getDdlTotal());
            verify(handler.inputOthersCounter, times(1)).inc(recorder.getOthersTotal());
            verify(handler.lastCapturedIncrementalEventAt, times(1)).setValue(time);
            verify(handler.pendingIncrementalEvent, times(1)).setValue(1);
        }

        @Test
        void handleStreamReadProcessCompleteHeartbeatOnly() {
            recorder.setNewestEventTimestamp(System.currentTimeMillis());
            long time = System.currentTimeMillis();
            handler.handleStreamReadReadComplete(time, recorder);

            verify(handler.lastCapturedIncrementalEventAt, times(1)).setValue(time);
            verify(handler.pendingIncrementalEvent, times(1)).setValue(1);
        }
    }

    @Nested
    class HandleWriteRecordStartTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(handler).handleWriteRecordStart(anyLong(), any(HandlerUtil.EventTypeRecorder.class));
        }

        @Test
        void testHandleWriteRecordStart() {
            long time = System.currentTimeMillis();
            handler.handleWriteRecordStart(time, recorder);

            verify(handler.inputSizeSpeed, times(1)).add(recorder.getMemorySize());
            verify(handler.inputSpeed, times(1)).add(recorder.getTotal());
            verify(handler.inputDdlCounter, times(1)).inc(recorder.getDdlTotal());
            verify(handler.inputInsertCounter, times(1)).inc(recorder.getInsertTotal());
            verify(handler.inputUpdateCounter, times(1)).inc(recorder.getUpdateTotal());
            verify(handler.inputDeleteCounter, times(1)).inc(recorder.getDdlTotal());
            verify(handler.inputOthersCounter, times(1)).inc(recorder.getOthersTotal());
        }
    }

    @Nested
    class HandleStreamReadEnqueuedTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(handler).handleStreamReadEnqueued(anyLong(), any(HandlerUtil.EventTypeRecorder.class));
        }

        @Test
        void testHandleStreamReadEnqueued() {
            recorder.incrInsertTotal();
            long time = System.currentTimeMillis();
            handler.handleStreamReadEnqueued(time, recorder);

            verify(handler.lastEnqueuedIncrementalEventAt, times(1)).setValue(time);
            verify(handler.pendingIncrementalEvent, times(1)).setValue(0);
        }

        @Test
        void testHandleStreamReadEnqueuedHeartbeatOnly() {
            recorder.setNewestEventTimestamp(System.currentTimeMillis());
            long time = System.currentTimeMillis();
            handler.handleStreamReadEnqueued(time, recorder);

            verify(handler.lastEnqueuedIncrementalEventAt, times(1)).setValue(time);
            verify(handler.pendingIncrementalEvent, times(1)).setValue(0);
        }
    }

    @Nested
    class HandleWriteRecordAcceptTest {
        @BeforeEach
        void init() {
            doCallRealMethod().when(handler).handleWriteRecordAccept(anyLong(), any(WriteListResult.class), any(HandlerUtil.EventTypeRecorder.class));
        }

        @Test
        void testHandleWriteRecordAccept() {
            long time = System.currentTimeMillis();
            WriteListResult<TapRecordEvent> result = new WriteListResult<>();
            handler.handleWriteRecordAccept(time, result, recorder);

            verify(handler.outputInsertCounter, times(1)).inc(result.getInsertedCount());
            verify(handler.outputUpdateCounter, times(1)).inc(result.getRemovedCount());
            verify(handler.outputDeleteCounter, times(1)).inc(result.getModifiedCount());
            verify(handler.outputSpeed, times(1)).add(result.getInsertedCount() + result.getRemovedCount() + result.getModifiedCount());
            verify(handler.outputSizeSpeed, times(1)).add(recorder.getMemorySize());
        }
    }
    @Nested
    class handleTableCountAcceptTest{
        @Test
        void testHandleTableCountAcceptSample() {
            Map<String, Long> currentSnapshotTableRowTotalMap = new HashMap<>();
            ReflectionTestUtils.setField(handler, "currentSnapshotTableRowTotalMap", currentSnapshotTableRowTotalMap);
            String table = "table1";
            doCallRealMethod().when(handler).handleTableCountAccept(anyString(), anyLong());
            handler.handleTableCountAccept(table, -1L);
            handler.handleTableCountAccept(table, 10L);
            assertEquals(10L, currentSnapshotTableRowTotalMap.get(table));
        }
    }
}
