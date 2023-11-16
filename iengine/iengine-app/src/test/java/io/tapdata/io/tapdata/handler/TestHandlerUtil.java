package io.tapdata.io.tapdata.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.index.TapDeleteIndexEvent;
import io.tapdata.entity.event.ddl.table.TapAlterDatabaseTimezoneEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldPrimaryKeyEvent;
import io.tapdata.entity.event.ddl.table.TapAlterTableCharsetEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.observable.metric.handler.HandlerUtil;
import io.tapdata.observable.metric.handler.RandomSampleEventHandler;
import io.tapdata.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHandlerUtil {
    TapEvent tapEvent;

    /**造数据*/
    @Before
    public void init() {
        TapUpdateRecordEvent event = new TapUpdateRecordEvent();
        event.referenceTime(System.currentTimeMillis());
        Map<String, Object> before = new HashMap<>();
        before.put("key", "hhh");
        Map<String, Object> after = new HashMap<>();
        after.put("key", "ooo");
        event.before(before);
        event.after(after);
        tapEvent = event;
    }

    /**测试countTapDataEvent方法， 预期结果*/
    @Test
    public void testCountTapDataEvent() {
        List<TapdataEvent> events = new ArrayList<>();
        TapdataEvent e = new TapdataEvent();
        e.setTapEvent(tapEvent);
        events.add(e);
        HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapdataEvent(events);
        Assert.assertNotNull(recorder);
        Assert.assertTrue(recorder.getMemorySize() > 0);
        Assert.assertEquals(1, recorder.getUpdateTotal());
        Assert.assertEquals(0, recorder.getInsertTotal());
        Assert.assertEquals(0, recorder.getDdlTotal());
        Assert.assertEquals(1, recorder.getTotal());
    }

    /**测试countTapEvent方法， 预期结果*/
    @Test
    public void testCountTapEvent() {
        List<TapEvent> events = new ArrayList<>();
        events.add(tapEvent);
        HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
        Assert.assertNotNull(recorder);
        Assert.assertTrue(recorder.getMemorySize() > 0);
        Assert.assertEquals(1, recorder.getUpdateTotal());
        Assert.assertEquals(0, recorder.getInsertTotal());
        Assert.assertEquals(0, recorder.getDdlTotal());
        Assert.assertEquals(1, recorder.getTotal());
    }

    /**测试countTapEvent方法， 边界：空TapEvent列表*/
    @Test
    public void testCountTapEvent1() {
        List<TapEvent> events = new ArrayList<>();
        HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(0, recorder.getMemorySize());
        Assert.assertEquals(0, recorder.getUpdateTotal());
        Assert.assertEquals(0, recorder.getInsertTotal());
        Assert.assertEquals(0, recorder.getDdlTotal());
        Assert.assertEquals(0, recorder.getTotal());
    }

    /**测试randomSampleEventHandler属性*/
    @Test
    public void testRandomSampleEventHandler() {
        Object randomSampleEventHandler = TestUtil.getStaticField(HandlerUtil.class, "randomSampleEventHandler");
        Assert.assertNotNull(randomSampleEventHandler);
        Assert.assertTrue(randomSampleEventHandler instanceof RandomSampleEventHandler);
    }

    /**测试covertTapDataEvent属性， 预期结果*/
    @Test
    public void testCovertTapDataEvent(){
        TapdataEvent e = new TapdataEvent();
        e.setTapEvent(tapEvent);
        TapEvent handel = testCovertTapDataEvent(e);
        Assert.assertNotNull(handel);
        Assert.assertEquals(tapEvent, handel);
    }

    /**测试covertTapDataEvent属性， 预期结果*/
    @Test
    public void testCovertTapDataEvent0(){
        TapEvent handel = testCovertTapDataEvent(tapEvent);
        Assert.assertNotNull(handel);
        Assert.assertEquals(tapEvent, handel);
    }

    /**测试covertTapDataEvent属性， 边界值：处理null值*/
    @Test
    public void testCovertTapDataEvent1(){
        TapEvent handel = testCovertTapDataEvent(null);
        Assert.assertNull(handel);
    }

    /**测试covertTapDataEvent属性， 边界值：处理其他类型值*/
    @Test
    public void testCovertTapDataEvent2(){
        TapEvent handel = testCovertTapDataEvent(new Object());
        Assert.assertNull(handel);
    }

    /**测试sampleMemoryTapEvent属性， 预期：sizeOfMemory = null时重新计算sizeOfMemory*/
    @Test
    public void testSampleMemoryTapEvent() {
        List<TapEvent> events = new ArrayList<>();
        events.add(tapEvent);
        HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
        recorder.setMemorySize(-1);
        HandlerUtil.sampleMemoryTapEvent(recorder, events, null);
        Assert.assertTrue(recorder.getMemorySize() > 0);
    }
    /**测试sampleMemoryTapEvent属性， 预期：sizeOfMemory != null时不重新计算sizeOfMemory*/
    @Test
    public void testSampleMemoryTapEvent0() {
        List<TapEvent> events = new ArrayList<>();
        events.add(tapEvent);
        HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
        long size = recorder.getMemorySize();
        HandlerUtil.sampleMemoryTapEvent(recorder, events, size);
        Assert.assertEquals(size, recorder.getMemorySize());
    }

    public TapEvent testCovertTapDataEvent(Object handleObject){
        Object handler = TestUtil.getStaticField(HandlerUtil.class, "covertTapDataEvent");
        Assert.assertNotNull(handler);
        Assert.assertTrue(handler instanceof RandomSampleEventHandler.HandleEvent);
        RandomSampleEventHandler.HandleEvent h = (RandomSampleEventHandler.HandleEvent)handler;
        return h.handel(handleObject);
    }

    /**测试countEventTypeAndGetReferenceTime方法*/
    @Test
    public void testCountEventTypeAndGetReferenceTime() {
        Long timestamp = ((TapUpdateRecordEvent)tapEvent).getReferenceTime();
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        executeCountEventTypeAndGetReferenceTime(tapEvent, recorder, timestamp);
        Assert.assertEquals(1, recorder.getUpdateTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, HeartbeatEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime0() {
        Long timestamp = System.currentTimeMillis();
        assertEvent(new HeartbeatEvent().referenceTime(timestamp), timestamp);
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapInsertRecordEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime1() {
        Long timestamp = System.currentTimeMillis();
        HandlerUtil.EventTypeRecorder recorder = assertEvent(new TapInsertRecordEvent().referenceTime(timestamp), timestamp);
        Assert.assertEquals(1, recorder.getInsertTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapDeleteRecordEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime2() {
        Long timestamp = System.currentTimeMillis();
        HandlerUtil.EventTypeRecorder recorder = assertEvent(new TapDeleteRecordEvent().referenceTime(timestamp), timestamp);
        Assert.assertEquals(1, recorder.getDeleteTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapDeleteIndexEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime3() {
        Long t = System.currentTimeMillis();
        TapDeleteIndexEvent event = new TapDeleteIndexEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapCreateIndexEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime4() {
        Long t = System.currentTimeMillis();
        TapCreateIndexEvent event = new TapCreateIndexEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapAlterDatabaseTimezoneEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime5() {
        Long t = System.currentTimeMillis();
        TapAlterDatabaseTimezoneEvent event = new TapAlterDatabaseTimezoneEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapAlterFieldAttributesEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime6() {
        Long t = System.currentTimeMillis();
        TapAlterFieldAttributesEvent event = new TapAlterFieldAttributesEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapAlterFieldNameEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime7() {
        Long t = System.currentTimeMillis();
        TapAlterFieldNameEvent event = new TapAlterFieldNameEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapAlterFieldPrimaryKeyEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime8() {
        Long t = System.currentTimeMillis();
        TapAlterFieldPrimaryKeyEvent event = new TapAlterFieldPrimaryKeyEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapAlterTableCharsetEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime9() {
        Long t = System.currentTimeMillis();
        TapAlterTableCharsetEvent event = new TapAlterTableCharsetEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapClearTableEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime10() {
        Long t = System.currentTimeMillis();
        TapClearTableEvent event = new TapClearTableEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapCreateTableEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime11() {
        Long t = System.currentTimeMillis();
        TapCreateTableEvent event = new TapCreateTableEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapDropFieldEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime12() {
        Long t = System.currentTimeMillis();
        TapDropFieldEvent event = new TapDropFieldEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapNewFieldEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime13() {
        Long t = System.currentTimeMillis();
        TapNewFieldEvent event = new TapNewFieldEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapNewFieldEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime14() {
        Long t = System.currentTimeMillis();
        TapNewFieldEvent event = new TapNewFieldEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, TapRenameTableEvent*/
    @Test
    public void testCountEventTypeAndGetReferenceTime15() {
        Long t = System.currentTimeMillis();
        TapRenameTableEvent event = new TapRenameTableEvent();
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getDdlTotal());
    }

    /**测试countEventTypeAndGetReferenceTime方法, default case */
    @Test
    public void testCountEventTypeAndGetReferenceTime16() {
        Long t = System.currentTimeMillis();
        TapDDLEvent event = new TapDDLEvent(99999){};
        event.setReferenceTime(t);
        HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
        Assert.assertEquals(1, recorder.getOthersTotal());
    }

    private HandlerUtil.EventTypeRecorder assertEvent(TapEvent event, Long timestamp){
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        executeCountEventTypeAndGetReferenceTime(event, recorder, timestamp);
        return recorder;
    }

    private void executeCountEventTypeAndGetReferenceTime(TapEvent event, HandlerUtil.EventTypeRecorder recorder, Long expected) {
        Object value =  TestUtil.invokerStaticPrivateMethod(
                HandlerUtil.class,
                "countEventTypeAndGetReferenceTime",
                new Class[]{TapEvent.class, HandlerUtil.EventTypeRecorder.class},
                new Object[]{event, recorder});
        Assert.assertNotNull(value);
        Assert.assertEquals(expected, value);
    }


    /**测试 setEventTimestamp 方法*/
    @Test
    public void testSetEventTimestamp() {
        Long timestamp = System.currentTimeMillis();
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        invokerSetEventTimestamp(recorder, timestamp);
        Assert.assertEquals(timestamp, recorder.getOldestEventTimestamp());
        Assert.assertEquals(timestamp, recorder.getNewestEventTimestamp());
    }

    /**测试 setEventTimestamp 方法, 边界：timestamp=null*/
    @Test
    public void testSetEventTimestamp0() {
        Long timestamp = null;
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        invokerSetEventTimestamp(recorder, timestamp);
        Assert.assertNull(recorder.getOldestEventTimestamp());
        Assert.assertNull(recorder.getNewestEventTimestamp());
    }

    /**测试 setEventTimestamp 方法, 边界：recorder中包含时间OldestEventTimestamp比Timestamp小*/
    @Test
    public void testSetEventTimestamp1() {
        Long timestamp = System.currentTimeMillis();
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        Long item = (timestamp - 1);
        recorder.setOldestEventTimestamp(item);
        invokerSetEventTimestamp(recorder, timestamp);
        Assert.assertEquals(item , recorder.getOldestEventTimestamp());
    }

    /**测试 setEventTimestamp 方法, 边界：recorder中包含OldestEventTimestamp时间比timestamp大*/
    @Test
    public void testSetEventTimestamp2() {
        Long timestamp = System.currentTimeMillis();
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        recorder.setOldestEventTimestamp(timestamp + 1);
        invokerSetEventTimestamp(recorder, timestamp);
        Assert.assertEquals(timestamp , recorder.getOldestEventTimestamp());
    }


    /**测试 setEventTimestamp 方法, 边界：recorder中包含NewestEventTimestamp时间比timestamp小*/
    @Test
    public void testSetEventTimestamp3() {
        Long timestamp = System.currentTimeMillis();
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        Long item = timestamp - 1;
        recorder.setNewestEventTimestamp(item);
        invokerSetEventTimestamp(recorder, timestamp);
        Assert.assertEquals(timestamp, recorder.getNewestEventTimestamp());
    }

    /**测试 setEventTimestamp 方法, 边界：recorder中包含NewestEventTimestamp时间比timestamp大*/
    @Test
    public void testSetEventTimestamp4() {
        Long timestamp = System.currentTimeMillis();
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        recorder.setOldestEventTimestamp(timestamp + 1);
        invokerSetEventTimestamp(recorder, timestamp);
        Assert.assertEquals(timestamp, recorder.getNewestEventTimestamp());
    }

    private void invokerSetEventTimestamp(HandlerUtil.EventTypeRecorder recorder, Long ts){
        TestUtil.invokerStaticPrivateMethod(
                HandlerUtil.class,
                "setEventTimestamp",
                new Class[]{HandlerUtil.EventTypeRecorder.class, Long.class},
                new Object[]{recorder, ts});
    }
}
