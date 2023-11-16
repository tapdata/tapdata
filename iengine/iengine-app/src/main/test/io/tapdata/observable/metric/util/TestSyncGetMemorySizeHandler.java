package io.tapdata.observable.metric.util;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.observable.metric.handler.HandlerUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TestSyncGetMemorySizeHandler {
    TapEvent tapEvent;
    SyncGetMemorySizeHandler handler;
    AtomicLong atomicLong;
    /**造数据*/
    @Before
    public void init() {
        atomicLong = new AtomicLong(-1);
        handler = new SyncGetMemorySizeHandler(atomicLong);
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
    /**测试getEventTypeRecorderSyncTapEvent属性*/
    @Test
    public void testGetEventTypeRecorderSyncTapEvent() {
        List<TapEvent> list = new ArrayList<>();
        list.add(tapEvent);
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertTrue(recorder.getMemorySize() > 0);
    }
    /**测试getEventTypeRecorderSyncTapEvent属性, 边界条件，空TapEvent列表*/
    @Test
    public void testGetEventTypeRecorderSyncTapEvent0() {
        List<TapEvent> list = new ArrayList<>();
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(0, recorder.getMemorySize());
    }
    /**测试getEventTypeRecorderSyncTapEvent属性, 边界条件，null TapEvent列表*/
    @Test
    public void testGetEventTypeRecorderSyncTapEvent1() {
        List<TapEvent> list = null;
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(0, recorder.getMemorySize());
    }
    /**测试getEventTypeRecorderSyncTapEvent属性, 边界条件，null AtomicLong*/
    @Test
    public void testGetEventTypeRecorderSyncTapEvent2() {
        handler = new SyncGetMemorySizeHandler(null);
        List<TapEvent> list = new ArrayList<>();
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(0, recorder.getMemorySize());
    }
    /**测试getEventTypeRecorderSyncTapEvent属性, 边界条件, AtomicLong > 0*/
    @Test
    public void testGetEventTypeRecorderSyncTapEvent3() {
        List<TapEvent> list = new ArrayList<>();
        list.add(tapEvent);
        handler = new SyncGetMemorySizeHandler(new AtomicLong(100));
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(100, recorder.getMemorySize());
    }
    /**测试getEventTypeRecorderSyncTapEvent属性*/
    @Test
    public void testGetEventTypeRecorderSyncTapDataEvent() {
        List<TapdataEvent> list = new ArrayList<>();
        TapdataEvent event = new TapdataEvent();
        event.setTapEvent(tapEvent);
        event.setSourceTime(System.nanoTime());
        list.add(event);
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapDataEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertTrue(recorder.getMemorySize() > 0);
    }
    /**测试getEventTypeRecorderSyncTapDataEvent属性, 边界条件，空TapEvent列表*/
    @Test
    public void testGetEventTypeRecorderSyncTapDataEvent0() {
        List<TapdataEvent> list = new ArrayList<>();
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapDataEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(0, recorder.getMemorySize());
    }
    /**测试getEventTypeRecorderSyncTapDataEvent属性, 边界条件，null TapEvent列表*/
    @Test
    public void testGetEventTypeRecorderSyncTapDataEvent1() {
        List<TapdataEvent> list = null;
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapDataEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(0, recorder.getMemorySize());
    }
    /**测试getEventTypeRecorderSyncTapDataEvent属性, 边界条件，null AtomicLong*/
    @Test
    public void testGetEventTypeRecorderSyncTapDataEvent2() {
        handler = new SyncGetMemorySizeHandler(null);
        List<TapdataEvent> list = new ArrayList<>();
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapDataEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(0, recorder.getMemorySize());
    }
    /**测试getEventTypeRecorderSyncTapDataEvent属性, 边界条件, AtomicLong > 0*/
    @Test
    public void testGetEventTypeRecorderSyncTapDataEvent3() {
        List<TapdataEvent> list = new ArrayList<>();
        TapdataEvent event = new TapdataEvent();
        event.setTapEvent(tapEvent);
        event.setSourceTime(System.nanoTime());
        list.add(event);
        handler = new SyncGetMemorySizeHandler(new AtomicLong(100));
        HandlerUtil.EventTypeRecorder recorder = handler.getEventTypeRecorderSyncTapDataEvent(list);
        Assert.assertNotNull(recorder);
        Assert.assertEquals(100, recorder.getMemorySize());
    }
}