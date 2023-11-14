package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import util.TestUtil;

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

    public TapEvent testCovertTapDataEvent(Object handleObject){
        Object handler = TestUtil.getStaticField(HandlerUtil.class, "covertTapDataEvent");
        Assert.assertNotNull(handler);
        Assert.assertTrue(handler instanceof RandomSampleEventHandler.HandleEvent);
        RandomSampleEventHandler.HandleEvent h = (RandomSampleEventHandler.HandleEvent)handler;
        return h.handel(handleObject);
    }
}
