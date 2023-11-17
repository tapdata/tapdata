package io.tapdata.observable.metric.handler;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.observable.metric.handler.HandlerUtil;
import io.tapdata.observable.metric.handler.RandomSampleEventHandler;
import io.tapdata.util.TestUtil;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class RandomSampleEventHandlerTest {

    /**
     * 测试取样方法，根据比例采样
     * */
    @Test
    public void testRandomSampleList() {
        testRandomSampleCase(4, 0.5, 4);
    }

    /**
     * 测试取样方法，根据比例采样,
     * */
    @Test
    public void testRandomSampleList0() {
        testRandomSampleCase(20, 0.5, 10);
    }

    /**
     * 测试取样方法，根据比例采样,
     * */
    @Test
    public void testRandomSampleLis1() {
        testRandomSampleCase(20, 1, 20);
    }

    /**
     * 测试取样方法，根据比例采样, 边界：采样空列表
     * */
    @Test
    public void testRandomSampleLis2() {
        testRandomSampleCase(0, 1, 0);
    }

    /**
     * 测试取样方法，根据比例采样, 边界：采样null列表
     * */
    @Test
    public void testRandomSampleLis3() {
        testRandomSampleCase(-1, 1, 0);
    }

    private void testRandomSampleCase(int arrayCount, double range, int expected) {
        RandomSampleEventHandler handler = new RandomSampleEventHandler(range);
        List<Integer> array = arrayCount < 0 ? null : (List<Integer>) simpleList(arrayCount, this::simpleInteger);
        Object randomSampleList = TestUtil.invokerPrivateMethod(
                handler,
                "randomSampleList",
                TestUtil.getArray(List.class, Double.class),
                array, range
        );
        Assert.assertNotNull(randomSampleList);
        Assert.assertTrue(randomSampleList instanceof Collection);
        Assert.assertEquals(expected, ((Collection<?>)randomSampleList).size());
    }


    /**
     * 测试内存计算方法
     * */
    @Test
    public void testSizeOfDataMap() {
        testSizeOfDataMap(5, 0, 0);
    }

    /**
     * 测试内存计算方法, 边界值：空Map
     * */
    @Test
    public void testSizeOfDataMap1() {
        testSizeOfDataMap(0, 0, -1);
    }

    /**
     * 测试内存计算方法, 边界值：null Map
     * */
    @Test
    public void testSizeOfDataMap2() {
        testSizeOfDataMap(-1, 0, -1);
    }

    /**
     * 测试内存计算方法, 边界值：初始大小 小于0，计算结果应该大于0
     * */
    @Test
    public void testSizeOfDataMap3() {
        testSizeOfDataMap(1, -100000, 0);
    }

    private void testSizeOfDataMap(int mapLength, long initSize, long expectedStart) {
        RandomSampleEventHandler handler = new RandomSampleEventHandler(1);
        Map<String, Object> map = simpleMap(mapLength, new Object());
        Object sizeOfMap = TestUtil.invokerPrivateMethod(
                handler,
                "sizeOfDataMap",
                TestUtil.getArray(Map.class, long.class),
                map, initSize
        );
        Assert.assertNotNull(sizeOfMap);
        Assert.assertTrue(sizeOfMap instanceof Number);
        Assert.assertTrue(((Number)sizeOfMap).longValue() > expectedStart);
    }

    /**
     * 测试TapEvent内存计算方法
     * */
    @Test
    public void sizeOfTapEvent1() {
        long size = sizeOfTapEvent(2);
        Assert.assertTrue(size > 0);
    }


    /**
     * 测试TapEvent内存计算方法
     * */
    @Test
    public void sizeOfTapEvent2() {
        long size = sizeOfTapEvent(0);
        Assert.assertEquals(0, size);
    }

    /**
     * 测试TapEvent内存计算方法
     * */
    @Test
    public void sizeOfTapEvent3() {
        long size = sizeOfTapEvent(-1);
        Assert.assertEquals(0, size);
    }

    private long sizeOfTapEvent(int mapLength) {
        RandomSampleEventHandler handler = new RandomSampleEventHandler(1);
        TapEvent map = mapLength < 0 ? null : simpleTapEvent(mapLength);
        Object sizeOfMap = TestUtil.invokerPrivateMethod(
                handler,
                "sizeOfTapEvent",
                TestUtil.getArray(TapEvent.class),
                map
        );
        Assert.assertNotNull(sizeOfMap);
        Assert.assertTrue(sizeOfMap instanceof Number);
        return ((Number)sizeOfMap).longValue();
    }


    /**
     * 测试TapEvent内存计算方法
     * */
    @Test
    public void testSimpleMemoryTapEvent() {
        HandlerUtil.EventTypeRecorder recorder = testSimpleMemoryTapEvent(10);
        Assert.assertTrue(recorder.getMemorySize() > 0);
    }

    /**
     * 测试TapEvent内存计算方法, 边界值：计算空TapEvent列表的内存大小
     * */
    @Test
    public void testSimpleMemoryTapEvent1() {
        HandlerUtil.EventTypeRecorder recorder = testSimpleMemoryTapEvent(0);
        Assert.assertEquals(0, recorder.getMemorySize());
    }

    /**
     * 测试TapEvent内存计算方法, 边界值：计算null TapEvent列表的内存大小
     * */
    @Test
    public void testSimpleMemoryTapEvent2() {
        HandlerUtil.EventTypeRecorder recorder = testSimpleMemoryTapEvent(-1);
        Assert.assertEquals(0, recorder.getMemorySize());
    }


    public HandlerUtil.EventTypeRecorder testSimpleMemoryTapEvent(int tapCount) {
        RandomSampleEventHandler handler = new RandomSampleEventHandler(1);
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        List<?> events = tapCount < 0 ? null : simpleList(tapCount, this::simpleTapEvent);
        RandomSampleEventHandler.HandleEvent handle = (e) -> (TapEvent) e;
        handler.sampleMemoryTapEvent(recorder, events, handle);
        return recorder;
    }

    private static List<?> simpleList(int size, Function<Object, Object> function) {
        List<Object> list = new ArrayList<>();
        if (size <= 0) return list;
        for (int index = 0; index < size; index++) {
            list.add(function.apply(size));
        }
        return list;
    }

    private Map<String, Object> simpleMap(int size, Object obj) {
        if (size < 0) return null;
        Map<String, Object> map = new HashMap<>();
        if (size > 0) {
            for (int index = 0; index < size; index++) {
                map.put(UUID.randomUUID().toString(), RandomUtils.nextInt(0, size));
            }
        }
        return map;
    }

    private TapEvent simpleTapEvent(Object size) {
        TapUpdateRecordEvent e = new TapUpdateRecordEvent();
        e.table("test");
        e.referenceTime(System.currentTimeMillis());
        e.after(simpleMap((int)size, null));
        e.before(simpleMap((int)size, null));
        return e;
    }

    private Integer simpleInteger(Object obj) {
        return RandomUtils.nextInt();
    }

}
