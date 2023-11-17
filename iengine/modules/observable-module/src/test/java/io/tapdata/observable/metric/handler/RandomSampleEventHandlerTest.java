package io.tapdata.observable.metric.handler;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class RandomSampleEventHandlerTest {

    @Nested
    class RandomSampleEventHandlerInitTest {
        @Test
        void rangeLessThenZero() {
            RandomSampleEventHandler r = new RandomSampleEventHandler(-1);
            Assertions.assertEquals(1, r.sampleRate);
        }
        @Test
        void rangeLagerThenOne() {
            RandomSampleEventHandler r = new RandomSampleEventHandler(21);
            Assertions.assertEquals(1, r.sampleRate);
        }

        @Test
        void rangeBetweenZeroAndOne() {
            RandomSampleEventHandler r = new RandomSampleEventHandler(0.8);
            Assertions.assertEquals(0.8, r.sampleRate);
        }
    }

    @Nested
    class RandomSampleListTest {
        /**
         * 测试取样方法，根据比例采样
         * */
        @Test
        public void testRandomSampleList() {
            testRandomSampleCase(4, 0.5, 4);
        }
        /**
         * 测试取样方法，根据比例采样
         * */
        @Test
        public void testRandomSampleListRangeLessThenZero() {
            testRandomSampleCase(4, -1, 4);
        }
        /**
         * 测试取样方法，根据比例采样
         * */
        @Test
        public void testRandomSampleListRangeLagerThenOne() {
            testRandomSampleCase(4, 11, 4);
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
            List<Integer> array = arrayCount < 0 ? null : (List<Integer>) simpleList(arrayCount, RandomSampleEventHandlerTest::simpleInteger);
            Method method = ReflectionUtils.getRequiredMethod(handler.getClass(), "randomSampleList", List.class, Double.class);
            Object randomSampleList = ReflectionUtils.invokeMethod(method, handler, array, range);
            Assert.assertNotNull(randomSampleList);
            Assert.assertEquals(ArrayList.class, randomSampleList.getClass());
            Assert.assertEquals(expected, ((Collection<?>)randomSampleList).size());
        }

    }

    @Nested
    class SizeOfDataMapTest {
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
            Method method = ReflectionUtils.getRequiredMethod(handler.getClass(), "sizeOfDataMap", Map.class, long.class);
            Object sizeOfMap = ReflectionUtils.invokeMethod(method, handler, map, initSize);
            Assert.assertNotNull(sizeOfMap);
            Assert.assertEquals(Long.class, sizeOfMap.getClass());
            Assert.assertTrue(((Number)sizeOfMap).longValue() > expectedStart);
        }
    }

    @Nested
    class SizeOfTapEvenTest {
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
            Method method = ReflectionUtils.getRequiredMethod(handler.getClass(), "sizeOfTapEvent", TapEvent.class);
            Object sizeOfMap = ReflectionUtils.invokeMethod(method, handler, map);
            Assert.assertNotNull(sizeOfMap);
            Assert.assertTrue(sizeOfMap instanceof Number);
            return ((Number)sizeOfMap).longValue();
        }
    }

    @Nested
    class SimpleMemoryTapEventTest {
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

        /**
         * 测试TapEvent内存计算方法, 边界值：event 列表中包含Null Event
         * */
        @Test
        public void testSimpleMemoryTapEvent3() {
            RandomSampleEventHandler handler = new RandomSampleEventHandler(1);
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            List<?> events = simpleList(3, RandomSampleEventHandlerTest::simpleTapEvent);
            events.add(null);
            RandomSampleEventHandler.HandleEvent handle = (e) -> (TapEvent) e;
            handler.sampleMemoryTapEvent(recorder, events, handle);
            Assertions.assertTrue(recorder.getMemorySize() > 0);
        }

        public HandlerUtil.EventTypeRecorder testSimpleMemoryTapEvent(int tapCount) {
            RandomSampleEventHandler handler = new RandomSampleEventHandler(1);
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            List<?> events = tapCount < 0 ? null : simpleList(tapCount, RandomSampleEventHandlerTest::simpleTapEvent);
            RandomSampleEventHandler.HandleEvent handle = (e) -> (TapEvent) e;
            handler.sampleMemoryTapEvent(recorder, events, handle);
            return recorder;
        }
    }


    public static List<?> simpleList(int size, Function<Object, Object> function) {
        List<Object> list = new ArrayList<>();
        if (size <= 0) return list;
        for (int index = 0; index < size; index++) {
            list.add(function.apply(size));
        }
        return list;
    }

    public static Map<String, Object> simpleMap(int size, Object obj) {
        if (size < 0) return null;
        Map<String, Object> map = new HashMap<>();
        if (size > 0) {
            for (int index = 0; index < size; index++) {
                map.put(UUID.randomUUID().toString(), RandomUtils.nextInt(0, size));
            }
        }
        return map;
    }

    public static TapEvent simpleTapEvent(Object size) {
        TapUpdateRecordEvent e = new TapUpdateRecordEvent();
        e.table("test");
        e.referenceTime(System.currentTimeMillis());
        e.after(simpleMap((int)size, null));
        e.before(simpleMap((int)size, null));
        return e;
    }

    public static Integer simpleInteger(Object obj) {
        return RandomUtils.nextInt();
    }

}
