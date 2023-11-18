package io.tapdata.observable.metric.handler;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataHeartbeatEvent;
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
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.ddl.table.TapRenameTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HandlerUtilTest {
    TapEvent tapEvent;

    /**造数据*/
    @BeforeEach
    void init() {
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

    @Nested
    class CountTapDataEventTest {
        /**测试countTapDataEvent方法， 预期结果*/
        @Test
        void testCountTapDataEvent() {
            List<TapdataEvent> events = new ArrayList<>();
            TapdataEvent e = new TapdataEvent();
            e.setTapEvent(tapEvent);
            events.add(e);
            HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapdataEvent(events);
            Assertions.assertNotNull(recorder);
            Assertions.assertTrue(recorder.getMemorySize() > 0);
            Assertions.assertEquals(1, recorder.getUpdateTotal());
            Assertions.assertEquals(0, recorder.getInsertTotal());
            Assertions.assertEquals(0, recorder.getDdlTotal());
            Assertions.assertEquals(1, recorder.getTotal());
        }

        /**测试countTapDataEvent方法，TapdataHeartbeatEvent, 预期结果*/
        @Test
        void testCountTapDataEventOfTapDataHeartbeatEvent() {
            List<TapdataEvent> events = new ArrayList<>();
            TapdataHeartbeatEvent event = new TapdataHeartbeatEvent();
            Long time = System.currentTimeMillis() - 5;
            event.setSourceTime(time);
            events.add(event);
            HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapdataEvent(events);
            Assertions.assertNotNull(recorder);
            Assertions.assertEquals(0, recorder.getMemorySize());
            Assertions.assertEquals(0, recorder.getUpdateTotal());
            Assertions.assertEquals(0, recorder.getInsertTotal());
            Assertions.assertEquals(0, recorder.getDdlTotal());
            Assertions.assertEquals(0, recorder.getTotal());
            Assertions.assertTrue(recorder.getReplicateLagTotal() > 4);
            Assertions.assertTrue(recorder.getReplicateLagTotal() > (System.currentTimeMillis() - time - 1 ));
        }
    }

    @Nested
    class CountTapEvenTest {
        /**测试countTapEvent方法， 预期结果*/
        @Test
        void testCountTapEvent() {
            List<TapEvent> events = new ArrayList<>();
            events.add(tapEvent);
            HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
            Assertions.assertNotNull(recorder);
            Assertions.assertTrue(recorder.getMemorySize() > 0);
            Assertions.assertEquals(1, recorder.getUpdateTotal());
            Assertions.assertEquals(0, recorder.getInsertTotal());
            Assertions.assertEquals(0, recorder.getDdlTotal());
            Assertions.assertEquals(1, recorder.getTotal());

            Assertions.assertTrue(recorder.getReplicateLagTotal()>=0);
            Assertions.assertNull(recorder.getProcessTimeTotal());
            Assertions.assertEquals(0, recorder.getOthersTotal());
            Assertions.assertEquals(((TapUpdateRecordEvent)tapEvent).getReferenceTime(), recorder.getNewestEventTimestamp());
            Assertions.assertEquals(((TapUpdateRecordEvent)tapEvent).getReferenceTime(), recorder.getOldestEventTimestamp());
            Assertions.assertEquals("B", recorder.getMemoryUtil());
        }

        /**测试countTapEvent方法， 边界：空TapEvent列表*/
        @Test
        public void testCountTapEvent1() {
            List<TapEvent> events = new ArrayList<>();
            HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
            Assertions.assertNotNull(recorder);
            Assertions.assertEquals(0, recorder.getMemorySize());
            Assertions.assertEquals(0, recorder.getUpdateTotal());
            Assertions.assertEquals(0, recorder.getInsertTotal());
            Assertions.assertEquals(0, recorder.getDdlTotal());
            Assertions.assertEquals(0, recorder.getTotal());
        }

        /**测试countTapEvent方法， 边界：TapdataHeartbeatEvent*/
        @Test
        public void testCountTapEvent2() {
            List<TapdataEvent> events = new ArrayList<>();
            TapdataHeartbeatEvent event = new TapdataHeartbeatEvent();
            event.setSourceTime(System.currentTimeMillis());
            events.add(event);
            HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapdataEvent(events);
            Assertions.assertNotNull(recorder);
            Assertions.assertEquals(0, recorder.getMemorySize());
            Assertions.assertEquals(0, recorder.getUpdateTotal());
            Assertions.assertEquals(0, recorder.getInsertTotal());
            Assertions.assertEquals(0, recorder.getDdlTotal());
            Assertions.assertEquals(0, recorder.getTotal());
        }
    }

    @Nested
    class RandomSampleEventHandlerTest {
        /**测试randomSampleEventHandler属性*/
        @Test
        public void testRandomSampleEventHandler() {
            Object randomSampleEventHandler = null;
            try {
                Field field = HandlerUtil.class.getDeclaredField("randomSampleEventHandler");
                field.setAccessible(true);
                randomSampleEventHandler = field.get(null);
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
            Assertions.assertNotNull(randomSampleEventHandler);
            Assertions.assertTrue(randomSampleEventHandler instanceof RandomSampleEventHandler);
        }
    }

    @Nested
    class CovertTapDataEvent {
        /**测试covertTapDataEvent属性， 预期结果*/
        @Test
        public void testCovertTapDataEvent(){
            TapdataEvent e = new TapdataEvent();
            e.setTapEvent(tapEvent);
            TapEvent handel = testCovertTapDataEvent(e);
            Assertions.assertNotNull(handel);
            Assertions.assertEquals(tapEvent, handel);
        }

        /**测试covertTapDataEvent属性， 预期结果*/
        @Test
        public void testCovertTapDataEvent0(){
            TapEvent handel = testCovertTapDataEvent(tapEvent);
            Assertions.assertNotNull(handel);
            Assertions.assertEquals(tapEvent, handel);
        }

        /**测试covertTapDataEvent属性， 边界值：处理null值*/
        @Test
        public void testCovertTapDataEvent1(){
            TapEvent handel = testCovertTapDataEvent(null);
            Assertions.assertNull(handel);
        }

        /**测试covertTapDataEvent属性， 边界值：处理其他类型值*/
        @Test
        public void testCovertTapDataEvent2(){
            TapEvent handel = testCovertTapDataEvent(new Object());
            Assertions.assertNull(handel);
        }

        public TapEvent testCovertTapDataEvent(Object handleObject){
            Object handler = null;
            try {
                Field field = HandlerUtil.class.getDeclaredField("covertTapDataEvent");
                field.setAccessible(true);
                handler = field.get(null);
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
            Assertions.assertNotNull(handler);
            RandomSampleEventHandler.HandleEvent h = (RandomSampleEventHandler.HandleEvent)handler;
            return h.handel(handleObject);
        }
    }

    @Nested
    class SampleMemoryTapEvent {
        /**测试sampleMemoryTapEvent属性， 预期：sizeOfMemory = null时重新计算sizeOfMemory*/
        @Test
        public void testSampleMemoryTapEvent() {
            List<TapEvent> events = new ArrayList<>();
            events.add(tapEvent);
            HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
            recorder.setMemorySize(-1);
            HandlerUtil.sampleMemoryTapEvent(recorder, events, null);
            Assertions.assertTrue(recorder.getMemorySize() > 0);
        }
        /**测试sampleMemoryTapEvent属性， 预期：sizeOfMemory != null时不重新计算sizeOfMemory*/
        @Test
        public void testSampleMemoryTapEvent0() {
            List<TapEvent> events = new ArrayList<>();
            events.add(tapEvent);
            HandlerUtil.EventTypeRecorder recorder = HandlerUtil.countTapEvent(events);
            long size = recorder.getMemorySize();
            HandlerUtil.sampleMemoryTapEvent(recorder, events, size);
            Assertions.assertEquals(size, recorder.getMemorySize());
        }
    }

    @Nested
    class CountEventTypeAndGetReferenceTime{
        /**测试countEventTypeAndGetReferenceTime方法*/
        @Test
        public void testCountEventTypeAndGetReferenceTime() {
            Long timestamp = ((TapUpdateRecordEvent)tapEvent).getReferenceTime();
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            executeCountEventTypeAndGetReferenceTime(tapEvent, recorder, timestamp);
            Assertions.assertEquals(1, recorder.getUpdateTotal());
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
            Assertions.assertEquals(1, recorder.getInsertTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapDeleteRecordEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime2() {
            Long timestamp = System.currentTimeMillis();
            HandlerUtil.EventTypeRecorder recorder = assertEvent(new TapDeleteRecordEvent().referenceTime(timestamp), timestamp);
            Assertions.assertEquals(1, recorder.getDeleteTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapDeleteIndexEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime3() {
            Long t = System.currentTimeMillis();
            TapDeleteIndexEvent event = new TapDeleteIndexEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapCreateIndexEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime4() {
            Long t = System.currentTimeMillis();
            TapCreateIndexEvent event = new TapCreateIndexEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapAlterDatabaseTimezoneEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime5() {
            Long t = System.currentTimeMillis();
            TapAlterDatabaseTimezoneEvent event = new TapAlterDatabaseTimezoneEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapAlterFieldAttributesEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime6() {
            Long t = System.currentTimeMillis();
            TapAlterFieldAttributesEvent event = new TapAlterFieldAttributesEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapAlterFieldNameEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime7() {
            Long t = System.currentTimeMillis();
            TapAlterFieldNameEvent event = new TapAlterFieldNameEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapAlterFieldPrimaryKeyEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime8() {
            Long t = System.currentTimeMillis();
            TapAlterFieldPrimaryKeyEvent event = new TapAlterFieldPrimaryKeyEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapAlterTableCharsetEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime9() {
            Long t = System.currentTimeMillis();
            TapAlterTableCharsetEvent event = new TapAlterTableCharsetEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapClearTableEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime10() {
            Long t = System.currentTimeMillis();
            TapClearTableEvent event = new TapClearTableEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapCreateTableEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime11() {
            Long t = System.currentTimeMillis();
            TapCreateTableEvent event = new TapCreateTableEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapDropFieldEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime12() {
            Long t = System.currentTimeMillis();
            TapDropFieldEvent event = new TapDropFieldEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapNewFieldEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime13() {
            Long t = System.currentTimeMillis();
            TapNewFieldEvent event = new TapNewFieldEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapNewFieldEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime14() {
            Long t = System.currentTimeMillis();
            TapNewFieldEvent event = new TapNewFieldEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, TapRenameTableEvent*/
        @Test
        public void testCountEventTypeAndGetReferenceTime15() {
            Long t = System.currentTimeMillis();
            TapRenameTableEvent event = new TapRenameTableEvent();
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }

        /**测试countEventTypeAndGetReferenceTime方法, default case */
        @Test
        public void testCountEventTypeAndGetReferenceTime16() {
            Long t = System.currentTimeMillis();
            TapDDLEvent event = new TapDDLEvent(99999){};
            event.setReferenceTime(t);
            HandlerUtil.EventTypeRecorder recorder = assertEvent(event, t);
            Assertions.assertEquals(1, recorder.getOthersTotal());
        }
    }


    private HandlerUtil.EventTypeRecorder assertEvent(TapEvent event, Long timestamp){
        HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
        executeCountEventTypeAndGetReferenceTime(event, recorder, timestamp);
        return recorder;
    }

    private void executeCountEventTypeAndGetReferenceTime(TapEvent event, HandlerUtil.EventTypeRecorder recorder, Long expected) {
        Long value = HandlerUtil.countEventTypeAndGetReferenceTime(event, recorder);
        Assertions.assertNotNull(value);
        Assertions.assertEquals(expected, value);
    }

    @Nested
    class SetEventTimestampTest {
        /**测试 setEventTimestamp 方法*/
        @Test
        public void testSetEventTimestamp() {
            Long timestamp = System.currentTimeMillis();
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            invokerSetEventTimestamp(recorder, timestamp);
            Assertions.assertEquals(timestamp, recorder.getOldestEventTimestamp());
            Assertions.assertEquals(timestamp, recorder.getNewestEventTimestamp());
        }

        /**测试 setEventTimestamp 方法, 边界：timestamp=null*/
        @Test
        public void testSetEventTimestamp0() {
            Long timestamp = null;
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            invokerSetEventTimestamp(recorder, timestamp);
            Assertions.assertNull(recorder.getOldestEventTimestamp());
            Assertions.assertNull(recorder.getNewestEventTimestamp());
        }

        /**测试 setEventTimestamp 方法, 边界：recorder中包含时间OldestEventTimestamp比Timestamp小*/
        @Test
        public void testSetEventTimestamp1() {
            Long timestamp = System.currentTimeMillis();
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            Long item = (timestamp - 1);
            recorder.setOldestEventTimestamp(item);
            invokerSetEventTimestamp(recorder, timestamp);
            Assertions.assertEquals(item , recorder.getOldestEventTimestamp());
        }

        /**测试 setEventTimestamp 方法, 边界：recorder中包含OldestEventTimestamp时间比timestamp大*/
        @Test
        public void testSetEventTimestamp2() {
            Long timestamp = System.currentTimeMillis();
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            recorder.setOldestEventTimestamp(timestamp + 1);
            invokerSetEventTimestamp(recorder, timestamp);
            Assertions.assertEquals(timestamp , recorder.getOldestEventTimestamp());
        }


        /**测试 setEventTimestamp 方法, 边界：recorder中包含NewestEventTimestamp时间比timestamp小*/
        @Test
        public void testSetEventTimestamp3() {
            Long timestamp = System.currentTimeMillis();
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            Long item = timestamp - 1;
            recorder.setNewestEventTimestamp(item);
            invokerSetEventTimestamp(recorder, timestamp);
            Assertions.assertEquals(timestamp, recorder.getNewestEventTimestamp());
        }

        /**测试 setEventTimestamp 方法, 边界：recorder中包含NewestEventTimestamp时间比timestamp大*/
        @Test
        public void testSetEventTimestamp4() {
            Long timestamp = System.currentTimeMillis();
            HandlerUtil.EventTypeRecorder recorder = new HandlerUtil.EventTypeRecorder();
            recorder.setOldestEventTimestamp(timestamp + 1);
            invokerSetEventTimestamp(recorder, timestamp);
            Assertions.assertEquals(timestamp, recorder.getNewestEventTimestamp());
        }

        private void invokerSetEventTimestamp(HandlerUtil.EventTypeRecorder recorder, Long ts){
            HandlerUtil.setEventTimestamp(recorder, ts);
        }
    }

    @Nested
    class HandlerUtilEventTypeRecorderTest {
        HandlerUtil.EventTypeRecorder recorder;
        @BeforeEach
        public void init() {
            recorder = new HandlerUtil.EventTypeRecorder();
        }

        @Test
        public void testDefaultMemoryUtil() {
            Assertions.assertNotNull(recorder.getMemoryUtil());
            Assertions.assertEquals("B", recorder.getMemoryUtil());
        }

        @Test
        public void testIncrDdlTotal() {
            recorder.incrDdlTotal();
            Assertions.assertEquals(1, recorder.getDdlTotal());
        }
        @Test
        public void testIncrInsertTotal() {
            recorder.incrInsertTotal();
            Assertions.assertEquals(1, recorder.getInsertTotal());
        }
        @Test
        public void testIncrUpdateTotal() {
            recorder.incrUpdateTotal();
            Assertions.assertEquals(1, recorder.getUpdateTotal());
        }
        @Test
        public void testIncrDeleteTotal() {
            recorder.incrDeleteTotal();
            Assertions.assertEquals(1, recorder.getDeleteTotal());
        }
        @Test
        public void testIcrOthersTotal() {
            recorder.incrOthersTotal();
            Assertions.assertEquals(1, recorder.getOthersTotal());
        }
        @Test
        public void testGetTotal() {
            recorder.incrDdlTotal();
            long all = recorder.getUpdateTotal() + recorder.getDeleteTotal() + recorder.getDdlTotal() + recorder.getInsertTotal() + recorder.getOthersTotal();
            Assertions.assertEquals(all, recorder.getTotal());
        }

        @Test
        public void testIncrProcessTimeTotal() {
            Long now = System.currentTimeMillis();
            Long time = System.currentTimeMillis() - 10;
            recorder.incrProcessTimeTotal(now, time);
            Assertions.assertNotNull(recorder.getProcessTimeTotal());
            Assertions.assertEquals(new Long(now-time), recorder.getProcessTimeTotal());
        }
        @Test
        public void testIncrProcessTimeTotal0() {
            recorder.incrProcessTimeTotal(System.currentTimeMillis(), null);
            Assertions.assertNull(recorder.getProcessTimeTotal());
        }
        
        @Test
        public void testIncrProcessTimeTotal1() {
            recorder.setProcessTimeTotal(null);
            Long now = System.currentTimeMillis();
            Long time = System.currentTimeMillis() - 10;
            recorder.incrProcessTimeTotal(now, time);
            Assertions.assertNotNull(recorder.getProcessTimeTotal());
            Assertions.assertEquals(new Long(now-time), recorder.getProcessTimeTotal());
        }

        @Test
        public void testCalculateMaxReplicateLag(){
            Long time = System.currentTimeMillis();
            List<Long> list = new ArrayList<>();
            list.add(System.currentTimeMillis() - 10);
            recorder.calculateMaxReplicateLag(time, list);
            Assertions.assertNotNull(recorder.getReplicateLagTotal());
            Assertions.assertEquals(new Long(10), recorder.getReplicateLagTotal());
        }
        @Test
        public void testCalculateMaxReplicateLag0(){
            Long time = System.currentTimeMillis();
            List<Long> list = null;
            recorder.calculateMaxReplicateLag(time, list);
            Assertions.assertNull(recorder.getReplicateLagTotal());
        }
        @Test
        public void testCalculateMaxReplicateLag1(){
            Long time = System.currentTimeMillis();
            List<Long> list = new ArrayList<>();
            recorder.calculateMaxReplicateLag(time, list);
            Assertions.assertNull(recorder.getReplicateLagTotal());
        }
        @Test
        public void testCalculateMaxReplicateLag2(){
            Long time = System.currentTimeMillis();
            List<Long> list = new ArrayList<>();
            list.add(time - 10);
            list.add(null);
            list.add(time -1);
            recorder.calculateMaxReplicateLag(time, list);
            Assertions.assertNotNull(recorder.getReplicateLagTotal());
            Assertions.assertEquals(new Long(10), recorder.getReplicateLagTotal());
        }
    }
}
