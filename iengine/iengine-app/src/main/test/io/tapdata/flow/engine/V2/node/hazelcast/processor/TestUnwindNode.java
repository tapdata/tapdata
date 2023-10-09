package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.unwind.EventHandel;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tapdata.tm.sdk.util.JacksonUtil.toJson;


public class TestUnwindNode {

    @Test
    public void deleteEventWithUnwindNode() throws JsonProcessingException {
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        List<Integer> arr = new ArrayList<>();
        arr.add(1);
        arr.add(2);
        before.put("field", arr);
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());
        UnwindProcessNode node = new UnwindProcessNode();
        node.setPath("field");
        List<TapEvent> handelResult1 = EventHandel.getHandelResult(node, event);
        boolean count = handelResult1.size() == 2;
        Assert.assertTrue(
                "Fail get 2 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult1.size() + " after unwind node",
                count);

        TapEvent tapEvent0 = handelResult1.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "\"{\"field\":1,\"id\":1}\" , but the result is  " + toJson(before0), "{\"field\":1,\"id\":1}", toJson(before0));

        TapEvent tapEvent1 = handelResult1.get(1);
        boolean type1 = tapEvent1 instanceof TapDeleteRecordEvent;
        Assert.assertTrue("ail translate insert event to delete event after unwind node for the second event", type1);
        Map<String, Object> before1 = ((TapDeleteRecordEvent) tapEvent1).getBefore();
        Assert.assertEquals(" The content of the second delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "\"{\"field\":2,\"id\":1}\" , but the result is  " + toJson(before1), "{\"field\":2,\"id\":1}", toJson(before1));

    }

    @Test
    public void insertEventWithUnwindNode() throws JsonProcessingException {
        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        List<Integer> arr = new ArrayList<>();
        arr.add(1);
        arr.add(2);
        after.put("field", arr);
        TapInsertRecordEvent event = TapInsertRecordEvent.create();
        event.after(after);
        event.setReferenceTime(System.currentTimeMillis());
        UnwindProcessNode node = new UnwindProcessNode();
        node.setPath("field");
        List<TapEvent> handelResult1 = EventHandel.getHandelResult(node, event);
        boolean count = handelResult1.size() == 2;
        Assert.assertTrue(
                "Fail get 2 insert event from tapdata event by unwind node, from event: " + toJson(after)
                        + ", only " + handelResult1.size() + " after unwind node",
                count);

        TapEvent tapEvent0 = handelResult1.get(0);
        boolean type0 = tapEvent0 instanceof TapInsertRecordEvent;
        Assert.assertTrue(
                "Fail translate insert event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> after0 = ((TapInsertRecordEvent) tapEvent0).getAfter();
        Assert.assertEquals(" The content of the first inserted event after processing by the Uwind node does not meet expectations,  It should be " +
                "\"{\"field\":1,\"id\":1}\" , but the result is  " + toJson(after0), "{\"field\":1,\"id\":1}", toJson(after0));

        TapEvent tapEvent1 = handelResult1.get(1);
        boolean type1 = tapEvent1 instanceof TapInsertRecordEvent;
        Assert.assertTrue("ail translate insert event to insert event after unwind node for the second event", type1);
        Map<String, Object> after1 = ((TapInsertRecordEvent) tapEvent1).getAfter();
        Assert.assertEquals(" The content of the second inserted event after processing by the Uwind node does not meet expectations,  It should be " +
                "\"{\"field\":2,\"id\":1}\" , but the result is  " + toJson(after1), "{\"field\":2,\"id\":1}", toJson(after1));



        node.setIncludeArrayIndex("index");
        List<TapEvent> handelResult2 = EventHandel.getHandelResult(node, event);
        count = handelResult2.size() == 2;
        Assert.assertTrue(
                "Fail get 2 insert event from tapdata event by unwind node, from event: " + toJson(after)
                        + ", only " + handelResult1.size() + " after unwind node",
                count);

        tapEvent0 = handelResult2.get(0);

        type0 = tapEvent0 instanceof TapInsertRecordEvent;
        Assert.assertTrue(
                "Fail translate insert event to insert event after unwind node for the first event",
                type0);

        after0 = ((TapInsertRecordEvent) tapEvent0).getAfter();
        Assert.assertEquals(" The content of the first inserted event after processing by the Uwind node does not meet expectations,  It should be " +
                "{\"field\":1,\"index\":0,\"id\":1} , but the result is  " + toJson(after0), "{\"field\":1,\"index\":0,\"id\":1}", toJson(after0));

        tapEvent1 = handelResult2.get(1);

        type1 = tapEvent1 instanceof TapInsertRecordEvent;
        Assert.assertTrue("Fail", type1);

        after1 = ((TapInsertRecordEvent) tapEvent1).getAfter();
        Assert.assertEquals(" The content of the second inserted event after processing by the Uwind node does not meet expectations,  It should be " +
                "{\"field\":2,\"index\":1,\"id\":1} , but the result is  " + toJson(after1), "{\"field\":2,\"index\":1,\"id\":1}", toJson(after1));

    }

    @Test
    public void updateEventWithUnwindNode() throws JsonProcessingException {

        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        List<Integer> arr0 = new ArrayList<>();
        arr0.add(1);
        before.put("field", arr0);

        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        List<Integer> arr = new ArrayList<>();
        arr.add(2);
        after.put("field", arr);

        TapUpdateRecordEvent event = TapUpdateRecordEvent.create();
        event.after(after);
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());
        UnwindProcessNode node = new UnwindProcessNode();
        node.setPath("field");

        /**
         * update event with before and after
         * */
        List<TapEvent> handelResult1 = EventHandel.getHandelResult(node, event);
        boolean count = handelResult1.size() == 2;
        Assert.assertTrue(
                "Fail get 2 event from tapdata event by unwind node, from event: " + toJson(after)
                        + ", only " + handelResult1.size() + " after unwind node",
                count);

        TapEvent tapEvent0 = handelResult1.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate update event to one delete event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first inserted event after processing by the Uwind node does not meet expectations,  It should be " +
                "\"{\"field\":1,\"id\":1}\" , but the result is  " + toJson(before0), "{\"field\":1,\"id\":1}", toJson(before0));

        TapEvent tapEvent1 = handelResult1.get(1);
        boolean type1 = tapEvent1 instanceof TapInsertRecordEvent;
        Assert.assertTrue("ail translate update event to insert event after unwind node for the second event", type1);
        Map<String, Object> after1 = ((TapInsertRecordEvent) tapEvent1).getAfter();
        Assert.assertEquals(" The content of the second inserted event after processing by the Uwind node does not meet expectations,  It should be " +
                "\"{\"field\":2,\"id\":1}\" , but the result is  " + toJson(after1), "{\"field\":2,\"id\":1}", toJson(after1));


        /**
         * update event with empty before
         * */
        event.before(null);
        handelResult1 = EventHandel.getHandelResult(node, event);
        count = handelResult1.size() == 1;
        Assert.assertTrue(
                "Fail get 1 event from tapdata event by unwind node about update event which not have before map, from event: " + toJson(after)
                        + ", only " + handelResult1.size() + " after unwind node",
                count);
        tapEvent0 = handelResult1.get(0);
        type0 = tapEvent0 instanceof TapUpdateRecordEvent;
        Assert.assertTrue(
                "Fail translate update event to one delete event after unwind node for the first event",
                type0);
        before0 = ((TapUpdateRecordEvent) tapEvent0).getBefore();
        Map<String, Object> after0 = ((TapUpdateRecordEvent) tapEvent0).getAfter();
        Assert.assertEquals(" The content of the first inserted event after processing by the Unwind node does not meet expectations,  It should be " +
                "\"{\"field\":2,\"id\":1}\" , but the result is  " + toJson(after0), "{\"field\":2,\"id\":1}", toJson(after0));
        Assert.assertEquals(" The content of the first inserted event before processing by the Unwind node does not meet expectations,  It should be " +
                "null , but the result is  " + toJson(before0), "null", toJson(before0));

    }

    @Test
    public void performanceTesting() {
        List<TapEvent> events = new ArrayList();
        for ( int index = 0;  index < 10000; index++) {
            Map<String, Object> before = new HashMap<>();
            before.put("id", 1);
            List<Integer> arr = new ArrayList<>();
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            arr.add(1);
            arr.add(2);
            before.put("field", arr);
            TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
            event.before(before);
            event.setReferenceTime(System.currentTimeMillis());
            events.add(event);
        }
        UnwindProcessNode node = new UnwindProcessNode();
        node.setPath("field");
        long start = System.currentTimeMillis();
        for (TapEvent event : events) {
            EventHandel.getHandelResult(node, event);
        }
        long end = System.currentTimeMillis();
        System.out.println("Cost time: " + (end-start) + " ms about 1000 records");
    }
}
