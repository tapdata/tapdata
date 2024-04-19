package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.BaseTest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.tm.commons.dag.UnwindModel;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.unwind.EventHandel;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.unwind.UnWindNodeUtil;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.*;

import static com.tapdata.tm.sdk.util.JacksonUtil.toJson;


public class UnwindNodeTest extends BaseTest {

    /**
     * 依次测试节点的各个属性值
     * */
    @Test
    public void unwindConfigTest() throws JsonProcessingException {
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
        node.setUnwindModel(UnwindModel.EMBEDDED);
        node.setPath("field");
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 2;
        Assert.assertTrue(
                "Fail get 2 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "\"{\"field\":1,\"id\":1}\" , but the result is  " + toJson(before0), "{\"field\":1,\"id\":1}", toJson(before0));

        TapEvent tapEvent1 = handelResult.get(1);
        boolean type1 = tapEvent1 instanceof TapDeleteRecordEvent;
        Assert.assertTrue("ail translate insert event to delete event after unwind node for the second event", type1);
        Map<String, Object> before1 = ((TapDeleteRecordEvent) tapEvent1).getBefore();
        Assert.assertEquals(" The content of the second delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "\"{\"field\":2,\"id\":1}\" , but the result is  " + toJson(before1), "{\"field\":2,\"id\":1}", toJson(before1));

        /**
         * unwind with IncludeArrayIndex
         * */
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 2;
        Assert.assertTrue(
                "Fail get 2 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":1,\"index\":0,\"id\":1} , but the result is  " + toJson(before0), "{\"field\":1,\"index\":0,\"id\":1}", toJson(before0));

        tapEvent1 = handelResult.get(1);
        type1 = tapEvent1 instanceof TapDeleteRecordEvent;
        Assert.assertTrue("ail translate insert event to delete event after unwind node for the second event", type1);
        before1 = ((TapDeleteRecordEvent) tapEvent1).getBefore();
        Assert.assertEquals(" The content of the second delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":2,\"index\":1,\"id\":1}, but the result is  " + toJson(before1), "{\"field\":2,\"index\":1,\"id\":1}", toJson(before1));


        /**
         * unwind with IncludeArrayIndex and PreserveNullAndEmptyArrays
         * */
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 2;
        Assert.assertTrue(
                "Fail get 2 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":1,\"index\":0,\"id\":1}, but the result is  " + toJson(before0), "{\"field\":1,\"index\":0,\"id\":1}", toJson(before0));

        tapEvent1 = handelResult.get(1);
        type1 = tapEvent1 instanceof TapDeleteRecordEvent;
        Assert.assertTrue("ail translate insert event to delete event after unwind node for the second event", type1);
        before1 = ((TapDeleteRecordEvent) tapEvent1).getBefore();
        Assert.assertEquals(" The content of the second delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":2,\"index\":1,\"id\":1}, but the result is  " + toJson(before1), "{\"field\":2,\"index\":1,\"id\":1}", toJson(before1));
    }

    /**
     * 测试源事件不包含path值的情况
     * */
    @Test
    public void emptyValueOfSinglePathWithUnwind() throws JsonProcessingException {
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
        node.setPath("field1");

        //不指定索引字段，忽略null或empty
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 0;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        //不指定索引字段，不忽略null或empty
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":[1,2],\"id\":1} , but the result is  " + toJson(before0), "{\"field\":[1,2],\"id\":1}", toJson(before0));


        //指定索引字段，不忽略null或empty
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":[1,2],\"id\":1} , but the result is  " + toJson(before0), "{\"field\":[1,2],\"id\":1}", toJson(before0));

    }

    /**
     * 测试源事件path值为null情况
     * */
    @Test
    public void nullValueOfSinglePathWithUnwind() throws JsonProcessingException {
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        before.put("field", null);
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());

        UnwindProcessNode node = new UnwindProcessNode();
        node.setUnwindModel(UnwindModel.EMBEDDED);
        node.setPath("field");

        //不指定索引字段，忽略null或empty
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 0;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        //不指定索引字段，不忽略null或empty
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":null,\"id\":1} , but the result is  " + toJson(before0), "{\"field\":null,\"id\":1}", toJson(before0));


        //指定索引字段，不忽略null或empty
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":null,\"index\":null,\"id\":1}, but the result is  " + toJson(before0),
                "{\"field\":null,\"index\":null,\"id\":1}",
                toJson(before0));

    }

    /**
     * 测试源事件path值为null情况
     * */
    @Test
    public void emptyListValueOfSinglePathWithUnwind() throws JsonProcessingException {
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        before.put("field", new ArrayList<>());
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());

        UnwindProcessNode node = new UnwindProcessNode();
        node.setPath("field");
        node.setUnwindModel(UnwindModel.EMBEDDED);

        //不指定索引字段，忽略null或empty
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 0;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        //不指定索引字段，不忽略null或empty
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"id\":1} , but the result is  " + toJson(before0), "{\"id\":1}", toJson(before0));


        //指定索引字段，不忽略null或empty
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"id\":1} , but the result is  " + toJson(before0), "{\"id\":1}", toJson(before0));

    }

    /**
     * 测试源事件path值为非数组或列表情况
     * */
    @Test
    public void unListValueOfSinglePathWithUnwind() throws JsonProcessingException {
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        before.put("field", "test case by gavin");
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());

        UnwindProcessNode node = new UnwindProcessNode();
        node.setUnwindModel(UnwindModel.EMBEDDED);
        node.setPath("field");

        //不指定索引字段，忽略null或empty
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":\"test case by gavin\",\"id\":1} , but the result is  " + toJson(before0), "{\"field\":\"test case by gavin\",\"id\":1}", toJson(before0));


        //指定索引字段，不忽略null或empty
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":\"test case by gavin\",\"index\":null,\"id\":1} , but the result is  " + toJson(before0), "{\"field\":\"test case by gavin\",\"index\":null,\"id\":1}", toJson(before0));

        //不指定索引字段，不忽略null或empty
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":\"test case by gavin\",\"index\":null,\"id\":1} , but the result is  " + toJson(before0), "{\"field\":\"test case by gavin\",\"index\":null,\"id\":1}", toJson(before0));

    }


    /**
     * 多层path测试源事件不包含path值的情况
     * */
    @Test
    public void emptyValueOfManyPathWithUnwind() throws JsonProcessingException {
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        Map<String, Object> map = new HashMap<>();
        map.put("key", "007");
        before.put("field", map);
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());

        UnwindProcessNode node = new UnwindProcessNode();
        node.setUnwindModel(UnwindModel.EMBEDDED);
        node.setPath("field.array");

        //不指定索引字段，忽略null或empty
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 0;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        //不指定索引字段，不忽略null或empty
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"key\":\"007\"},\"id\":1} , but the result is  " + toJson(before0), "{\"field\":{\"key\":\"007\"},\"id\":1}", toJson(before0));


        //指定索引字段，不忽略null或empty
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"key\":\"007\"},\"id\":1} , but the result is  " + toJson(before0), "{\"field\":{\"key\":\"007\"},\"id\":1}", toJson(before0));

    }

    /**
     * 多层path测试源事件path值为null情况
     * */
    @Test
    public void nullValueOfManyPathWithUnwind() throws JsonProcessingException {
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        Map<String, Object> map = new HashMap<>();
        map.put("key", "007");
        map.put("array", null);
        before.put("field", map);
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());

        UnwindProcessNode node = new UnwindProcessNode();
        node.setUnwindModel(UnwindModel.EMBEDDED);
        node.setPath("field.array");

        //不指定索引字段，忽略null或empty
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 0;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        //不指定索引字段，不忽略null或empty
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"array\":null,\"key\":\"007\"},\"id\":1} , but the result is  " + toJson(before0), "{\"field\":{\"array\":null,\"key\":\"007\"},\"id\":1}", toJson(before0));


        //指定索引字段，不忽略null或empty
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"array\":null,\"index\":null,\"key\":\"007\"},\"id\":1}, but the result is  " + toJson(before0),
                "{\"field\":{\"array\":null,\"index\":null,\"key\":\"007\"},\"id\":1}",
                toJson(before0));

    }

    /**
     * 多层path测试源事件path值为null情况
     * */
    @Test
    public void emptyListValueOfManyPathWithUnwind() throws JsonProcessingException {
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        Map<String, Object> map = new HashMap<>();
        map.put("key", "007");
        map.put("array", new ArrayList<Object>());
        before.put("field", map);
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());

        UnwindProcessNode node = new UnwindProcessNode();
        node.setUnwindModel(UnwindModel.EMBEDDED);
        node.setPath("field.array");

        //不指定索引字段，忽略null或empty
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 0;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        //不指定索引字段，不忽略null或empty
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"key\":\"007\"},\"id\":1} , but the result is  " + toJson(before0), "{\"field\":{\"key\":\"007\"},\"id\":1}", toJson(before0));


        //指定索引字段，不忽略null或empty
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"key\":\"007\"},\"id\":1} , but the result is  " + toJson(before0), "{\"field\":{\"key\":\"007\"},\"id\":1}", toJson(before0));

    }

    /**
     * 多层path测试源事件path值为非数组或列表情况
     * */
    @Test
    public void unListValueOfManyPathWithUnwind() throws JsonProcessingException {
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        Map<String, Object> map = new HashMap<>();
        map.put("key", "007");
        map.put("array", "test case by gavin");
        before.put("field", map);
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());

        UnwindProcessNode node = new UnwindProcessNode();
        node.setUnwindModel(UnwindModel.EMBEDDED);
        node.setPath("field.array");

        //不指定索引字段，忽略null或empty
        List<TapEvent> handelResult = EventHandel.getHandelResult(node, event);
        boolean count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        TapEvent tapEvent0 = handelResult.get(0);
        boolean type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        Map<String, Object> before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"array\":\"test case by gavin\",\"key\":\"007\"},\"id\":1}, but the result is  " + toJson(before0), "{\"field\":{\"array\":\"test case by gavin\",\"key\":\"007\"},\"id\":1}", toJson(before0));


        //指定索引字段，不忽略null或empty
        node.setIncludeArrayIndex("index");
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);

        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"array\":\"test case by gavin\",\"index\":null,\"key\":\"007\"},\"id\":1} , but the result is  " + toJson(before0),
                "{\"field\":{\"array\":\"test case by gavin\",\"index\":null,\"key\":\"007\"},\"id\":1}",
                toJson(before0));

        //不指定索引字段，不忽略null或empty
        node.setPreserveNullAndEmptyArrays(true);
        handelResult = EventHandel.getHandelResult(node, event);
        count = handelResult.size() == 1;
        Assert.assertTrue(
                "Fail get 1 delete event from tapdata event by unwind node, from event: " + toJson(before)
                        + ", only " + handelResult.size() + " after unwind node",
                count);
        tapEvent0 = handelResult.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate delete event to insert event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first delete event after processing by the Unwind node does not meet expectations,  It should be " +
                "{\"field\":{\"array\":\"test case by gavin\",\"index\":null,\"key\":\"007\"},\"id\":1}, but the result is  " + toJson(before0),
                "{\"field\":{\"array\":\"test case by gavin\",\"index\":null,\"key\":\"007\"},\"id\":1}",
                toJson(before0));

    }

    /**
     * 删除事件的测试
     * */
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
        node.setUnwindModel(UnwindModel.EMBEDDED);
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

    /**
     * 新增事件的测试
     * */
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
        node.setUnwindModel(UnwindModel.EMBEDDED);
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

    /**
     * 修改事件的测试
     * */
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
        node.setUnwindModel(UnwindModel.EMBEDDED);
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
        count = handelResult1.size() == 2;
        Assert.assertTrue(
                "Fail get 1 event from tapdata event by unwind node about update event which not have before map, from event: " + toJson(after)
                        + ", only " + handelResult1.size() + " after unwind node",
                count);
        tapEvent0 = handelResult1.get(0);
        type0 = tapEvent0 instanceof TapDeleteRecordEvent;
        Assert.assertTrue(
                "Fail translate update event to one delete event after unwind node for the first event",
                type0);
        before0 = ((TapDeleteRecordEvent) tapEvent0).getBefore();
        Assert.assertEquals(" The content of the first inserted event after processing by the Uwind node does not meet expectations,  It should be " +
                "\"{\"field\":2,\"id\":1}\" , but the result is  " + toJson(before0), "{\"field\":2,\"id\":1}", toJson(before0));

        tapEvent1 = handelResult1.get(1);
        type1 = tapEvent1 instanceof TapInsertRecordEvent;
        Assert.assertTrue("ail translate update event to insert event after unwind node for the second event", type1);
        after1 = ((TapInsertRecordEvent) tapEvent1).getAfter();
        Assert.assertEquals(" The content of the second inserted event after processing by the Uwind node does not meet expectations,  It should be " +
                "\"{\"field\":2,\"id\":1}\" , but the result is  " + toJson(after1), "{\"field\":2,\"id\":1}", toJson(after1));

    }

    /**
     * 性能检查
     * */
    @Test
    public void performanceTesting() {
        List<TapEvent> events = new ArrayList();
        final int arraySize = 5000;
        final int itemSize = 25;
        for ( int index = 0;  index < arraySize; index++) {
            Map<String, Object> before = new HashMap<>();
            before.put("id", 1);
            List<Integer> arr = new ArrayList<>();
            for (int i = 0; i < itemSize; i++) {
                arr.add(new Random().nextInt());
            }
            before.put("field", arr);
            TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
            event.before(before);
            event.setReferenceTime(System.currentTimeMillis());
            events.add(event);
        }
        UnwindProcessNode node = new UnwindProcessNode();
        node.setUnwindModel(UnwindModel.EMBEDDED);
        node.setPath("field");
        long start = System.currentTimeMillis();
        for (TapEvent event : events) {
            EventHandel.getHandelResult(node, event);
        }
        long end = System.currentTimeMillis();
        long cost = end-start;
        //  x = 1000*1000/cost*1000   1000/56 * 1000
        System.out.println("=========[Performance Test of Unwind Node]===========");
        System.out.println("\t- The performance test results are as follows: " +
                         "\n\t    Cost time: " + cost + " ms about " + arraySize  + " records and each record has " + itemSize +" items in array");
        System.out.println("\t- QPS is approximately: " +
                (new DecimalFormat("0.000")).format(((double)( Float.parseFloat((new DecimalFormat("0.0000000000")).format(arraySize / cost)) * 1000)) / 10000) + " w/s");
        System.out.println("=====================================================");
    }
    @Test
    public void testFlattenMapIsNotNull(){
        Map<String, Object> record = new HashMap<>();
        record.put("t","test");
        Document document = new Document("id","id").append("name","test");
        UnWindNodeUtil.serializationFlattenFields("t",record,document,true,"_");
        Assert.assertEquals(record.get("t_id"),"id");
        Assert.assertEquals(record.get("t_name"),"test");

    }

    @Test
    public void testFlattenMapIsNull(){
        Map<String, Object> record = new HashMap<>();
        record.put("t","test");
        Document document = new Document("id","id").append("name","test");
        UnWindNodeUtil.serializationFlattenFields("t",record,document,true,null);
        Assert.assertFalse(record.containsKey("t_id"));
    }

    @Test
    public void testObjectIsNull(){
        Map<String, Object> record = new HashMap<>();
        record.put("t","test");
        UnWindNodeUtil.serializationFlattenFields("t",record,null,true,"_");
        Assert.assertFalse(record.containsKey("t_id"));
    }

    @Test
    public void testRecordIsNull(){
        Document document = new Document("id","id").append("name","test");
        UnWindNodeUtil.serializationFlattenFields("t",null,document,true,"_");
        Assert.assertEquals(document,document);
    }

    @Test
    public void testFlattenIsNullObject(){
        Map<String, Object> record = new HashMap<>();
        record.put("t","t");
        String s = "test";
        UnWindNodeUtil.serializationFlattenFields("t",record,s,true,"_");
        Assert.assertEquals("test",record.get("t"));
    }

    @Test
    public void testContainsPathAndSetValueFlattenIsTrue(){
        Map<String, Object> record = new HashMap<>();
        record.put("t","test");
        Document document = new Document("id","id").append("name","test");
        Map<String,Object> result = UnWindNodeUtil.containsPathAndSetValue("t",record,document,"",1,true,"_");
        Assert.assertEquals(result.get("t_id"),"id");
        Assert.assertEquals(result.get("t_name"),"test");
    }

    @Test
    public void testContainsPathAndSetValueFlattenIsFalse(){
        Map<String, Object> record = new HashMap<>();
        record.put("t","test");
        Document document = new Document("id","id").append("name","test");
        Map<String,Object> result = UnWindNodeUtil.containsPathAndSetValue("t.id",record,document,"",1,false,"_");
        Assert.assertNull(result);
    }

    @Test
    public void testContainsPathAndSetValueFlattenIsFalseAndObjectIsNotNull(){
        Map<String, Object> record = new HashMap<>();
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("id","test");
        record.put("t",objectMap);
        Document document = new Document("id","id").append("name","test");
        Map<String,Object> result = UnWindNodeUtil.containsPathAndSetValue("t.id",record,document,"",1,false,"_");
        Assert.assertNotNull(result);
    }
    @Test
    public void testArray(){
        Object[] objects = {};
        Map<String, Object> map = new HashMap<>();
        map.put("t","test");
        Map<String, Object> parentMap = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        List<Integer> arr = new ArrayList<>();
        arr.add(1);
        arr.add(2);
        before.put("field", arr);
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());
        EventHandel handel = new EventHandel() {
            @Override
            public List<TapEvent> handel(UnwindProcessNode node, TapEvent event) {
                return null;
            }

            @Override
            public void copyEvent(List<TapEvent> events, Map<String, Object> item, TapEvent event) {

            }
        };
        boolean result = UnWindNodeUtil.array(objects,new ArrayList<>(),"test",true,map,"t",parentMap,new String[]{""},event,handel,false,null);
        Assert.assertTrue(result);
    }

    @Test
    public void testArrayIsNotNull(){
        Object[] objects = {"test1","test2"};
        Map<String, Object> map = new HashMap<>();
        map.put("t","test");
        Map<String, Object> parentMap = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put("id", 1);
        List<Integer> arr = new ArrayList<>();
        arr.add(1);
        arr.add(2);
        before.put("field", arr);
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create();
        event.before(before);
        event.setReferenceTime(System.currentTimeMillis());
        EventHandel handel = new EventHandel() {
            @Override
            public List<TapEvent> handel(UnwindProcessNode node, TapEvent event) {
                return null;
            }

            @Override
            public void copyEvent(List<TapEvent> events, Map<String, Object> item, TapEvent event) {

            }
        };
        boolean result = UnWindNodeUtil.array(objects,new ArrayList<>(),"test",true,map,"t",parentMap,new String[]{""},event,handel,false,null);
        Assert.assertFalse(result);
    }
}
