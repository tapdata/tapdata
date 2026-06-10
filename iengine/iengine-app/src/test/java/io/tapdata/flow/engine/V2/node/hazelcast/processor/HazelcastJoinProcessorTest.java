package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import base.hazelcast.BaseHazelcastNodeTest;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TransformToTapValueResult;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.BytesIMap;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor;
import io.tapdata.schema.TapTableMap;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class HazelcastJoinProcessorTest extends BaseHazelcastNodeTest {
    HazelcastJoinProcessor hazelcastJoinProcessor;
    JoinProcessorNode joinProcessorNode;
    TapTable tapTable;
    @BeforeEach
    void beforeEach() {
        super.allSetup();
        joinProcessorNode = new JoinProcessorNode();
        joinProcessorNode.setId("node_test");
        joinProcessorNode.setJoinType("left");
        joinProcessorNode.setEmbeddedMode(false);
        JoinProcessorNode.JoinExpression joinExpression = new JoinProcessorNode.JoinExpression();
        joinExpression.setExpression("test");
        joinExpression.setLeft("k");
        joinExpression.setRight("k");
        joinProcessorNode.setJoinExpressions(Arrays.asList(joinExpression));
        joinProcessorNode.setLeftNodeId("leftNodeId");
        joinProcessorNode.setRightNodeId("rightNodeId");
        joinProcessorNode.setLeftPrimaryKeys(Arrays.asList("id"));
        joinProcessorNode.setRightPrimaryKeys(Arrays.asList("rid"));

        setupContext(joinProcessorNode);

        TapTableMap<String, TapTable> tapTableMap = mock(TapTableMap.class);
        when(processorBaseContext.getTapTableMap()).thenReturn(tapTableMap);
        when(tapTableMap.keySet()).thenReturn(new HashSet<>(Arrays.asList("left")));
        tapTable = mock(TapTable.class);
        when(tapTableMap.get("left")).thenReturn(tapTable);
        LinkedHashMap<String, io.tapdata.entity.schema.TapField> nameFieldMap = new LinkedHashMap<>();
        io.tapdata.entity.schema.TapField pkField = mock(io.tapdata.entity.schema.TapField.class);
        when(pkField.getPrimaryKeyPos()).thenReturn(1);
        when(pkField.getName()).thenReturn("id");
        io.tapdata.entity.schema.TapField rightField = mock(io.tapdata.entity.schema.TapField.class);
        when(rightField.getPrimaryKeyPos()).thenReturn(null);
        when(rightField.getName()).thenReturn("right");
        nameFieldMap.put("id", pkField);
        nameFieldMap.put("right", rightField);
        when(tapTable.getNameFieldMap()).thenReturn(nameFieldMap);
        hazelcastJoinProcessor = spy(new HazelcastJoinProcessor(processorBaseContext));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "mapIterator", new AllLayerMapIterator());
    }

    @Test
    void testDoInit(){
        doNothing().when(hazelcastJoinProcessor).initNode();
        hazelcastJoinProcessor.doInit(jetContext);
        Assertions.assertNotNull(ReflectionTestUtils.getField(hazelcastJoinProcessor,"mapIterator"));
    }

    @Test
    void testConstructor_WhenNoSchema_ShouldThrow() {
        TapTableMap<String, TapTable> tapTableMap = mock(TapTableMap.class);
        when(processorBaseContext.getTapTableMap()).thenReturn(tapTableMap);
        when(tapTableMap.keySet()).thenReturn(new HashSet<>());
        Assertions.assertThrows(RuntimeException.class, () -> new HazelcastJoinProcessor(processorBaseContext));
    }

    @Test
    void testInitNode_ShouldBuildCachesAndClearWhenTaskNotRun() throws Exception {
        JoinProcessorNode.EmbeddedSetting embeddedSetting = new JoinProcessorNode.EmbeddedSetting();
        embeddedSetting.setPath("embed");
        joinProcessorNode.setEmbeddedMode(true);
        joinProcessorNode.setEmbeddedSetting(embeddedSetting);
        joinProcessorNode.setJoinType("left");

        io.tapdata.entity.schema.TapIndex uniqueIndex = mock(io.tapdata.entity.schema.TapIndex.class);
        when(uniqueIndex.isUnique()).thenReturn(true);
        when(tapTable.getIndexList()).thenReturn(Arrays.asList(uniqueIndex));
        when(tapTable.primaryKeys()).thenReturn(Arrays.asList("id"));
        taskDto.setAttrs(new HashMap<>());

        try (MockedConstruction<BytesIMap> mocked = mockConstruction(BytesIMap.class, (mock, ctx) -> {
            doNothing().when(mock).clear();
        })) {
            hazelcastJoinProcessor.doInit(jetContext);
            Assertions.assertNotNull(ReflectionTestUtils.getField(hazelcastJoinProcessor, "leftJoinCache"));
            Assertions.assertNotNull(ReflectionTestUtils.getField(hazelcastJoinProcessor, "rightJoinCache"));
            Assertions.assertEquals("embed", ReflectionTestUtils.getField(hazelcastJoinProcessor, "embeddedPath"));
            Assertions.assertEquals(2, mocked.constructed().size());
            verify(mocked.constructed().get(0), times(1)).clear();
            verify(mocked.constructed().get(1), times(1)).clear();
        }
    }

    @Test
    void testClearCache_ShouldClearAndDestroyBothCaches() throws Exception {
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);

        try (MockedStatic<HazelcastUtil> hazelcastUtilMock = mockStatic(HazelcastUtil.class);
             MockedStatic<ExternalStorageUtil> extStorageMock = mockStatic(ExternalStorageUtil.class);
             MockedConstruction<BytesIMap> mocked = mockConstruction(BytesIMap.class, (mock, ctx) -> {
                 doNothing().when(mock).clear();
                 doNothing().when(mock).destroy();
             })) {
            hazelcastUtilMock.when(HazelcastUtil::getInstance).thenReturn(hazelcastInstance);
            extStorageMock.when(() -> ExternalStorageUtil.getExternalStorage(joinProcessorNode)).thenReturn(externalStorageDto);

            HazelcastJoinProcessor.clearCache(joinProcessorNode);
            Assertions.assertEquals(2, mocked.constructed().size());
            verify(mocked.constructed().get(0), times(1)).clear();
            verify(mocked.constructed().get(0), times(1)).destroy();
            verify(mocked.constructed().get(1), times(1)).clear();
            verify(mocked.constructed().get(1), times(1)).destroy();
        }
    }

    @Test
    void testClearCache_WhenNotJoinNode_ShouldReturn() {
        HazelcastJoinProcessor.clearCache(mock(Node.class));
    }

    @Test
    void testClearCache_WhenClearThrows_ShouldThrow() {
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        try (MockedStatic<HazelcastUtil> hazelcastUtilMock = mockStatic(HazelcastUtil.class);
             MockedStatic<ExternalStorageUtil> extStorageMock = mockStatic(ExternalStorageUtil.class);
             MockedConstruction<BytesIMap> mocked = mockConstruction(BytesIMap.class, (mock, ctx) -> {
                 org.mockito.Mockito.doThrow(new RuntimeException("x")).when(mock).clear();
             })) {
            hazelcastUtilMock.when(HazelcastUtil::getInstance).thenReturn(hazelcastInstance);
            extStorageMock.when(() -> ExternalStorageUtil.getExternalStorage(joinProcessorNode)).thenReturn(externalStorageDto);
            Assertions.assertThrows(RuntimeException.class, () -> HazelcastJoinProcessor.clearCache(joinProcessorNode));
        }
    }

    @Test
    void testTransformDateTime(){
        ReflectionTestUtils.setField(hazelcastJoinProcessor,"mapIterator",new AllLayerMapIterator());
        Map<String, Object> before = new HashMap<>();
        before.put("date",new DateTime(new Date()));
        before.put("text","text");
        Map<String, Object> after = new HashMap<>();
        after.put("date",new DateTime(new Date()));
        after.put("text","text");
        hazelcastJoinProcessor.transformDateTime(before,after);
        Assertions.assertInstanceOf(Date.class, before.get("date"));
        Assertions.assertInstanceOf(Date.class, after.get("date"));
    }

    @Test
    void testNeedCopyBatchEventWrapper(){
        Assertions.assertTrue(hazelcastJoinProcessor.needCopyBatchEventWrapper());
    }

    @Test
    void testPkChecker_MissingLeftPk_ShouldThrow() throws Exception {
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", new ArrayList<>());
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("pkChecker");
        m.setAccessible(true);
        Assertions.assertThrows(RuntimeException.class, () -> {
            try {
                m.invoke(hazelcastJoinProcessor);
            } catch (Exception e) {
                throw (RuntimeException) e.getCause();
            }
        });
    }

    @Test
    void testPkChecker_MissingRightPk_ShouldThrow() throws Exception {
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", Arrays.asList("id"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", new ArrayList<>());
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("pkChecker");
        m.setAccessible(true);
        Assertions.assertThrows(RuntimeException.class, () -> {
            try {
                m.invoke(hazelcastJoinProcessor);
            } catch (Exception e) {
                throw (RuntimeException) e.getCause();
            }
        });
    }

    @Test
    void testVerifyJoinNode_WhenDisabled_ShouldReturnFalse() throws Exception {
        JoinProcessorNode node = mock(JoinProcessorNode.class);
        when(node.disabledNode()).thenReturn(true);
        when(node.getLeftNodeId()).thenReturn("l");
        when(node.getRightNodeId()).thenReturn("r");
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("verifyJoinNode", Node.class);
        m.setAccessible(true);
        Object r = m.invoke(hazelcastJoinProcessor, node);
        Assertions.assertEquals(Boolean.FALSE, r);
    }

    @Test
    void testVerifyPreNodeHasDisabled_WhenDagThrows_ShouldReturnTrue() throws Exception {
        JoinProcessorNode node = mock(JoinProcessorNode.class);
        com.tapdata.tm.commons.dag.DAG dag = mock(com.tapdata.tm.commons.dag.DAG.class);
        when(node.getDag()).thenReturn(dag);
        when(dag.getNode("l")).thenThrow(new RuntimeException("x"));
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("verifyPreNodeHasDisabled", Node.class, String.class);
        m.setAccessible(true);
        Object r = m.invoke(hazelcastJoinProcessor, node, "l");
        Assertions.assertEquals(Boolean.TRUE, r);
    }

    @Test
    void testVerifyPreNodeHasDisabled_WhenPreNodeEnabled_ShouldReturnFalse() throws Exception {
        JoinProcessorNode node = mock(JoinProcessorNode.class);
        com.tapdata.tm.commons.dag.DAG dag = mock(com.tapdata.tm.commons.dag.DAG.class);
        Node preNode = mock(Node.class);
        when(preNode.disabledNode()).thenReturn(false);
        when(node.getDag()).thenReturn(dag);
        when(dag.getNode("l")).thenReturn((Node) preNode);
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("verifyPreNodeHasDisabled", Node.class, String.class);
        m.setAccessible(true);
        Object r = m.invoke(hazelcastJoinProcessor, node, "l");
        Assertions.assertEquals(Boolean.FALSE, r);
    }

    @Test
    void testTryProcessBatch_Empty_ShouldNotEmit() throws Exception {
        List<List<HazelcastProcessorBaseNode.BatchProcessResult>> accepted = new ArrayList<>();
        invokeTryProcessBatch(hazelcastJoinProcessor, new ArrayList<>(), accepted::add);
        Assertions.assertTrue(accepted.isEmpty());
    }

    @Test
    void testEachJoinResult_Empty_ShouldReturn() throws Exception {
        List<HazelcastProcessorBaseNode.BatchProcessResult> out = new ArrayList<>();
        var m = HazelcastJoinProcessor.class.getDeclaredMethod(
                "eachJoinResult",
                List.class,
                TapRecordEvent.class,
                TapdataEvent.class,
                List.class,
                Consumer.class,
                Node.class
        );
        m.setAccessible(true);
        m.invoke(
                hazelcastJoinProcessor,
                new ArrayList<>(),
                new TapInsertRecordEvent(),
                new TapdataEvent(),
                out,
                (Consumer<List<HazelcastProcessorBaseNode.BatchProcessResult>>) bs -> { },
                processorBaseContext.getNode()
        );
        Assertions.assertTrue(out.isEmpty());
    }

    @Test
    void testGenerateDeleteEventForModifyJoinKey_WhenPkChanged_ShouldGenerateDelete() throws Exception {
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        TapUpdateRecordEvent update = new TapUpdateRecordEvent();
        TapEventUtil.setBefore(update, new HashMap<>(Map.of("rid", 1)));
        TapEventUtil.setAfter(update, new HashMap<>(Map.of("rid", 2)));
        update.setTableId("t");

        var m = HazelcastJoinProcessor.class.getDeclaredMethod("generateDeleteEventForModifyJoinKey", TapRecordEvent.class);
        m.setAccessible(true);
        TapRecordEvent del = (TapRecordEvent) m.invoke(hazelcastJoinProcessor, update);
        Assertions.assertNotNull(del);
        Assertions.assertTrue(del instanceof TapDeleteRecordEvent);
        Assertions.assertEquals("t", del.getTableId());
    }

    @Test
    void testVatidate_IndexListNull_ShouldThrow() throws Exception {
        LinkedHashMap<String, io.tapdata.entity.schema.TapField> nameFieldMap = new LinkedHashMap<>();
        io.tapdata.entity.schema.TapField pkField = mock(io.tapdata.entity.schema.TapField.class);
        when(pkField.getPrimaryKeyPos()).thenReturn(1);
        when(pkField.getName()).thenReturn("id");
        nameFieldMap.put("id", pkField);
        when(tapTable.getNameFieldMap()).thenReturn(nameFieldMap);
        when(tapTable.getIndexList()).thenReturn(null);
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("vatidate", Node.class);
        m.setAccessible(true);
        Assertions.assertThrows(RuntimeException.class, () -> {
            try {
                m.invoke(hazelcastJoinProcessor, processorBaseContext.getNode());
            } catch (Exception e) {
                throw (RuntimeException) e.getCause();
            }
        });
    }

    @Test
    void testVatidate_UniqueIndexNull_ShouldThrow() throws Exception {
        LinkedHashMap<String, io.tapdata.entity.schema.TapField> nameFieldMap = new LinkedHashMap<>();
        io.tapdata.entity.schema.TapField pkField = mock(io.tapdata.entity.schema.TapField.class);
        when(pkField.getPrimaryKeyPos()).thenReturn(1);
        when(pkField.getName()).thenReturn("id");
        nameFieldMap.put("id", pkField);
        when(tapTable.getNameFieldMap()).thenReturn(nameFieldMap);
        io.tapdata.entity.schema.TapIndex nonUnique = mock(io.tapdata.entity.schema.TapIndex.class);
        when(nonUnique.isUnique()).thenReturn(false);
        when(tapTable.getIndexList()).thenReturn(Arrays.asList(nonUnique));
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("vatidate", Node.class);
        m.setAccessible(true);
        Assertions.assertThrows(RuntimeException.class, () -> {
            try {
                m.invoke(hazelcastJoinProcessor, processorBaseContext.getNode());
            } catch (Exception e) {
                throw (RuntimeException) e.getCause();
            }
        });
    }

    @Test
    void testUpdateNodeConfig_WhenInitNodeThrows_ShouldWrap() throws Exception {
        HazelcastJoinProcessor p = spy(new HazelcastJoinProcessor(processorBaseContext));
        org.mockito.Mockito.doThrow(new RuntimeException("x")).when(p).initNode();
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("updateNodeConfig", TapdataEvent.class);
        m.setAccessible(true);
        Assertions.assertThrows(RuntimeException.class, () -> {
            try {
                m.invoke(p, new TapdataEvent());
            } catch (Exception e) {
                throw (RuntimeException) e.getCause();
            }
        });
    }

    @Test
    void testVatidate_PksEmpty_ShouldThrow() throws Exception {
        when(tapTable.getNameFieldMap()).thenReturn(new LinkedHashMap<>());
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("vatidate", Node.class);
        m.setAccessible(true);
        Assertions.assertThrows(RuntimeException.class, () -> {
            try {
                m.invoke(hazelcastJoinProcessor, processorBaseContext.getNode());
            } catch (Exception e) {
                throw (RuntimeException) e.getCause();
            }
        });
    }

    @Test
    void testDoClose_DestroyFailure_ShouldNotThrow() throws Exception {
        BytesIMap<Map<String, Map<String, Object>>> left = mock(BytesIMap.class);
        BytesIMap<Map<String, Map<String, Object>>> right = mock(BytesIMap.class);
        ObsLogger obsLogger = mock(ObsLogger.class);
        when(left.getName()).thenReturn("l");
        when(right.getName()).thenReturn("r");
        doNothing().when(left).destroy();
        org.mockito.Mockito.doThrow(new RuntimeException("x")).when(right).destroy();
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", left);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", right);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "obsLogger", obsLogger);
        hazelcastJoinProcessor.doClose();
        verify(left, times(1)).destroy();
        verify(right, times(1)).destroy();
    }

    @Test
    void testTryProcessSingle_ShouldEmit() throws Exception {
        BytesIMap<Map<String, Map<String, Object>>> leftIMap = mockBytesIMapWithStore(new HashMap<>());
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(new HashMap<>());
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", leftIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", Arrays.asList("id"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftNodeId", "leftNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightNodeId", "rightNodeId");

        TapInsertRecordEvent leftInsert = new TapInsertRecordEvent();
        TapEventUtil.setAfter(leftInsert, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L")));
        TapdataEvent leftEvent = new TapdataEvent();
        leftEvent.setTapEvent(leftInsert);
        leftEvent.setNodeIds(Arrays.asList("leftNodeId"));

        List<TapdataEvent> emitted = new ArrayList<>();
        BiConsumer<TapdataEvent, HazelcastProcessorBaseNode.ProcessResult> consumer = (e, r) -> emitted.add(e);

        var m = HazelcastJoinProcessor.class.getDeclaredMethod("tryProcess", TapdataEvent.class, BiConsumer.class);
        m.setAccessible(true);
        m.invoke(hazelcastJoinProcessor, leftEvent, consumer);
        Assertions.assertFalse(emitted.isEmpty());
    }

    @Test
    void testDealWithLeftJoinRightRowOpType_DeleteBranch() throws Exception {
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(new HashMap<>());
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        Map<String, Map<String, Map<String, Object>>> rightCache = new HashMap<>();
        rightCache.put("[\"J1\"]", new HashMap<>(Map.of("[10]", new HashMap<>(Map.of("rid", 10)))));
        var m = HazelcastJoinProcessor.class.getDeclaredMethod(
                "dealWithLeftJoinRightRowOpType",
                Map.class, Map.class, String.class, String.class, Map.class
        );
        m.setAccessible(true);
        Object r1 = m.invoke(hazelcastJoinProcessor, new HashMap<>(), new HashMap<>(), "d", "[\"J1\"]", rightCache);
        Assertions.assertEquals("d", r1);
        rightCache.clear();
        Object r2 = m.invoke(hazelcastJoinProcessor, new HashMap<>(), new HashMap<>(), "d", "[\"J1\"]", rightCache);
        Assertions.assertEquals("u", r2);
    }

    @Test
    void testDealWithLeftJoinRightRowOpType_WhenPkUnchanged_ShouldReturnUpdate() throws Exception {
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        var m = HazelcastJoinProcessor.class.getDeclaredMethod(
                "dealWithLeftJoinRightRowOpType",
                Map.class, Map.class, String.class, String.class, Map.class
        );
        m.setAccessible(true);
        Object r = m.invoke(
                hazelcastJoinProcessor,
                new HashMap<>(Map.of("rid", 1)),
                new HashMap<>(Map.of("rid", 1)),
                "u",
                "[J1]",
                new HashMap<>()
        );
        Assertions.assertEquals("u", r);
    }

    @Test
    void testJoin_Branches() throws Exception {
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("join", Map.class, HazelcastJoinProcessor.JoinOperation.class, Map.class);
        m.setAccessible(true);

        ReflectionTestUtils.setField(hazelcastJoinProcessor, "embeddedPath", "embed");
        m.invoke(hazelcastJoinProcessor, new HashMap<>(Map.of("right", "R")), HazelcastJoinProcessor.JoinOperation.Upsert, null);

        Map<String, Object> leftRow0 = new HashMap<>();
        m.invoke(hazelcastJoinProcessor, new HashMap<>(Map.of("right", "R0")), HazelcastJoinProcessor.JoinOperation.Upsert, leftRow0);
        Assertions.assertTrue(leftRow0.get("embed") instanceof Map);

        Map<String, Object> leftRow = new HashMap<>(Map.of("embed", "notMap"));
        m.invoke(hazelcastJoinProcessor, new HashMap<>(Map.of("right", "R")), HazelcastJoinProcessor.JoinOperation.Upsert, leftRow);
        Assertions.assertTrue(leftRow.get("embed") instanceof Map);

        Map<String, Object> leftRow2 = new HashMap<>();
        leftRow2.put("embed", new HashMap<>(Map.of("right", "R")));
        m.invoke(hazelcastJoinProcessor, new HashMap<>(Map.of("right", "R2")), HazelcastJoinProcessor.JoinOperation.Delete, leftRow2);
        Object embed2 = leftRow2.get("embed");
        Assertions.assertTrue(embed2 instanceof Map);
        Assertions.assertNull(((Map<?, ?>) embed2).get("right"));

        ReflectionTestUtils.setField(hazelcastJoinProcessor, "embeddedPath", null);
        Map<String, Object> leftRow3 = new HashMap<>(Map.of("right", "R", "keep", "K"));
        m.invoke(hazelcastJoinProcessor, new HashMap<>(Map.of("right", "R2")), HazelcastJoinProcessor.JoinOperation.Delete, leftRow3);
        Assertions.assertFalse(leftRow3.containsKey("right"));
        Assertions.assertEquals("K", leftRow3.get("keep"));
    }

    @Test
    void testTryProcessBatch_LeftInsert_WhenRightCacheHasData_ShouldJoinRightRows() throws Exception {
        Map<String, Map<String, Map<String, Object>>> rightStore = new HashMap<>();
        rightStore.put("[J1]", new HashMap<>(Map.of("[10]", new HashMap<>(Map.of("k", "J1", "rid", 10, "right", "R")))));
        BytesIMap<Map<String, Map<String, Object>>> leftIMap = mockBytesIMapWithStore(new HashMap<>());
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(rightStore);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", leftIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", Arrays.asList("id"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftNodeId", "leftNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightNodeId", "rightNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "embeddedPath", null);

        TapInsertRecordEvent leftInsert = new TapInsertRecordEvent();
        TapEventUtil.setAfter(leftInsert, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L")));
        TapdataEvent leftEvent = new TapdataEvent();
        leftEvent.setTapEvent(leftInsert);
        leftEvent.setNodeIds(Arrays.asList("leftNodeId"));

        List<HazelcastProcessorBaseNode.BatchEventWrapper> batch = Arrays.asList(
                new HazelcastProcessorBaseNode.BatchEventWrapper(leftEvent)
        );
        List<List<HazelcastProcessorBaseNode.BatchProcessResult>> accepted = new ArrayList<>();
        invokeTryProcessBatch(hazelcastJoinProcessor, batch, accepted::add);
        TapRecordEvent out = (TapRecordEvent) accepted.get(0).get(0).getBatchEventWrapper().getTapdataEvent().getTapEvent();
        Assertions.assertEquals("R", TapEventUtil.getAfter(out).get("right"));
    }

    @Test
    void testTryProcessBatch_LeftDelete_ShouldExecuteDeleteBranch() throws Exception {
        Map<String, Map<String, Map<String, Object>>> leftStore = new HashMap<>();
        leftStore.put("[J1]", new HashMap<>(Map.of("[1]", new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L")))));
        BytesIMap<Map<String, Map<String, Object>>> leftIMap = mockBytesIMapWithStore(leftStore);
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(new HashMap<>());
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", leftIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", Arrays.asList("id"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftNodeId", "leftNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightNodeId", "rightNodeId");

        TapDeleteRecordEvent leftDelete = new TapDeleteRecordEvent();
        TapEventUtil.setBefore(leftDelete, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L")));
        TapdataEvent leftEvent = new TapdataEvent();
        leftEvent.setTapEvent(leftDelete);
        leftEvent.setNodeIds(Arrays.asList("leftNodeId"));

        List<HazelcastProcessorBaseNode.BatchEventWrapper> batch = Arrays.asList(
                new HazelcastProcessorBaseNode.BatchEventWrapper(leftEvent)
        );
        List<List<HazelcastProcessorBaseNode.BatchProcessResult>> accepted = new ArrayList<>();
        invokeTryProcessBatch(hazelcastJoinProcessor, batch, accepted::add);
        Assertions.assertFalse(accepted.isEmpty());
    }

    @Test
    void testLeftJoinRightProcess_WhenCacheContainsNullKey_ShouldUseAsBefore() throws Exception {
        Map<String, Map<String, Map<String, Object>>> store = new HashMap<>();
        Map<String, Map<String, Object>> byPk = new HashMap<>();
        byPk.put(null, new HashMap<>(Map.of("k", "J1", "rid", 0, "right", "R0")));
        store.put("[J1]", byPk);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", mockBytesIMapWithStore(new HashMap<>()));
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(store);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));

        var m = HazelcastJoinProcessor.class.getDeclaredMethod(
                "leftJoinRightProcess",
                Map.class, Map.class, String.class, Map.class, Map.class
        );
        m.setAccessible(true);
        m.invoke(
                hazelcastJoinProcessor,
                new HashMap<>(),
                new HashMap<>(Map.of("k", "J1", "rid", 10, "right", "R10")),
                "u",
                new HashMap<>(),
                new HashMap<>()
        );
    }

    @Test
    void testLeftJoinFillRightRow_WhenUnmodifiable_ShouldThrow() throws Exception {
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("leftJoinFillRightRow", Map.class);
        m.setAccessible(true);
        Map<String, Object> unmodifiable = Collections.unmodifiableMap(new HashMap<>(Map.of("left", "L")));
        Assertions.assertThrows(RuntimeException.class, () -> {
            try {
                m.invoke(hazelcastJoinProcessor, unmodifiable);
            } catch (Exception e) {
                throw (RuntimeException) e.getCause();
            }
        });
    }

    @Test
    void testHandleTransformToTapValueResult_ShouldClear() throws Exception {
        TapdataEvent e = new TapdataEvent();
        e.setTransformToTapValueResult(TransformToTapValueResult.create());
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("handleTransformToTapValueResult", TapdataEvent.class);
        m.setAccessible(true);
        m.invoke(hazelcastJoinProcessor, e);
        Assertions.assertNull(e.getTransformToTapValueResult());
    }

    @Test
    void testTryProcessBatch_LeftInsertThenRightInsert_ShouldJoinAndFillNull() throws Exception {
        BytesIMap<Map<String, Map<String, Object>>> leftIMap = mockBytesIMapWithStore(new HashMap<>());
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(new HashMap<>());
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", leftIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", Arrays.asList("id"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftNodeId", "leftNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightNodeId", "rightNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "embeddedPath", null);

        TapInsertRecordEvent leftInsert = new TapInsertRecordEvent();
        leftInsert.setTableId("t");
        TapEventUtil.setAfter(leftInsert, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L")));
        TapdataEvent leftEvent = new TapdataEvent();
        leftEvent.setTapEvent(leftInsert);
        leftEvent.setNodeIds(Arrays.asList("leftNodeId"));

        TapInsertRecordEvent rightInsert = new TapInsertRecordEvent();
        rightInsert.setTableId("t");
        TapEventUtil.setAfter(rightInsert, new HashMap<>(Map.of("k", "J1", "rid", 10, "right", "R")));
        TapdataEvent rightEvent = new TapdataEvent();
        rightEvent.setTapEvent(rightInsert);
        rightEvent.setNodeIds(Arrays.asList("rightNodeId"));

        List<HazelcastProcessorBaseNode.BatchEventWrapper> batch = Arrays.asList(
                new HazelcastProcessorBaseNode.BatchEventWrapper(leftEvent),
                new HazelcastProcessorBaseNode.BatchEventWrapper(rightEvent)
        );

        List<List<HazelcastProcessorBaseNode.BatchProcessResult>> accepted = new ArrayList<>();
        invokeTryProcessBatch(hazelcastJoinProcessor, batch, accepted::add);
        Assertions.assertEquals(1, accepted.size());
        List<HazelcastProcessorBaseNode.BatchProcessResult> results = accepted.get(0);
        Assertions.assertEquals(2, results.size());

        TapRecordEvent out1 = (TapRecordEvent) results.get(0).getBatchEventWrapper().getTapdataEvent().getTapEvent();
        TapRecordEvent out2 = (TapRecordEvent) results.get(1).getBatchEventWrapper().getTapdataEvent().getTapEvent();
        Assertions.assertEquals("t", out1.getTableId());
        Assertions.assertEquals("t", out2.getTableId());
        Assertions.assertEquals("L", TapEventUtil.getAfter(out1).get("left"));
        Assertions.assertNull(TapEventUtil.getAfter(out1).get("right"));
        Assertions.assertEquals("R", TapEventUtil.getAfter(out2).get("right"));
        Assertions.assertEquals("L", TapEventUtil.getAfter(out2).get("left"));
    }

    @Test
    void testTryProcessBatch_EmbeddedMode_ShouldPutRightRowUnderEmbeddedPath() throws Exception {
        BytesIMap<Map<String, Map<String, Object>>> leftIMap = mockBytesIMapWithStore(new HashMap<>());
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(new HashMap<>());
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", leftIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", Arrays.asList("id"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftNodeId", "leftNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightNodeId", "rightNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "embeddedPath", "embed");

        TapInsertRecordEvent leftInsert = new TapInsertRecordEvent();
        TapEventUtil.setAfter(leftInsert, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L", "embed", new HashMap<>(Map.of("old", "x")))));
        TapdataEvent leftEvent = new TapdataEvent();
        leftEvent.setTapEvent(leftInsert);
        leftEvent.setNodeIds(Arrays.asList("leftNodeId"));

        TapInsertRecordEvent rightInsert = new TapInsertRecordEvent();
        TapEventUtil.setAfter(rightInsert, new HashMap<>(Map.of("k", "J1", "rid", 10, "right", "R")));
        TapdataEvent rightEvent = new TapdataEvent();
        rightEvent.setTapEvent(rightInsert);
        rightEvent.setNodeIds(Arrays.asList("rightNodeId"));

        List<HazelcastProcessorBaseNode.BatchEventWrapper> batch = Arrays.asList(
                new HazelcastProcessorBaseNode.BatchEventWrapper(leftEvent),
                new HazelcastProcessorBaseNode.BatchEventWrapper(rightEvent)
        );

        List<List<HazelcastProcessorBaseNode.BatchProcessResult>> accepted = new ArrayList<>();
        invokeTryProcessBatch(hazelcastJoinProcessor, batch, accepted::add);
        TapRecordEvent out = (TapRecordEvent) accepted.get(0).get(1).getBatchEventWrapper().getTapdataEvent().getTapEvent();
        Map<String, Object> after = TapEventUtil.getAfter(out);
        Assertions.assertTrue(after.containsKey("embed"));
        Assertions.assertFalse(after.containsKey("right"));
        Object embed = after.get("embed");
        Assertions.assertTrue(embed instanceof Map);
        Assertions.assertEquals("R", ((Map<?, ?>) embed).get("right"));
        Assertions.assertEquals("x", ((Map<?, ?>) embed).get("old"));
    }

    @Test
    void testTryProcessBatch_RightDelete_ShouldReturnDeleteOrUpdateDependingOnRemaining() throws Exception {
        Map<String, Map<String, Map<String, Object>>> leftStore = new HashMap<>();
        Map<String, Map<String, Map<String, Object>>> rightStore = new HashMap<>();
        BytesIMap<Map<String, Map<String, Object>>> leftIMap = mockBytesIMapWithStore(leftStore);
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(rightStore);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", leftIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", Arrays.asList("id"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftNodeId", "leftNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightNodeId", "rightNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "embeddedPath", null);

        TapInsertRecordEvent leftInsert = new TapInsertRecordEvent();
        TapEventUtil.setAfter(leftInsert, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L")));
        TapdataEvent leftEvent = new TapdataEvent();
        leftEvent.setTapEvent(leftInsert);
        leftEvent.setNodeIds(Arrays.asList("leftNodeId"));

        TapInsertRecordEvent rightInsert1 = new TapInsertRecordEvent();
        TapEventUtil.setAfter(rightInsert1, new HashMap<>(Map.of("k", "J1", "rid", 10, "right", "R10")));
        TapdataEvent rightEvent1 = new TapdataEvent();
        rightEvent1.setTapEvent(rightInsert1);
        rightEvent1.setNodeIds(Arrays.asList("rightNodeId"));

        TapInsertRecordEvent rightInsert2 = new TapInsertRecordEvent();
        TapEventUtil.setAfter(rightInsert2, new HashMap<>(Map.of("k", "J1", "rid", 11, "right", "R11")));
        TapdataEvent rightEvent2 = new TapdataEvent();
        rightEvent2.setTapEvent(rightInsert2);
        rightEvent2.setNodeIds(Arrays.asList("rightNodeId"));

        TapDeleteRecordEvent rightDelete = new TapDeleteRecordEvent();
        TapEventUtil.setBefore(rightDelete, new HashMap<>(Map.of("k", "J1", "rid", 10, "right", "R10")));
        TapdataEvent rightDeleteEvent = new TapdataEvent();
        rightDeleteEvent.setTapEvent(rightDelete);
        rightDeleteEvent.setNodeIds(Arrays.asList("rightNodeId"));

        List<HazelcastProcessorBaseNode.BatchEventWrapper> batch = Arrays.asList(
                new HazelcastProcessorBaseNode.BatchEventWrapper(leftEvent),
                new HazelcastProcessorBaseNode.BatchEventWrapper(rightEvent1),
                new HazelcastProcessorBaseNode.BatchEventWrapper(rightEvent2),
                new HazelcastProcessorBaseNode.BatchEventWrapper(rightDeleteEvent)
        );

        List<List<HazelcastProcessorBaseNode.BatchProcessResult>> accepted = new ArrayList<>();
        invokeTryProcessBatch(hazelcastJoinProcessor, batch, accepted::add);

        boolean hasDeleteOp = accepted.stream()
                .flatMap(List::stream)
                .map(r -> TapEventUtil.getOp(r.getBatchEventWrapper().getTapdataEvent().getTapEvent()))
                .anyMatch(op -> "d".equals(op));
        boolean hasUpdateOp = accepted.stream()
                .flatMap(List::stream)
                .map(r -> TapEventUtil.getOp(r.getBatchEventWrapper().getTapdataEvent().getTapEvent()))
                .anyMatch(op -> "u".equals(op));

        Assertions.assertTrue(hasDeleteOp || hasUpdateOp);
    }

    @Test
    void testAppendIMap_ShouldFlushOnDuplicateKey() throws Exception {
        Map<String, Map<String, Map<String, Object>>> store = new HashMap<>();
        BytesIMap<Map<String, Map<String, Object>>> leftIMap = mockBytesIMapWithStore(store);
        BytesIMap<Map<String, Map<String, Object>>> rightIMap = mockBytesIMapWithStore(new HashMap<>());
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinCache", leftIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinCache", rightIMap);
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightJoinKeyFields", Arrays.asList("k"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftPrimaryKeys", Arrays.asList("id"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightPrimaryKeys", Arrays.asList("rid"));
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "leftNodeId", "leftNodeId");
        ReflectionTestUtils.setField(hazelcastJoinProcessor, "rightNodeId", "rightNodeId");

        TapInsertRecordEvent leftInsert1 = new TapInsertRecordEvent();
        TapEventUtil.setAfter(leftInsert1, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L1")));
        TapdataEvent leftEvent1 = new TapdataEvent();
        leftEvent1.setTapEvent(leftInsert1);
        leftEvent1.setNodeIds(Arrays.asList("leftNodeId"));

        TapUpdateRecordEvent leftUpdate = new TapUpdateRecordEvent();
        TapEventUtil.setAfter(leftUpdate, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L2")));
        TapEventUtil.setBefore(leftUpdate, new HashMap<>(Map.of("k", "J1", "id", 1, "left", "L1")));
        TapdataEvent leftEvent2 = new TapdataEvent();
        leftEvent2.setTapEvent(leftUpdate);
        leftEvent2.setNodeIds(Arrays.asList("leftNodeId"));

        List<HazelcastProcessorBaseNode.BatchEventWrapper> batch = Arrays.asList(
                new HazelcastProcessorBaseNode.BatchEventWrapper(leftEvent1),
                new HazelcastProcessorBaseNode.BatchEventWrapper(leftEvent2)
        );

        AtomicInteger insertManyCount = new AtomicInteger(0);
        when(leftIMap.insertMany(org.mockito.ArgumentMatchers.anyMap())).thenAnswer(inv -> {
            insertManyCount.incrementAndGet();
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Map<String, Object>>> m = (Map<String, Map<String, Map<String, Object>>>) inv.getArgument(0);
            store.putAll(m);
            return (long) m.size();
        });

        List<List<HazelcastProcessorBaseNode.BatchProcessResult>> accepted = new ArrayList<>();
        invokeTryProcessBatch(hazelcastJoinProcessor, batch, accepted::add);
        Assertions.assertTrue(insertManyCount.get() >= 1);
    }

    private static void invokeTryProcessBatch(HazelcastJoinProcessor processor,
                                              List<HazelcastProcessorBaseNode.BatchEventWrapper> batch,
                                              Consumer<List<HazelcastProcessorBaseNode.BatchProcessResult>> consumer) throws Exception {
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("tryProcess", List.class, Consumer.class);
        m.setAccessible(true);
        m.invoke(processor, batch, consumer);
    }

    private static BytesIMap<Map<String, Map<String, Object>>> mockBytesIMapWithStore(Map<String, Map<String, Map<String, Object>>> store) throws Exception {
        @SuppressWarnings("unchecked")
        BytesIMap<Map<String, Map<String, Object>>> imap = (BytesIMap<Map<String, Map<String, Object>>>) mock(BytesIMap.class);
        when(imap.find(org.mockito.ArgumentMatchers.anyString())).thenAnswer(inv -> store.get(inv.getArgument(0)));
        when(imap.exists(org.mockito.ArgumentMatchers.anyString())).thenAnswer(inv -> store.containsKey(inv.getArgument(0)));
        when(imap.insert(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any())).thenAnswer(inv -> {
            store.put(inv.getArgument(0), inv.getArgument(1));
            return 1;
        });
        when(imap.update(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any())).thenAnswer(inv -> {
            store.put(inv.getArgument(0), inv.getArgument(1));
            return 1;
        });
        when(imap.delete(org.mockito.ArgumentMatchers.anyString())).thenAnswer(inv -> {
            store.remove(inv.getArgument(0));
            return 1;
        });
        when(imap.insertMany(org.mockito.ArgumentMatchers.anyMap())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Map<String, Object>>> m = (Map<String, Map<String, Map<String, Object>>>) inv.getArgument(0);
            store.putAll(m);
            return (long) m.size();
        });
        return imap;
    }

    @Test
    void testAppendIMap_WhenKeyExists_ShouldFlushAndClear() throws Exception {
        BytesIMap<Map<String, Map<String, Object>>> joinCache = mock(BytesIMap.class);
        Map<String, Map<String, Map<String, Object>>> cache = new HashMap<>();
        cache.put("k1", new HashMap<>(Map.of("[1]", new HashMap<>(Map.of("id", 1)))));

        ReflectionTestUtils.setField(hazelcastJoinProcessor, "maxBatchSize", 1000);
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("appendIMap", String.class, Map.class, Map.class, BytesIMap.class);
        m.setAccessible(true);

        Map<String, Map<String, Object>> newValue = new HashMap<>(Map.of("[2]", new HashMap<>(Map.of("id", 2))));
        m.invoke(hazelcastJoinProcessor, "k1", newValue, cache, joinCache);

        verify(joinCache, times(1)).insertMany(org.mockito.ArgumentMatchers.anyMap());
        Assertions.assertTrue(cache.containsKey("k1"));
        Assertions.assertEquals(newValue, cache.get("k1"));
    }

    @Test
    void testAppendIMap_WhenOverBatch_ShouldFlushAndClear() throws Exception {
        BytesIMap<Map<String, Map<String, Object>>> joinCache = mock(BytesIMap.class);
        Map<String, Map<String, Map<String, Object>>> cache = new HashMap<>();

        ReflectionTestUtils.setField(hazelcastJoinProcessor, "maxBatchSize", 0);
        var m = HazelcastJoinProcessor.class.getDeclaredMethod("appendIMap", String.class, Map.class, Map.class, BytesIMap.class);
        m.setAccessible(true);

        m.invoke(
                hazelcastJoinProcessor,
                "k2",
                new HashMap<>(Map.of("[3]", new HashMap<>(Map.of("id", 3)))),
                cache,
                joinCache
        );

        verify(joinCache, times(1)).insertMany(org.mockito.ArgumentMatchers.anyMap());
        Assertions.assertTrue(cache.isEmpty());
    }

    @Test
    void testFindAndExists_ShouldPreferLocalCache() throws Exception {
        BytesIMap<Map<String, Map<String, Object>>> joinCache = mock(BytesIMap.class);
        Map<String, Map<String, Map<String, Object>>> cache = new HashMap<>();
        Map<String, Map<String, Object>> v = new HashMap<>(Map.of("[1]", new HashMap<>(Map.of("id", 1))));
        cache.put("k", v);

        var findM = HazelcastJoinProcessor.class.getDeclaredMethod("find", String.class, Map.class, BytesIMap.class);
        findM.setAccessible(true);
        Object found = findM.invoke(hazelcastJoinProcessor, "k", cache, joinCache);
        Assertions.assertEquals(v, found);

        var existsM = HazelcastJoinProcessor.class.getDeclaredMethod("exists", String.class, Map.class, BytesIMap.class);
        existsM.setAccessible(true);
        Object exists = existsM.invoke(hazelcastJoinProcessor, "k", cache, joinCache);
        Assertions.assertEquals(Boolean.TRUE, exists);

        verify(joinCache, times(0)).find(org.mockito.ArgumentMatchers.anyString());
        verify(joinCache, times(0)).exists(org.mockito.ArgumentMatchers.anyString());
    }

}
