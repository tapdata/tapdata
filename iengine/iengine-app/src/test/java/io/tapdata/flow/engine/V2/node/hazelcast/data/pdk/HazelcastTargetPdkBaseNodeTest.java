package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.google.common.collect.Lists;
import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-01-22 18:46
 **/
@DisplayName("HazelcastTargetPdkBaseNode Class Test")
public class HazelcastTargetPdkBaseNodeTest extends BaseHazelcastNodeTest {
    private HazelcastTargetPdkBaseNode hazelcastTargetPdkBaseNode;

    @BeforeEach
    void setUp() {
        hazelcastTargetPdkBaseNode = mock(HazelcastTargetPdkBaseNode.class);
        when(hazelcastTargetPdkBaseNode.getDataProcessorContext()).thenReturn(dataProcessorContext);
    }

    @Nested
    @DisplayName("fromTapValueMergeInfo method test")
    class fromTapValueMergeInfoTest {
        @BeforeEach
        void setUp() {
            doCallRealMethod().when(hazelcastTargetPdkBaseNode).fromTapValueMergeInfo(any());
        }

        @Test
        @DisplayName("main process test")
        void testMainProcess() {
            MergeInfo mergeInfo = new MergeInfo();
            TapdataEvent tapdataEvent = new TapdataEvent();
            TapEvent tapEvent = mock(TapEvent.class);
            when(tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY)).thenReturn(mergeInfo);
            tapdataEvent.setTapEvent(tapEvent);

            hazelcastTargetPdkBaseNode.fromTapValueMergeInfo(tapdataEvent);
            verify(hazelcastTargetPdkBaseNode, times(1)).recursiveMergeInfoTransformFromTapValue(any());
        }

        @Test
        @DisplayName("when tapEvent not have mergeInfo")
        void notHaveMergeInfo() {
            TapdataEvent tapdataEvent = new TapdataEvent();
            TapEvent tapEvent = mock(TapEvent.class);
            when(tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY)).thenReturn(null);
            tapdataEvent.setTapEvent(tapEvent);

            hazelcastTargetPdkBaseNode.fromTapValueMergeInfo(tapdataEvent);
            verify(hazelcastTargetPdkBaseNode, times(0)).recursiveMergeInfoTransformFromTapValue(any());
        }
    }

    @Nested
    @DisplayName("recursiveMergeInfoTransformFromTapValue method test")
    class recursiveMergeInfoTransformFromTapValueTest {
        @BeforeEach
        void setUp() {
            doCallRealMethod().when(hazelcastTargetPdkBaseNode).recursiveMergeInfoTransformFromTapValue(any());
        }

        @Test
        @DisplayName("main process test")
        void testMainProcess() {
            List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
            MergeLookupResult mergeLookupResult = new MergeLookupResult();
            Map<String, Object> data = new HashMap<>();
            data.put("_id", 1);
            mergeLookupResult.setData(data);
            mergeLookupResults.add(mergeLookupResult);
            TapTable tapTable = new TapTable();
            mergeLookupResult.setTapTable(tapTable);
            List<MergeLookupResult> childMergeLookupResults = new ArrayList<>();
            MergeLookupResult childMergeLookupResult = new MergeLookupResult();
            Map<String, Object> childData = new HashMap<>(data);
            childMergeLookupResult.setData(childData);
            childMergeLookupResults.add(childMergeLookupResult);
            childMergeLookupResult.setTapTable(tapTable);
            TapCodecsFilterManager tapCodecsFilterManager = mock(TapCodecsFilterManager.class);
            ReflectionTestUtils.setField(hazelcastTargetPdkBaseNode, "codecsFilterManager", tapCodecsFilterManager);
            mergeLookupResult.setMergeLookupResults(childMergeLookupResults);

            hazelcastTargetPdkBaseNode.recursiveMergeInfoTransformFromTapValue(mergeLookupResults);
            verify(hazelcastTargetPdkBaseNode, times(2)).fromTapValue(data, tapCodecsFilterManager, tapTable);
        }

        @Test
        @DisplayName("when mergeLookupResults is empty")
        void mergeLookupResultsIsEmpty() {
            List<MergeLookupResult> mergeLookupResults = new ArrayList<>();

            hazelcastTargetPdkBaseNode.recursiveMergeInfoTransformFromTapValue(mergeLookupResults);
            verify(hazelcastTargetPdkBaseNode, times(0)).fromTapValue(any(Map.class), any(TapCodecsFilterManager.class), any(TapTable.class));
        }
    }

    @DisplayName("test ignorePksAndIndices normal")
    @Test
    void ignorePksAndIndicesTest1() {
        TapTable tapTable = new TapTable();
        TapField field = getField("_id");
        TapField index = getField("index");
        tapTable.add(field);
        tapTable.add(index);
        List<String> list = Arrays.asList("_id", "index");

        HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable, list);
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        TapField idField = nameFieldMap.get("_id");
        TapField indexField = nameFieldMap.get("index");
        assertEquals(2, nameFieldMap.size());
        assertEquals(0, idField.getPrimaryKeyPos());
        assertEquals(0, indexField.getPrimaryKeyPos());
        assertEquals(false, indexField.getPrimaryKey());
        assertEquals(false, idField.getPrimaryKey());
    }

    @DisplayName("test ignorePksAndIndices logic primary key is null")
    @Test
    void ignorePksAndIndicesTest2() {
        TapTable tapTable = new TapTable();
        TapField field = getField("_id");
        TapField index = getField("index");
        tapTable.add(field);
        tapTable.add(index);
        HazelcastTargetPdkBaseNode.ignorePksAndIndices(tapTable, null);
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        TapField idField = nameFieldMap.get("_id");
        TapField indexField = nameFieldMap.get("index");
        assertEquals(2, nameFieldMap.size());
        assertEquals(0, idField.getPrimaryKeyPos());
        assertEquals(0, indexField.getPrimaryKeyPos());
        assertEquals(false, indexField.getPrimaryKey());
        assertEquals(false, idField.getPrimaryKey());
    }

    public TapField getField(String name) {
        TapField field = new TapField();
        field.setName(name);
        return field;
    }

    class usePkAsUpdateConditionsTest {
        @BeforeEach
        void setUp() {
            hazelcastTargetPdkBaseNode = spy(HazelcastTargetPdkBaseNode.class);
        }

        @DisplayName("test pk as update conditions same1")
        @Test
        void usePkAsUpdateConditionsTest1() {
            assertTrue(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(Collections.emptyList(), null));
        }

        @DisplayName("test pk as update conditions same2")
        @Test
        void usePkAsUpdateConditionsTest2() {
            assertTrue(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(Lists.newArrayList("A", "B"), Lists.newArrayList("A", "B")));
        }

        @DisplayName("test pk as update conditions different1")
        @Test
        void usePkAsUpdateConditionsTest3() {
            assertFalse(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(Lists.newArrayList("A", "B"), Lists.newArrayList("C")));
        }

        @DisplayName("test pk as update conditions different2")
        @Test
        void usePkAsUpdateConditionsTest4() {
            assertFalse(hazelcastTargetPdkBaseNode.usePkAsUpdateConditions(Lists.newArrayList("A", "B", "C"), Lists.newArrayList("A", "B")));
        }
    }

    @Nested
    class createTableTest{
        @Test
        void test(){
            hazelcastTargetPdkBaseNode.createTable(mock(TapTable.class),new AtomicBoolean(false));
        }
    }

}
