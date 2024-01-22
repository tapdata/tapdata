package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}
