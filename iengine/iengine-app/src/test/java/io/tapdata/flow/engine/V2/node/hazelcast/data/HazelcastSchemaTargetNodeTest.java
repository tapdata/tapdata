package io.tapdata.flow.engine.V2.node.hazelcast.data;

import base.hazelcast.BaseHazelcastNodeTest;
import com.hazelcast.jet.core.Inbox;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.error.VirtualTargetExCode_14;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 21:38
 **/
@DisplayName("HazelcastSchemaTargetNode Class Test")
class HazelcastSchemaTargetNodeTest extends BaseHazelcastNodeTest {

	HazelcastSchemaTargetNode hazelcastSchemaTargetNode;

	@BeforeEach
	void beforeEach() {
		super.allSetup();
		hazelcastSchemaTargetNode = new HazelcastSchemaTargetNode(dataProcessorContext);
	}

	@Nested
	@DisplayName("DoInit method test")
	class DoInitTest {
		@Test
		@DisplayName("Test init tapTableMap")
		void testDoInitInitTapTableMap() {
			try (MockedStatic<TapTableUtil> tapTableUtilMockedStatic = mockStatic(TapTableUtil.class)) {
				TapTableMap tapTableMap = mock(TapTableMap.class);
				tapTableUtilMockedStatic.when(() -> TapTableUtil.getTapTableMap(anyString(), any(Node.class), any()))
						.thenReturn(tapTableMap);
				Node<?> mockNode = mock(Node.class);
				Node<?> spyNode = spy(dataProcessorContext.getNode());
				List preNodes = new ArrayList<>();
				preNodes.add(mockNode);
				doReturn(preNodes).when(spyNode).predecessors();
				when(dataProcessorContext.getNode()).thenReturn((Node) spyNode);
				assertDoesNotThrow(() -> hazelcastSchemaTargetNode.doInit(jetContext));
				verify(spyNode, new Times(1)).predecessors();
				Object actualObj = ReflectionTestUtils.getField(hazelcastSchemaTargetNode, "oldTapTableMap");
				assertEquals(tapTableMap, actualObj);
			}
		}

		@Test
		@DisplayName("When predecessors more then one")
		void testDoInitPreNodeMoreThanOne() {
			Node<?> mockNode = mock(Node.class);
			Node<?> spyNode = spy(dataProcessorContext.getNode());
			List preNodes = new ArrayList<>();
			preNodes.add(mockNode);
			preNodes.add(mockNode);
			doReturn(preNodes).when(spyNode).predecessors();
			when(dataProcessorContext.getNode()).thenReturn((Node) spyNode);
			IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> hazelcastSchemaTargetNode.doInit(jetContext));
			assertEquals("HazelcastSchemaTargetNode only allows one predecessor node", illegalArgumentException.getMessage());
		}
	}
	@Test
	void testProcess_DECLARE_ERROR() {
		ObsLogger obsLogger = mock(ObsLogger.class);
		boolean multipleTables = true;
		boolean needToDeclare = true;
		Function<Object, Object> declareFunction = mock(Function.class);
		ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "obsLogger", obsLogger);
		ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "multipleTables", multipleTables);
		ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "needToDeclare", needToDeclare);
		ReflectionTestUtils.setField(hazelcastSchemaTargetNode, "declareFunction", declareFunction);
		hazelcastSchemaTargetNode = spy(hazelcastSchemaTargetNode);
		int ordinal = 1;
		Inbox inbox = mock(Inbox.class);
		when(inbox.isEmpty()).thenReturn(false);
		doCallRealMethod().when(inbox).drainTo(anyList(), anyInt());
		TapdataEvent tapdataEvent = mock(TapdataEvent.class);
		when(inbox.poll()).thenReturn(tapdataEvent);
		when(tapdataEvent.getTapEvent()).thenReturn(mock(TapRecordEvent.class));
		when(hazelcastSchemaTargetNode.isRunning()).thenReturn(true);
		when(declareFunction.apply(anyList())).thenThrow(RuntimeException.class);
		TapCodeException exception = assertThrows(TapCodeException.class, () -> hazelcastSchemaTargetNode.process(ordinal, inbox));
		assertEquals(VirtualTargetExCode_14.DECLARE_ERROR, exception.getCode());
	}
}
