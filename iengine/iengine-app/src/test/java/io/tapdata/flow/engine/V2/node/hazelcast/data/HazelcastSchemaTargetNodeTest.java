package io.tapdata.flow.engine.V2.node.hazelcast.data;

import base.hazelcast.BaseHazelcastNodeTest;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
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
}
