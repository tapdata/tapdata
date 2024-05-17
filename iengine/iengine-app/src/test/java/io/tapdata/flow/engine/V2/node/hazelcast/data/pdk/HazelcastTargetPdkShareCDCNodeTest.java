package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.jet.core.Processor;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.sharecdc.LogContent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.common.sharecdc.ShareCdcUtil;
import io.tapdata.construct.HazelcastConstruct;
import io.tapdata.entity.TapProcessorNodeContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.TapCodeException;
import io.tapdata.node.pdk.processor.TapProcessorNode;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-01-31 12:07
 **/
@DisplayName("HazelcastTargetPdkShareCDCNode Class Test")
class HazelcastTargetPdkShareCDCNodeTest {

	private HazelcastTargetPdkShareCDCNode hazelcastTargetPdkShareCDCNode;

	@BeforeEach
	void setUp() {
		hazelcastTargetPdkShareCDCNode = mock(HazelcastTargetPdkShareCDCNode.class);
	}

	@Nested
	@DisplayName("Method doInit test")
	class doInitTest {

		private Processor.Context context;
		private ScheduledExecutorService flushOffsetExecutor;
		private DataProcessorContext dataProcessorContext;

		@BeforeEach
		void setUp() {
			context = mock(Processor.Context.class);
			doCallRealMethod().when(hazelcastTargetPdkShareCDCNode).doInit(context);
			ThreadPoolExecutorEx queueConsumerThreadPool = AsyncUtils.createThreadPoolExecutor("test", 1, new ThreadGroup("test"), HazelcastTargetPdkBaseNodeTest.class.getSimpleName());
			ReflectionTestUtils.setField(hazelcastTargetPdkShareCDCNode, "queueConsumerThreadPool", queueConsumerThreadPool);
			flushOffsetExecutor = new ScheduledThreadPoolExecutor(1, r -> {
				Thread thread = new Thread(r);
				thread.setName("test");
				return thread;
			});
			flushOffsetExecutor = spy(flushOffsetExecutor);
			ReflectionTestUtils.setField(hazelcastTargetPdkShareCDCNode, "flushOffsetExecutor", flushOffsetExecutor);
			Node node = new TableNode();
			node.setId("1");
			node.setName("test");
			when(hazelcastTargetPdkShareCDCNode.getNode()).thenReturn(node);
			dataProcessorContext = new DataProcessorContext.DataProcessorContextBuilder().build();
			ReflectionTestUtils.setField(hazelcastTargetPdkShareCDCNode, "dataProcessorContext", dataProcessorContext);
		}

		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			TapProcessorNodeContext tapProcessorNodeContext = mock(TapProcessorNodeContext.class);
			when(hazelcastTargetPdkShareCDCNode.initTapProcessorNodeContext()).thenReturn(tapProcessorNodeContext);
			TapProcessorNode processorNode = mock(TapProcessorNode.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkShareCDCNode, "processorNode", processorNode);

			hazelcastTargetPdkShareCDCNode.doInit(context);

			verify(hazelcastTargetPdkShareCDCNode).doInit(context);
			verify(hazelcastTargetPdkShareCDCNode).initTapProcessorNodeContext();
			assertEquals(10000, hazelcastTargetPdkShareCDCNode.targetBatch);
			assertEquals(1000, hazelcastTargetPdkShareCDCNode.targetBatchIntervalMs);
			verify(processorNode).doInit(context, tapProcessorNodeContext);
			assertTrue(hazelcastTargetPdkShareCDCNode.illegalDateAcceptable);
		}
	}
}
