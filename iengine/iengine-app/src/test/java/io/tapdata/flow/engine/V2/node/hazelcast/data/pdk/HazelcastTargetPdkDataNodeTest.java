package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import base.BaseTaskTest;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.observable.logging.ObsLogger;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2023-12-13 10:38
 **/
class HazelcastTargetPdkDataNodeTest extends BaseTaskTest {

	@Nested
	@DisplayName("ProcessEvents Method Test")
	class processEventsTest {
		private HazelcastTargetPdkDataNode hazelcastTargetPdkDataNode;

		@BeforeEach
		void beforeEach() {
			hazelcastTargetPdkDataNode = mock(HazelcastTargetPdkDataNode.class);
			ReflectionTestUtils.setField(hazelcastTargetPdkDataNode, "obsLogger", mockObsLogger);
			doCallRealMethod().when(hazelcastTargetPdkDataNode).processEvents(anyList());
		}

		@Test
		@SneakyThrows
		@DisplayName("Main process test, all dml event")
		void mainProcessTest() {
			int tableCount = 10;
			int rows = 3000;
			List<TapEvent> tapEvents = mockTapEvents(tableCount, rows);
			doAnswer(invocationOnMock -> {
				Object argument = invocationOnMock.getArgument(0);
				assertNotNull(argument);
				assertInstanceOf(List.class, argument);
				List list = (List) argument;
				assertEquals(rows / tableCount, list.size());
				return null;
			}).when(hazelcastTargetPdkDataNode).writeRecord(anyList());
			hazelcastTargetPdkDataNode.processEvents(tapEvents);
			verify(hazelcastTargetPdkDataNode, times(tableCount)).writeRecord(anyList());
		}

		@NotNull
		private List<TapEvent> mockTapEvents(int tableCount, int rows) {
			LinkedBlockingQueue<String> tableNames = new LinkedBlockingQueue<>();
			IntStream.range(0, tableCount).forEach(i -> tableNames.offer("table_" + (i + 1)));
			List<TapEvent> tapEvents = new ArrayList<>();
			IntStream.range(0, rows).forEach(i -> {
				TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
				String tableName;
				try {
					tableName = tableNames.take();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				tapInsertRecordEvent.table(tableName);
				when(hazelcastTargetPdkDataNode.getTgtTableNameFromTapEvent(tapInsertRecordEvent)).thenReturn(tableName);
				tableNames.offer(tableName);
				tapInsertRecordEvent.after(new HashMap<>());
				tapInsertRecordEvent.setReferenceTime(System.currentTimeMillis());
				tapEvents.add(tapInsertRecordEvent);
			});
			return tapEvents;
		}
	}
}
