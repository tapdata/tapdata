package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TapdataTaskSchedulerEngineStartTest {

	@Test
	@DisplayName("engine startup recovery should refresh queued running task ping time by expected startup slot")
	void shouldRefreshQueuedRunningTaskPingTimeByExpectedStartupSlot() {
		TapdataTaskScheduler scheduler = new TapdataTaskScheduler();
		TaskDto firstTask = buildTask("first");
		TaskDto secondTask = buildTask("second");
		TaskDto thirdTask = buildTask("third");
		RecordingClientMongoOperator clientMongoOperator = new RecordingClientMongoOperator(Arrays.asList(firstTask, secondTask, thirdTask));
		ReflectionTestUtils.setField(scheduler, "clientMongoOperator", clientMongoOperator);
		ReflectionTestUtils.setField(scheduler, "instanceNo", "agent-1");
		ReflectionTestUtils.setField(scheduler, "engineStartTaskSchedulerStarted", true);

		scheduler.runTaskIfNeedWhenEngineStart();

		assertEquals(3, clientMongoOperator.updates.size());
		assertEquals(firstTask.getId(), queryId(clientMongoOperator.updateQueries.get(0)));
		assertEquals(secondTask.getId(), queryId(clientMongoOperator.updateQueries.get(1)));
		assertEquals(thirdTask.getId(), queryId(clientMongoOperator.updateQueries.get(2)));

		long firstPingTime = pingTime(clientMongoOperator.updates.get(0));
		long secondPingTime = pingTime(clientMongoOperator.updates.get(1));
		long thirdPingTime = pingTime(clientMongoOperator.updates.get(2));
		assertEquals(firstPingTime, firstTask.getPingTime());
		assertEquals(secondPingTime, secondTask.getPingTime());
		assertEquals(thirdPingTime, thirdTask.getPingTime());
	}

	private static long pingTime(Update update) {
		Document set = update.getUpdateObject().get("$set", Document.class);
		return ((Number) set.get(TaskDto.PING_TIME_FIELD)).longValue();
	}

	private static Object queryId(Query query) {
		return query.getQueryObject().get("_id");
	}

	private static TaskDto buildTask(String name) {
		TaskDto taskDto = new TaskDto();
		taskDto.setId(ObjectId.get());
		taskDto.setName(name);
		taskDto.setStatus(TaskDto.STATUS_RUNNING);
		return taskDto;
	}

	private static class RecordingClientMongoOperator extends ClientMongoOperator {
		private final List<TaskDto> tasks;
		private final List<Update> updates = new ArrayList<>();
		private final List<Query> updateQueries = new ArrayList<>();
		private final List<String> updateCollections = new ArrayList<>();

		private RecordingClientMongoOperator(List<TaskDto> tasks) {
			this.tasks = tasks;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T> List<T> find(Query query, String collection, Class<T> className) {
			assertEquals(ConnectorConstant.TASK_COLLECTION, collection);
			assertEquals(TaskDto.class, className);
			return (List<T>) tasks;
		}

		@Override
		public UpdateResult update(Query query, Update update, String collection) {
			updates.add(update);
			updateQueries.add(query);
			updateCollections.add(collection);
			return UpdateResult.acknowledged(1L, 1L, null);
		}

		@Override
		public <T> T updateById(Update update, String collection, String id, Class<T> className) {
			throw new AssertionError("Task ping time refresh must use query update instead of updateById");
		}
	}
}
