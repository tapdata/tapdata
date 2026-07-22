package com.tapdata.tm.ws.cs;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.ws.dto.EditFlushCache;
import com.tapdata.tm.ws.handler.EditFlushHandler;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.messaging.Message;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;

class EditFlushListenerTest {
	private static final ObjectId TASK_OBJECT_ID = new ObjectId("507f1f77bcf86cd799439011");
	private static final String TASK_ID = TASK_OBJECT_ID.toHexString();

	private EditFlushListener listener;

	@BeforeEach
	void setUp() {
		listener = new EditFlushListener();
		EditFlushHandler.editFlushMap.clear();
	}

	@AfterEach
	void tearDown() {
		EditFlushHandler.editFlushMap.clear();
	}

	@Test
	void allFieldsIncludesTransformFieldsWhenTransformedUpdated() {
		BsonDocument updatedFields = new BsonDocument()
				.append("transformed", BsonBoolean.FALSE);

		List<String> fields = EditFlushListener.UpdateFields.allFields(updatedFields);

		assertTrue(fields.contains("transformed"));
		assertTrue(fields.contains("transformUuid"));
		assertFalse(fields.contains("transformedUuid"));
	}

	@Test
	void allFieldsIncludesStatusCompanionFieldsWhenStatusUpdated() {
		BsonDocument updatedFields = new BsonDocument()
				.append("status", new BsonString("running"));

		List<String> fields = EditFlushListener.UpdateFields.allFields(updatedFields);

		assertTrue(fields.contains("status"));
		assertTrue(fields.contains("agentId"));
		assertTrue(fields.contains("startTime"));
		assertTrue(fields.contains("scheduledTime"));
		assertTrue(fields.contains("schedulingTime"));
		assertTrue(fields.contains("stoppingTime"));
		assertTrue(fields.contains("runningTime"));
		assertTrue(fields.contains("errorTime"));
		assertTrue(fields.contains("stopTime"));
		assertTrue(fields.contains("finishTime"));
		assertTrue(fields.contains("scheduleDate"));
		assertTrue(fields.contains("stopedDate"));
		assertTrue(fields.contains("pingTime"));
	}

	@Test
	void allFieldsIncludesLogSettingWhenLogSettingUpdated() {
		BsonDocument updatedFields = new BsonDocument()
				.append("logSetting", new BsonDocument("level", new BsonString("DEBUG")));

		List<String> fields = EditFlushListener.UpdateFields.allFields(updatedFields);

		assertTrue(fields.contains("logSetting"));
		assertFalse(fields.contains("level"));
	}

	@Test
	void allFieldsReturnsEmptyWhenUpdatedFieldsIsNull() {
		List<String> fields = EditFlushListener.UpdateFields.allFields(null);

		assertTrue(fields.isEmpty());
	}

	@Test
	void containsMatchesOnlyTrackedTopLevelFields() {
		assertTrue(EditFlushListener.UpdateFields.contains(new BsonDocument("status", new BsonString("running")).keySet()));
		assertFalse(EditFlushListener.UpdateFields.contains(new BsonDocument("name", new BsonString("task")).keySet()));
		assertEquals("status", EditFlushListener.UpdateFields.STATUS.getType());
		assertTrue(EditFlushListener.UpdateFields.STATUS.getFields().contains("pingTime"));
	}

	@Test
	void entityFromUpdateAppliesOnlyTrackedCompanionFields() {
		TaskEntity task = taskEntity();
		task.setName("old-name");
		BsonDocument updatedFields = new BsonDocument()
				.append("status", new BsonString("running"))
				.append("agentId", new BsonString("agent-1"))
				.append("transformed", BsonBoolean.TRUE)
				.append("transformUuid", new BsonString("transform-1"))
				.append("name", new BsonString("new-name"));

		listener.entityFromUpdate(updatedFields, task);

		assertEquals("running", task.getStatus());
		assertEquals("agent-1", task.getAgentId());
		assertTrue(task.getTransformed());
		assertEquals("transform-1", task.getTransformUuid());
		assertEquals("old-name", task.getName());
	}

	@Test
	void entityFromUpdateAppliesNestedLogSettingAsMap() {
		TaskEntity task = taskEntity();
		BsonDocument updatedFields = new BsonDocument()
				.append("logSetting", new BsonDocument("level", new BsonString("DEBUG")));

		listener.entityFromUpdate(updatedFields, task);

		assertEquals("DEBUG", task.getLogSetting().get("level"));
	}

	@Test
	void entityFromUpdateReturnsWhenInputIsMissing() {
		TaskEntity task = taskEntity();
		task.setStatus("old");

		listener.entityFromUpdate(null, task);
		listener.entityFromUpdate(new BsonDocument("status", new BsonString("running")), null);

		assertEquals("old", task.getStatus());
	}

	@Test
	void loadVariableLeavesTaskUnchangedWhenFieldIsNotAllowed() {
		TaskEntity task = taskEntity();
		task.setStatus("old");

		listener.loadVariable("status", "running", task, List.of("agentId"));

		assertEquals("old", task.getStatus());
	}

	@Test
	void onMessageIgnoresMissingBodyRawUpdateDescriptionAndUpdatedFields() {
		try (MockedStatic<EditFlushHandler> mockedHandler = Mockito.mockStatic(EditFlushHandler.class)) {
			listener.onMessage(new TestMessage(null, null));
			listener.onMessage(new TestMessage(null, taskEntity()));
			listener.onMessage(new TestMessage(changeStream((UpdateDescription) null), taskEntity()));
			listener.onMessage(new TestMessage(changeStream(new UpdateDescription(List.of(), null)), taskEntity()));

			mockedHandler.verifyNoInteractions();
		}
	}

	@Test
	void onMessageIgnoresUntrackedUpdatedFields() {
		BsonDocument updatedFields = new BsonDocument("name", new BsonString("new-name"));

		try (MockedStatic<EditFlushHandler> mockedHandler = Mockito.mockStatic(EditFlushHandler.class)) {
			listener.onMessage(new TestMessage(changeStream(updatedFields), taskEntity()));

			mockedHandler.verifyNoInteractions();
		}
	}

	@Test
	void onMessageStopsChangeStreamWhenTrackedUpdateHasNoCaches() {
		try (MockedStatic<EditFlushHandler> mockedHandler = Mockito.mockStatic(EditFlushHandler.class)) {
			listener.onMessage(new TestMessage(changeStream(new BsonDocument("status", new BsonString("running"))), taskEntity()));

			mockedHandler.verify(EditFlushHandler::stopChangeStream);
			mockedHandler.verifyNoMoreInteractions();
		}
	}

	@Test
	void onMessageReturnsWhenTaskHasNoSubscribers() {
		EditFlushHandler.editFlushMap.put("other-task", new ArrayList<>());

		try (MockedStatic<EditFlushHandler> mockedHandler = Mockito.mockStatic(EditFlushHandler.class)) {
			listener.onMessage(new TestMessage(changeStream(new BsonDocument("status", new BsonString("running"))), taskEntity()));

			mockedHandler.verifyNoInteractions();
		}
	}

	@Test
	void onMessageSendsUpdatedTaskToEverySubscriber() {
		EditFlushHandler.editFlushMap.put(TASK_ID, List.of(
				new EditFlushCache("session-1", "receiver-1"),
				new EditFlushCache("session-2", "receiver-2")
		));
		BsonDocument updatedFields = new BsonDocument()
				.append("transformed", BsonBoolean.TRUE)
				.append("transformUuid", new BsonString("transform-1"));

		try (MockedStatic<EditFlushHandler> mockedHandler = Mockito.mockStatic(EditFlushHandler.class)) {
			listener.onMessage(new TestMessage(changeStream(updatedFields), taskEntity()));

			ArgumentCaptor<Object> firstMessage = ArgumentCaptor.forClass(Object.class);
			ArgumentCaptor<Object> secondMessage = ArgumentCaptor.forClass(Object.class);
			mockedHandler.verify(() -> EditFlushHandler.sendEditFlushMessage(eq("receiver-1"), eq(TASK_ID), firstMessage.capture()));
			mockedHandler.verify(() -> EditFlushHandler.sendEditFlushMessage(eq("receiver-2"), eq(TASK_ID), secondMessage.capture()));
			assertSentTask(firstMessage.getValue());
			assertSentTask(secondMessage.getValue());
			mockedHandler.verifyNoMoreInteractions();
		}
	}

	@Test
	void onMessageCatchesExceptionsFromHandlerBoundary() {
		EditFlushHandler.editFlushMap.put(TASK_ID, List.of(new EditFlushCache("session-1", "receiver-1")));

		try (MockedStatic<EditFlushHandler> mockedHandler = Mockito.mockStatic(EditFlushHandler.class)) {
			mockedHandler.when(() -> EditFlushHandler.sendEditFlushMessage(eq("receiver-1"), eq(TASK_ID), (Object) Mockito.any()))
					.thenThrow(new RuntimeException("send failed"));

			listener.onMessage(new TestMessage(changeStream(new BsonDocument("status", new BsonString("running"))), taskEntity()));

			mockedHandler.verify(() -> EditFlushHandler.sendEditFlushMessage(eq("receiver-1"), eq(TASK_ID), (Object) Mockito.any()));
		}
	}

	private static TaskEntity taskEntity() {
		TaskEntity task = new TaskEntity();
		task.setId(TASK_OBJECT_ID);
		return task;
	}

	private static ChangeStreamDocument<Document> changeStream(BsonDocument updatedFields) {
		return changeStream(new UpdateDescription(List.of(), updatedFields));
	}

	private static ChangeStreamDocument<Document> changeStream(UpdateDescription updateDescription) {
		return new ChangeStreamDocument<>(
				"update",
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				updateDescription,
				null,
				null,
				null,
				null,
				null
		);
	}

	private static void assertSentTask(Object value) {
		assertTrue(value instanceof TaskDto);
		TaskDto taskDto = (TaskDto) value;
		assertEquals(TASK_OBJECT_ID, taskDto.getId());
		assertTrue(taskDto.getTransformed());
		assertEquals("transform-1", taskDto.getTransformUuid());
	}

	private static class TestMessage implements Message<ChangeStreamDocument<Document>, TaskEntity> {
		private final ChangeStreamDocument<Document> raw;
		private final TaskEntity body;

		private TestMessage(ChangeStreamDocument<Document> raw, TaskEntity body) {
			this.raw = raw;
			this.body = body;
		}

		@Override
		public ChangeStreamDocument<Document> getRaw() {
			return raw;
		}

		@Override
		public TaskEntity getBody() {
			return body;
		}

		@Override
		public Message.MessageProperties getProperties() {
			return null;
		}
	}
}
