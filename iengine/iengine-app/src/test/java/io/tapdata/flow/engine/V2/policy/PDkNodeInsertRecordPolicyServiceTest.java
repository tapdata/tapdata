package io.tapdata.flow.engine.V2.policy;

import com.tapdata.tm.commons.dag.DmlPolicy;
import com.tapdata.tm.commons.dag.DmlPolicyEnum;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.TapPdkViolateUniqueEx;
import io.tapdata.node.pdk.ConnectorNodeService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2024-07-29 18:54
 **/
@DisplayName("Class PDkNodeInsertRecordPolicyService Test")
class PDkNodeInsertRecordPolicyServiceTest {

	private ConnectorCapabilities connectorCapabilities;
	private String associateId;
	private TaskDto taskDto;
	private TableNode node;
	private MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic;
	private MockedStatic<ConnectorNodeService> connectorNodeServiceMockedStatic;
	private PDkNodeInsertRecordPolicyService policyService;

	@BeforeEach
	void setUp() {
		associateId = "test_id";
		ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
		connectorCapabilities = ConnectorCapabilities.create();
		TapConnectorContext tapConnectorContext = mock(TapConnectorContext.class);
		ConnectorNode connectorNode = mock(ConnectorNode.class);
		ConnectorNodeService connectorNodeService = mock(ConnectorNodeService.class);
		when(tapConnectorContext.getConnectorCapabilities()).thenReturn(connectorCapabilities);
		when(connectorNode.getConnectorContext()).thenReturn(tapConnectorContext);
		when(connectorNodeService.getConnectorNode(associateId)).thenReturn(connectorNode);
		taskDto = new TaskDto();
		taskDto.setId(new ObjectId());
		node = new TableNode();
		node.setId("1");
		DmlPolicy dmlPolicy = new DmlPolicy();
		dmlPolicy.setInsertPolicy(DmlPolicyEnum.update_on_exists);
		node.setDmlPolicy(dmlPolicy);
		obsLoggerFactoryMockedStatic = Mockito.mockStatic(ObsLoggerFactory.class);
		connectorNodeServiceMockedStatic = Mockito.mockStatic(ConnectorNodeService.class);
		obsLoggerFactoryMockedStatic.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
		connectorNodeServiceMockedStatic.when(ConnectorNodeService::getInstance).thenReturn(connectorNodeService);
		policyService = new PDkNodeInsertRecordPolicyService(taskDto, node, associateId);
		ObsLogger obsLogger = mock(ObsLogger.class);
		ReflectionTestUtils.setField(policyService, "obsLogger", obsLogger);
	}

	@AfterEach
	void tearDown() {
		obsLoggerFactoryMockedStatic.close();
		connectorNodeServiceMockedStatic.close();
	}

	@Test
	@DisplayName("test dml insert policy is update_on_exists")
	void test1() {
		List<TapRecordEvent> events = Collections.singletonList(TapInsertRecordEvent.create().after(new Document("id", 1)));
		assertDoesNotThrow(() -> policyService.writeRecordWithPolicyControl(
				"test",
				events,
				tapRecordEvents -> {
					assertEquals(events, tapRecordEvents);
					return null;
				}
		));
		assertEquals(DmlPolicyEnum.just_insert.name(), connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY));
	}

	@Test
	@DisplayName("test over continuous duplicate key error threshold: 10")
	void test2() {
		List<TapRecordEvent> events = Collections.singletonList(TapInsertRecordEvent.create().after(new Document("id", 1)));
		for (int i = 0; i < PDkNodeInsertRecordPolicyService.DEFAULT_DUPLICATE_KEY_ERROR_THRESHOLD + 1; i++) {
			AtomicInteger counter = new AtomicInteger(0);
			assertDoesNotThrow(() -> policyService.writeRecordWithPolicyControl(
					"test",
					events,
					tapRecordEvents -> {
						if (counter.getAndIncrement() == 0) {
							throw new TapPdkViolateUniqueEx("1", "test", "test", "unique", null);
						}
						return null;
					}
			));
		}
		assertDoesNotThrow(() -> policyService.writeRecordWithPolicyControl(
				"test",
				events,
				tapRecordEvents -> null
		));
		assertEquals(DmlPolicyEnum.update_on_exists.name(), connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY));
	}

	@Test
	@DisplayName("test insert policy is not update_on_exists")
	void test3() {
		node.getDmlPolicy().setInsertPolicy(DmlPolicyEnum.ignore_on_exists);
		policyService = new PDkNodeInsertRecordPolicyService(taskDto, node, associateId);
		List<TapRecordEvent> events = Collections.singletonList(TapInsertRecordEvent.create().after(new Document("id", 1)));
		assertDoesNotThrow(() -> policyService.writeRecordWithPolicyControl(
				"test",
				events,
				tapRecordEvents -> null
		));
		assertNull(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY));
	}

	@Test
	@DisplayName("test input event list is empty")
	void test4() {
		List<TapRecordEvent> events = Collections.emptyList();
		assertDoesNotThrow(() -> policyService.writeRecordWithPolicyControl(
				"test",
				events,
				tapRecordEvents -> null
		));
		assertNull(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY));
	}

	@Test
	@DisplayName("test node's dml policy is null")
	void test5() {
		node.setDmlPolicy(null);
		policyService = new PDkNodeInsertRecordPolicyService(taskDto, node, associateId);
		List<TapRecordEvent> events = Collections.singletonList(TapInsertRecordEvent.create().after(new Document("id", 1)));
		assertDoesNotThrow(() -> policyService.writeRecordWithPolicyControl(
				"test",
				events,
				tapRecordEvents -> null
		));
		assertNull(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY));
	}

	@Test
	@DisplayName("test node's unique index enable is false")
	void test6() {
		node.setUniqueIndexEnable(false);
		policyService = new PDkNodeInsertRecordPolicyService(taskDto, node, associateId);
		List<TapRecordEvent> events = Collections.singletonList(TapInsertRecordEvent.create().after(new Document("id", 1)));
		assertDoesNotThrow(() -> policyService.writeRecordWithPolicyControl(
				"test",
				events,
				tapRecordEvents -> null
		));
		assertNull(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY));
	}

	@Test
	@DisplayName("test just_insert and update_on_exists all error")
	void test7() {
		List<TapRecordEvent> events = Collections.singletonList(TapInsertRecordEvent.create().after(new Document("id", 1)));
		TapPdkViolateUniqueEx error = new TapPdkViolateUniqueEx("1", "test", "test", "unique", null);
		TapPdkViolateUniqueEx tapPdkViolateUniqueEx = assertThrows(TapPdkViolateUniqueEx.class, () -> policyService.writeRecordWithPolicyControl(
				"test",
				events,
				tapRecordEvents -> {
					throw error;
				}
		));
		assertSame(error, tapPdkViolateUniqueEx);
	}

	@Test
	@DisplayName("test error is not TapPdkViolateUniqueEx")
	void test8() {
		List<TapRecordEvent> events = Collections.singletonList(TapInsertRecordEvent.create().after(new Document("id", 1)));
		Exception error = new Exception();
		Exception exception = assertThrows(Exception.class, () -> policyService.writeRecordWithPolicyControl(
				"test",
				events,
				tapRecordEvents -> {
					throw error;
				}
		));
		assertSame(error, exception);
	}
}