package com.tapdata.tm.servingindex;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * P1-2 · TM 索引读回触发服务（{@link ServingIndexService}）。
 *
 * <p>只锚定<b>触发契约</b>（ADR-0009）：按连接的 access-node 解析目标引擎，把
 * {@code data.type = "queryIndexes"} 的 {@code type="pipe"} 消息<b>非阻塞</b>发射给该引擎，
 * 并把发起方前端 ws 会话作为 {@code sender}（引擎回推 {@code receiver=sender}，经 PipeHandler 中继回前端）。
 * 应答不在此路径（前端在自己的 ws 上按 {@code connectionId+tableName+reqId} 关联，见 ADR-0009）。</p>
 */
class ServingIndexServiceTest {

	private WorkerService workerService;
	private AgentGroupService agentGroupService;
	private MessageQueueService messageQueueService;
	private ServingIndexService service;
	private UserDetail user;

	@BeforeEach
	void setUp() {
		workerService = mock(WorkerService.class);
		agentGroupService = mock(AgentGroupService.class);
		messageQueueService = mock(MessageQueueService.class);
		service = new ServingIndexService(workerService, agentGroupService, messageQueueService);
		user = mock(UserDetail.class);
	}

	private DataSourceConnectionDto conn(String name) {
		DataSourceConnectionDto dto = new DataSourceConnectionDto();
		dto.setName(name);
		// accessNodeType 留空 → 自动分配路径（findAvailableAgent）
		return dto;
	}

	private Worker worker(String processId) {
		Worker w = new Worker();
		w.setProcessId(processId);
		return w;
	}

	@Test
	@DisplayName("发射 queryIndexes pipe：receiver=解析出的引擎，sender=前端会话，data 含 type/tableName/reqId/connections（ADR-0009）")
	void sendQueryIndexes_emitsPipeToResolvedEngine() {
		when(workerService.findAvailableAgent(user)).thenReturn(Collections.singletonList(worker("engine-1")));

		service.sendQueryIndexes(conn("mongo-a"), "orders", "r-1", "fe-session-9", user);

		ArgumentCaptor<MessageQueueDto> cap = ArgumentCaptor.forClass(MessageQueueDto.class);
		verify(messageQueueService).sendMessage(cap.capture());
		MessageQueueDto dto = cap.getValue();
		assertEquals("pipe", dto.getType());
		assertEquals("engine-1", dto.getReceiver());
		assertEquals("fe-session-9", dto.getSender());
		assertTrue(dto.getData() instanceof Map, "data should be a Map");
		Map<?, ?> data = (Map<?, ?>) dto.getData();
		assertEquals("queryIndexes", data.get("type"));
		assertEquals("orders", data.get("tableName"));
		assertEquals("r-1", data.get("reqId"));
		assertTrue(data.get("connections") instanceof Map,
				"连接体须嵌套在 'connections' 键下，供引擎 QueryIndexesHandler 反解为 Connections");
	}

	@Test
	@DisplayName("手动指定 access-node：按 access-node 解析引擎，priorityProcessId 命中则优先（同 testConnection）")
	void sendQueryIndexes_manualAccessNode_honorsPriority() {
		DataSourceConnectionDto dto = conn("mongo-a");
		dto.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
		dto.setPriorityProcessId("engine-2");
		when(agentGroupService.getProcessNodeListWithGroup(dto, user))
				.thenReturn(Arrays.asList("engine-1", "engine-2"));
		when(workerService.findAvailableAgentByAccessNode(user, Arrays.asList("engine-1", "engine-2")))
				.thenReturn(Arrays.asList(worker("engine-1"), worker("engine-2")));

		service.sendQueryIndexes(dto, "orders", "r-2", "fe-session-9", user);

		ArgumentCaptor<MessageQueueDto> cap = ArgumentCaptor.forClass(MessageQueueDto.class);
		verify(messageQueueService).sendMessage(cap.capture());
		assertEquals("engine-2", cap.getValue().getReceiver());
		verify(workerService, never()).findAvailableAgent(any());
	}

	@Test
	@DisplayName("无可用引擎：非阻塞返回、绝不发射（不误报成功）")
	void sendQueryIndexes_noAgent_doesNotEmit() {
		when(workerService.findAvailableAgent(user)).thenReturn(Collections.emptyList());

		service.sendQueryIndexes(conn("mongo-a"), "orders", "r-1", "fe-session-9", user);

		verify(messageQueueService, never()).sendMessage(any());
	}
}
