package com.tapdata.tm.servingindex;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexField;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * P1-2 · TM 侧「服务型索引」读回触发服务（TAP-12057 / ADR-0009）。
 *
 * <p>加载勾选面板要「按连接 + 表读回该 collection 全部物理索引」。传输机制见
 * <a href="../../../../../../../../adr/0009-p1-2-index-readback-transport.md">ADR-0009</a>：<b>不</b>做阻塞式同步 REST，
 * 而是复用平台既有「面向连接的运维动作」推送——本服务按连接的 access-node 解析目标引擎（与
 * {@code testConnection} 同款），把 {@code data.type = "queryIndexes"} 的 {@code type="pipe"} 消息<b>非阻塞</b>
 * 发射给该引擎；引擎侧 {@code QueryIndexesHandler} 读回后把自描述结果推回<b>发起方前端 ws 会话</b>
 * （{@code receiver=sender}），前端按 {@code connectionId + tableName + reqId} 关联。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：本服务只做「解析引擎 + 发射消息」，索引本体读写在引擎侧经 PDK 连接器作用于
 * 用户库；本包（{@code com.tapdata.tm.servingindex..}）<b>绝不</b>依赖 {@code MongoTemplate}
 * （由 {@code ServingIndexPackageRedlineArchTest} 编译期强制）。</p>
 */
@Service
@Slf4j
public class ServingIndexService {

	/** 引擎侧 {@code QueryIndexesHandler} 的 {@code @EventHandlerAnnotation(type=...)} 派发键。 */
	static final String ACTION_TYPE = "queryIndexes";
	/** 引擎侧 {@code CreateIndexHandler} 的派发键（P3-3 落地腿）。 */
	static final String CREATE_ACTION_TYPE = "createIndex";
	/** 面向连接的运维动作外层信封类型（与 testConnection 一致）。 */
	static final String PIPE = "pipe";

	private final WorkerService workerService;
	private final AgentGroupService agentGroupService;
	private final MessageQueueService messageQueueService;

	public ServingIndexService(WorkerService workerService, AgentGroupService agentGroupService,
							   MessageQueueService messageQueueService) {
		this.workerService = workerService;
		this.agentGroupService = agentGroupService;
		this.messageQueueService = messageQueueService;
	}

	/**
	 * 触发「按连接 + 表读回索引」：解析目标引擎、非阻塞发射 {@code queryIndexes} pipe 消息。
	 *
	 * @param connectionDto 目标连接（含 access-node 配置）
	 * @param tableName     目标表
	 * @param reqId         前端生成的关联键（引擎在结果里回显，见 ADR-0009）
	 * @param sender        发起方前端 ws 会话 id（引擎回推的 {@code receiver}，经 PipeHandler 中继回该会话）
	 * @param user          发起用户
	 */
	public void sendQueryIndexes(DataSourceConnectionDto connectionDto, String tableName, String reqId,
								 String sender, UserDetail user) {
		String processId = resolveProcessId(connectionDto, tableName, user);
		if (processId == null) {
			return;
		}

		MessageQueueDto queueDto = new MessageQueueDto();
		queueDto.setSender(sender);
		queueDto.setReceiver(processId);
		queueDto.setType(PIPE);
		queueDto.setData(buildData(connectionDto, tableName, reqId));

		log.info("build send query indexes pipe, processId = {}, table = {}, reqId = {}", processId, tableName, reqId);
		messageQueueService.sendMessage(queueDto);
	}

	/**
	 * 解析目标引擎的 {@code processId}：与 {@code testConnection}（{@code DataSourceServiceImpl#sendTestConnection}）
	 * 同款的 access-node 解析，避免打到连不上用户库的引擎。找不到可用引擎返回 {@code null}（调用方不发消息）。
	 */
	private String resolveProcessId(DataSourceConnectionDto connectionDto, String tableName, UserDetail user) {
		List<Worker> availableAgent;
		if (StringUtils.isNotEmpty(connectionDto.getAccessNodeType())
				&& AccessNodeTypeEnum.isManually(connectionDto.getAccessNodeType())) {
			availableAgent = workerService.findAvailableAgentByAccessNode(user,
					agentGroupService.getProcessNodeListWithGroup(connectionDto, user));
			List<String> processIds = availableAgent.stream().map(Worker::getProcessId).collect(Collectors.toList());
			if (StringUtils.isNotEmpty(connectionDto.getPriorityProcessId())
					&& processIds.contains(connectionDto.getPriorityProcessId())) {
				availableAgent = availableAgent.stream()
						.filter(worker -> worker.getProcessId().equals(connectionDto.getPriorityProcessId()))
						.collect(Collectors.toList());
			}
		} else {
			availableAgent = workerService.findAvailableAgent(user);
		}
		if (CollectionUtils.isEmpty(availableAgent)) {
			log.info("serving index action, agent not found, connection = {}, table = {}",
					connectionDto.getName(), tableName);
			return null;
		}
		return availableAgent.get(0).getProcessId();
	}

	/**
	 * 触发「按连接 + 表建索引」（P3-3 落地腿）：解析目标引擎、非阻塞发射 {@code createIndex} pipe。
	 *
	 * <p>入参 {@code indexes} 必须是<b>已比对出的「将创建」那一桶</b>（{@code ServingIndexLandingPlanner}
	 * 的产出）——引擎侧不做身份比对，收到什么就建什么。空桶不该走到这里（引擎会响亮失败）。</p>
	 *
	 * @param indexes 将创建的索引；按 {@code CreateIndexHandler} 契约拍平为
	 *                {@code {name, unique, fields:[{field, asc}]}}
	 * @param sender  引擎回推 {@code createIndexResult} 的地址；落地腿传
	 *                {@link ServingIndexRendezvous#newToken()}（ADR-0012 D1）
	 */
	public void sendCreateIndexes(DataSourceConnectionDto connectionDto, String tableName, List<ServingIndex> indexes,
								  String reqId, String sender, UserDetail user) {
		String processId = resolveProcessId(connectionDto, tableName, user);
		if (processId == null) {
			return;
		}
		Map<String, Object> data = buildData(connectionDto, tableName, reqId);
		data.put("type", CREATE_ACTION_TYPE);
		data.put("indexes", toWireIndexes(indexes));

		MessageQueueDto queueDto = new MessageQueueDto();
		queueDto.setSender(sender);
		queueDto.setReceiver(processId);
		queueDto.setType(PIPE);
		queueDto.setData(data);

		log.info("build send create index pipe, processId = {}, table = {}, reqId = {}, indexes = {}",
				processId, tableName, reqId, indexes == null ? 0 : indexes.size());
		messageQueueService.sendMessage(queueDto);
	}

	/**
	 * 按 {@code CreateIndexHandler} 契约拍平：{@code {name, unique, fields:[{field, asc}]}}。
	 *
	 * <p>刻意<b>不</b>直接塞 {@code ServingIndex} 让 Jackson 序列化——它还带 {@code collected}/
	 * {@code attribution} 等纯展示字段（ADR-0011），那些不该出现在下发给引擎的动作载荷里。</p>
	 */
	private List<Map<String, Object>> toWireIndexes(List<ServingIndex> indexes) {
		List<Map<String, Object>> wire = new ArrayList<>();
		if (indexes == null) {
			return wire;
		}
		for (ServingIndex index : indexes) {
			List<Map<String, Object>> fields = new ArrayList<>();
			if (index.getFields() != null) {
				for (ServingIndexField field : index.getFields()) {
					Map<String, Object> f = new HashMap<>();
					f.put("field", field.getField());
					f.put("asc", field.getAsc());
					fields.add(f);
				}
			}
			Map<String, Object> one = new HashMap<>();
			one.put("name", index.getName());
			one.put("unique", index.getUnique());
			one.put("fields", fields);
			wire.add(one);
		}
		return wire;
	}

	/**
	 * 组装 pipe 载荷：连接体嵌套在 {@code connections} 键下（供引擎 {@code QueryIndexesHandler}
	 * 反解为 {@code Connections}），并带 {@code tableName} / {@code reqId} / 派发键 {@code type}。
	 */
	private Map<String, Object> buildData(DataSourceConnectionDto connectionDto, String tableName, String reqId) {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> connections;
		try {
			String json = objectMapper.writeValueAsString(connectionDto);
			connections = objectMapper.readValue(json, Map.class);
		} catch (JsonProcessingException e) {
			log.warn("serialize connection failed, connection = {}", connectionDto.getName());
			throw new BizException("SystemError");
		}
		Map<String, Object> data = new HashMap<>();
		data.put("type", ACTION_TYPE);
		data.put("tableName", tableName);
		data.put("reqId", reqId);
		data.put("connections", connections);
		return data;
	}
}
