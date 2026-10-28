package com.tapdata.tm.audit;

import com.tapdata.tm.Settings.constant.SettingsEnum;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.bson.Document;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 核心服务生命周期的 SYSTEM 事件（需求 9.9 / US-09）：Management Service 自身启停；Engine 与 API Server
 * 由心跳判定启动或恢复连接，由 componentStopped 判定正常停止，由心跳超时判定失联。服务状态跃迁保存在
 * AuditServiceStates 并原子更新，多个 TM 实例只记录一次；心跳仅在本实例未确认运行时访问数据库。
 */
@Slf4j
@Component
public class ServiceLifecycleAudit {
	public static final String COLLECTION = "AuditServiceStates";
	static final String UP = "up";
	static final String DOWN = "down";
	static final String LOST = "lost";
	private static final Map<String, String> SERVICE_NAMES = Map.of(
			"connector", "Engine", "api-server", "API Server", "management", "Management Service");

	private final AuditEventService auditEventService;
	private final MongoTemplate mongoTemplate;
	private final Map<String, Boolean> knownUp = new ConcurrentHashMap<>();
	private final String managementInstance = UUID.randomUUID().toString();

	public ServiceLifecycleAudit(AuditEventService auditEventService, MongoTemplate mongoTemplate) {
		this.auditEventService = auditEventService;
		this.mongoTemplate = mongoTemplate;
	}

	/** Worker 心跳：首次出现或停止后再次上报为启动，失联后再次上报为恢复连接。 */
	public void onHeartbeat(String workerType, String processId, String hostname, String ip) {
		if (!SERVICE_NAMES.containsKey(workerType) || processId == null) {
			return;
		}
		String key = workerType + ":" + processId;
		if (Boolean.TRUE.equals(knownUp.get(key))) {
			return;
		}
		try {
			String previous = transition(key, UP, workerType, processId, hostname, ip);
			knownUp.put(key, true);
			if (UP.equals(previous)) {
				return;
			}
			AuditAction action = LOST.equals(previous) ? AuditAction.SERVICE_RECONNECT : AuditAction.SERVICE_START;
			record(action, AuditOutcome.SUCCESS, workerType, processId, hostname, ip, null);
		} catch (Exception e) {
			log.warn("Audit service heartbeat failed, key={}", key, e);
		}
	}

	/** 组件主动上报停止（tapdata-agent componentStopped）。 */
	public void onStopped(String workerType, String processId, String hostname, String ip) {
		markDown(workerType, processId, hostname, ip, DOWN, AuditAction.SERVICE_STOP, AuditOutcome.SUCCESS, null);
	}

	/** 定时检查心跳超时的服务，记录失联；只处理状态仍为运行中的服务。 */
	@Scheduled(fixedDelay = 30000, initialDelay = 60000)
	@SchedulerLock(name = "audit_service_lost_lock", lockAtMostFor = "25s", lockAtLeastFor = "5s")
	public void detectLost() {
		try {
			int overTime = SettingsEnum.WORKER_HEART_OVERTIME.getIntValue(30);
			long deadline = System.currentTimeMillis() - overTime * 1000L;
			Query query = Query.query(Criteria.where("state").is(UP).and("kind").in("connector", "api-server"));
			for (Document state : mongoTemplate.find(query, Document.class, COLLECTION)) {
				String type = state.getString("kind");
				String processId = state.getString("processId");
				Document worker = mongoTemplate.findOne(Query.query(Criteria.where("process_id").is(processId).and("worker_type").is(type)),
						Document.class, "Workers");
				Long ping = worker == null ? null : toLong(worker.get("ping_time"));
				boolean deleted = worker != null && Boolean.TRUE.equals(worker.get("isDeleted"));
				if (deleted || ping == null || ping <= deadline) {
					markDown(type, processId, state.getString("hostname"), state.getString("ip"), LOST,
							AuditAction.SERVICE_DISCONNECT, AuditOutcome.FAILED, "audit_reason_heartbeat_timeout");
				}
			}
		} catch (Exception e) {
			log.warn("Audit lost service detection failed", e);
		}
	}

	@EventListener(ApplicationReadyEvent.class)
	public void onManagementStarted() {
		String[] host = localHost();
		record(AuditAction.SERVICE_START, AuditOutcome.SUCCESS, "management", managementInstance, host[0], host[1], null);
	}

	@EventListener(ContextClosedEvent.class)
	public void onManagementStopped() {
		String[] host = localHost();
		record(AuditAction.SERVICE_STOP, AuditOutcome.SUCCESS, "management", managementInstance, host[0], host[1], null);
	}

	private void markDown(String workerType, String processId, String hostname, String ip, String state,
						  AuditAction action, AuditOutcome outcome, String reason) {
		if (!SERVICE_NAMES.containsKey(workerType) || processId == null) {
			return;
		}
		String key = workerType + ":" + processId;
		knownUp.remove(key);
		try {
			Query query = Query.query(Criteria.where("_id").is(key).and("state").is(UP));
			Document previous = mongoTemplate.findAndModify(query, new Update().set("state", state).set("at", new Date()),
					FindAndModifyOptions.options().returnNew(false), Document.class, COLLECTION);
			if (previous != null) {
				record(action, outcome, workerType, processId,
						hostname != null ? hostname : previous.getString("hostname"), ip != null ? ip : previous.getString("ip"), reason);
			}
		} catch (Exception e) {
			log.warn("Audit service stop failed, key={}", key, e);
		}
	}

	private String transition(String key, String state, String workerType, String processId, String hostname, String ip) {
		Update update = new Update().set("state", state).set("at", new Date()).set("kind", workerType).set("processId", processId);
		if (hostname != null) {
			update.set("hostname", hostname);
		}
		if (ip != null) {
			update.set("ip", ip);
		}
		Document previous = mongoTemplate.findAndModify(Query.query(Criteria.where("_id").is(key)), update,
				FindAndModifyOptions.options().upsert(true).returnNew(false), Document.class, COLLECTION);
		return previous == null ? null : previous.getString("state");
	}

	private void record(AuditAction action, AuditOutcome outcome, String workerType, String processId, String hostname, String ip, String reason) {
		AuditEvent event = AuditEvent.of(action, outcome).system();
		String service = SERVICE_NAMES.get(workerType);
		event.setNode(new AuditNode(service, processId, hostname, ip));
		event.object(processId, service + (hostname == null ? "" : " @ " + hostname));
		event.setReason(reason);
		auditEventService.record(event, null);
	}

	private static Long toLong(Object value) {
		return value instanceof Number number ? number.longValue() : null;
	}

	private static String[] localHost() {
		try {
			InetAddress local = InetAddress.getLocalHost();
			return new String[]{local.getHostName(), local.getHostAddress()};
		} catch (Exception e) {
			return new String[]{null, null};
		}
	}

	List<String> knownServices() {
		return List.copyOf(knownUp.keySet());
	}
}
