package com.tapdata.tm.audit;

import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;

import static com.tapdata.tm.audit.AuditAction.*;

/**
 * 旧 addUserLog(Modular, Operation) 调用到统一 Action 的映射。返回 null 的组合沿用旧格式记录，
 * 以免丢失尚未迁移的调用点；新代码应直接使用 @Audited 或 AuditEventService。
 */
public final class LegacyAuditMapping {
	private LegacyAuditMapping() {
	}

	public static AuditAction map(Modular modular, Operation operation) {
		if (modular == null || operation == null) {
			return null;
		}
		return switch (modular) {
			case SYNC, MIGRATION, LOG_COLLECTOR, CONN_HEARTBEAT, JOBS -> task(operation);
			case MEM_CACHE -> switch (operation) {
				case CREATE -> LIVE_CACHE_CREATE;
				case UPDATE, BATCH_UPDATE, RENAME -> LIVE_CACHE_UPDATE;
				case DELETE -> LIVE_CACHE_DELETE;
				case COPY -> LIVE_CACHE_COPY;
				default -> task(operation);
			};
			case CONNECTION -> switch (operation) {
				case CREATE -> DATASOURCE_CREATE;
				case UPDATE, RENAME, BATCH_UPDATE -> DATASOURCE_UPDATE;
				case COPY -> DATASOURCE_COPY;
				case DELETE -> DATASOURCE_DELETE;
				default -> null;
			};
			case AGENT, WORKERS -> switch (operation) {
				case START -> SERVICE_START;
				case STOP -> SERVICE_STOP;
				case RESTART_AGENT -> SERVICE_RESTART;
				case UPDATE -> SERVICE_UPGRADE;
				case CREATE -> AGENT_CREATE;
				case DELETE -> AGENT_DELETE;
				default -> null;
			};
			case INSPECT -> switch (operation) {
				case CREATE -> INSPECT_CREATE;
				case UPDATE -> INSPECT_UPDATE;
				case DELETE -> INSPECT_DELETE;
				case START -> INSPECT_START;
				case STOP -> INSPECT_STOP;
				default -> null;
			};
			case MESSAGE -> switch (operation) {
				case READ, READ_ALL, UNREAD -> MESSAGE_READ;
				case DELETE, DELETE_ALL -> MESSAGE_DELETE;
				default -> null;
			};
			case USER_NOTIFICATION -> operation == Operation.UPDATE ? NOTIFICATION_UPDATE : null;
			case USER -> switch (operation) {
				case CREATE -> USER_CREATE;
				case UPDATE -> USER_UPDATE;
				case BATCH_UPDATE -> USER_IMPORT;
				case DELETE -> USER_DELETE;
				case LOGIN -> LOGIN;
				case LOGOUT -> LOGOUT;
				case UPDATE_PHONE, BIND_PHONE, UPDATE_EMAIL, BIND_EMAIL -> USER_UPDATE_CONTACT;
				case RESET_PASSWORD -> USER_RESET_PASSWORD;
				default -> null;
			};
			case USER_GROUP -> switch (operation) {
				case CREATE -> USER_GROUP_CREATE;
				case UPDATE -> USER_GROUP_UPDATE;
				case DELETE -> USER_GROUP_DELETE;
				default -> null;
			};
			case ALARM_RECEIVER -> ALARM_RECEIVER_UPDATE;
			case ALARM -> operation == Operation.CLOSE ? ALARM_CLOSE : null;
			case ROLE -> switch (operation) {
				case CREATE -> ROLE_CREATE;
				case UPDATE, BATCH_UPDATE -> ROLE_UPDATE;
				case DELETE -> ROLE_DELETE;
				default -> null;
			};
			case ACCESS_CODE -> USER_RESET_ACCESS_CODE;
			case CUSTOMER -> CUSTOMER_UPDATE;
			case MCP -> API_AUTHENTICATE;
			default -> null;
		};
	}

	private static AuditAction task(Operation operation) {
		return switch (operation) {
			case CREATE -> TASK_CREATE;
			case UPDATE, BATCH_UPDATE -> TASK_UPDATE;
			case RENAME -> TASK_RENAME;
			case COPY -> TASK_COPY;
			case DELETE -> TASK_DELETE;
			case START, REBALANCE_START -> TASK_START;
			case STOP, PAUSE, REBALANCE_STOP -> TASK_STOP;
			case FORCE_STOP -> TASK_FORCE_STOP;
			case RESET, BATCH_RESET -> TASK_RESET;
			default -> null;
		};
	}

	/** 这些模块的 parameter1 是秘密值或无意义，不作为对象名。 */
	public static boolean parameterIsSecret(Modular modular) {
		return modular == Modular.ACCESS_CODE || modular == Modular.MCP;
	}
}
