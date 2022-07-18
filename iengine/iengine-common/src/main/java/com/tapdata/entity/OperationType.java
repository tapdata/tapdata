package com.tapdata.entity;

import com.tapdata.entity.dataflow.SyncObjects;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description 数据操作枚举类
 * @create 2020-09-08 19:06
 **/
public enum OperationType {

	INSERT("i", "dml", true),
	ABSOLUTE_INSERT("abi", "dml", true),
	UPDATE("u", "dml", true),
	DELETE("d", "dml", true),
	DDL("ddl", "ddl", true),
	CREATE_TABLE("create_table", "ddl", true),
	CREATE_VIEW("create_view", "ddl", true),
	CREATE_FUNCTION("create_function", "ddl", true),
	CREATE_PROCEDURE("create_procedure", "ddl", true),
	CREATE_INDEX("create_index", "ddl", true),
	COMMIT_OFFSET("commit_offset", "", false),
	END_DDL("end_ddl", "notify", true),
	JOB_ERROR("job_error", "", false),
	RETRY("retry", "", false),
	SWITCH_CUSTOM_SQL_CDC("switch_custom_sql_cdc", "", false),
	;

	private String op;
	private String type;
	private boolean processable;

	OperationType(String op, String type, boolean processable) {
		this.op = op;
		this.type = type;
		this.processable = processable;
	}

	public String getOp() {
		return op;
	}

	public String getType() {
		return type;
	}

	public boolean isProcessable() {
		return processable;
	}

	private static Map<String, OperationType> opMap = new HashMap<>();

	static {
		for (OperationType operationType : OperationType.values()) {
			opMap.put(operationType.getOp(), operationType);
		}
	}

	public static OperationType fromOp(String op) {
		return opMap.get(op);
	}

	public static boolean isDml(String op) {
		if (!opMap.containsKey(op)) {
			return false;
		}

		return isDml(opMap.get(op));
	}

	public static boolean isDml(OperationType operationType) {
		return operationType.getType().equals("dml");
	}

	public static boolean isDdl(String op) {
		if (!opMap.containsKey(op)) {
			return false;
		}

		return isDdl(opMap.get(op));
	}

	public static boolean isNotify(String op) {
		if (!opMap.containsKey(op)) {
			return false;
		}

		return isNotify(opMap.get(op));
	}

	public static boolean isDdl(OperationType operationType) {
		return operationType.getType().equals("ddl");
	}

	public static boolean isNotify(OperationType operationType) {
		return operationType.getType().equals("notify");
	}

	public static OperationType fromSyncObjectType(String type) {
		switch (type) {
			case SyncObjects.TABLE_TYPE:
				return CREATE_TABLE;
			case SyncObjects.VIEW_TYPE:
				return CREATE_VIEW;
			case SyncObjects.FUNCTION_TYPE:
				return CREATE_FUNCTION;
			case SyncObjects.PROCEDURE_TYPE:
				return CREATE_PROCEDURE;
			case SyncObjects.INDEX_TYPE:
				return CREATE_INDEX;
			default:
				return null;
		}
	}
}
