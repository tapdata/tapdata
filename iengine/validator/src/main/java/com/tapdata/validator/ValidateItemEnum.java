package com.tapdata.validator;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.TestConnectionItemConstant;
import com.tapdata.entity.DatabaseTypeEnum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum ValidateItemEnum {

	// redis validate item
	REDIS_HOST_PORT(DatabaseTypeEnum.REDIS.getType(), ValidatorConstant.VALIDATE_CODE_REDIS_HOST_IP, TestConnectionItemConstant.CHECK_CONNECT, true, 1, ConnectorConstant.CONNECTION_TYPE_TARGET),
	REDIS_DB_INDEX(DatabaseTypeEnum.REDIS.getType(), ValidatorConstant.VALIDATE_CODE_REDIS_DB_INDEX, TestConnectionItemConstant.CHECK_AUTH, true, 2, ConnectorConstant.CONNECTION_TYPE_TARGET),

	MONGO_HOST_PORT(DatabaseTypeEnum.MONGODB.getType(), ValidatorConstant.VALIDATE_CODE_MONGODB_HOST_IP, TestConnectionItemConstant.CHECK_CONNECT, true, 1, ConnectorConstant.CONNECTION_TYPE_SOURCE_TARGET),
	MONGO_USERNAME_PASSWORD(DatabaseTypeEnum.MONGODB.getType(), ValidatorConstant.VALIDATE_CODE_MONGODB_USERNAME_PASSWORD, TestConnectionItemConstant.CHECK_AUTH, true, 2, ConnectorConstant.CONNECTION_TYPE_SOURCE_TARGET),
	MONGO_PRIVILEGES(DatabaseTypeEnum.MONGODB.getType(), ValidatorConstant.VALIDATE_CODE_MONGODB_PRIVILEGES, TestConnectionItemConstant.CHECK_PERMISSION, false, 3, ConnectorConstant.CONNECTION_TYPE_SOURCE_TARGET),
	MONGO_DATABASE_CDC(DatabaseTypeEnum.MONGODB.getType(), ValidatorConstant.VALIDATE_CODE_MONGODB_CDC, TestConnectionItemConstant.CHECK_CDC_PERMISSION, false, 4, ConnectorConstant.CONNECTION_TYPE_SOURCE),
	MONGO_DATABASE_SCHEMA(DatabaseTypeEnum.MONGODB.getType(), ValidatorConstant.VALIDATE_CODE_MONGODB_DATABASE_SCHEMA, TestConnectionItemConstant.LOAD_SCHEMA, false, 5, ConnectorConstant.CONNECTION_TYPE_SOURCE_TARGET),
	;

	private String type;
	private String stage_code;
	private String show_msg;
	private boolean required;
	private int sort;
	private String connectionType;

	ValidateItemEnum(String type, String stage_code, String show_msg, boolean required, int sort, String connectionType) {
		this.type = type;
		this.stage_code = stage_code;
		this.show_msg = show_msg;
		this.required = required;
		this.sort = sort;
		this.connectionType = connectionType;
	}

	public String getType() {
		return type;
	}

	private static Map<String, List<ValidateItemEnum>> map = new HashMap<>();

	static {
		for (ValidateItemEnum validateItemEnum : ValidateItemEnum.values()) {
			String connectionType = validateItemEnum.getType();
			if (!map.containsKey(connectionType)) {
				map.put(connectionType, new ArrayList<>());
			}
			List<ValidateItemEnum> itemEnumList = map.get(connectionType);
			itemEnumList.add(validateItemEnum);
		}
	}

	public static List<ValidateItemEnum> fromConnectionType(String connectionType) {
		return map.get(connectionType);
	}

	public String getStage_code() {
		return stage_code;
	}

	public String getShow_msg() {
		return show_msg;
	}

	public boolean getRequired() {
		return required;
	}

	public int getSort() {
		return sort;
	}

	public String getConnectionType() {
		return connectionType;
	}
}
