package com.tapdata.entity;

import java.util.HashMap;
import java.util.Map;

public class DataRules {


	private String fieldName;

	private String name;

//    private Map<String, Object> rules;

	private String rules;

	private int status;

	private String user_id;

	public DataRules() {

	}

	public DataRules(String fieldName, String name, String rules, int status, String user_id) {
		this.fieldName = fieldName;
		this.name = name;
		this.rules = rules;
		this.status = status;
		this.user_id = user_id;
	}


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRules() {
		return rules;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public void setRules(String rules) {
		this.rules = rules;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	@Override
	public String toString() {
		return "DataRules{" +
				", fieldName='" + fieldName + '\'' +
				", name='" + name + '\'' +
				", rules='" + rules + '\'' +
				", status=" + status +
				", user_id='" + user_id + '\'' +
				'}';
	}

	public enum RuleType {
		EXISTS("exists"),
		NULLABLE("nullable"),
		TYPE("type"),
		RANGE("range"),
		ENUM("enum"),
		REGEX("regex"),
		;

		private String rule;

		RuleType(String rule) {
			this.rule = rule;
		}

		public String getRule() {
			return rule;
		}

		private static Map<String, RuleType> map = new HashMap<>();

		static {
			for (RuleType ruleType : RuleType.values()) {
				map.put(ruleType.getRule(), ruleType);
			}
		}

		public static RuleType fromString(String rule) {
			return map.get(rule);
		}
	}
}
