package com.tapdata.entity;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Map;

public class FieldRule implements Serializable {

	public static final String RULEID = "ruleId";
	private static final long serialVersionUID = 3110706952353299844L;

	private String fieldName;

	private Map<String, Object> rule;

	public FieldRule() {
	}

	public FieldRule(String fieldName, Map<String, Object> rule) {
		this.fieldName = fieldName;
		this.rule = rule;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public Map<String, Object> getRule() {
		return rule;
	}

	public void setRule(Map<String, Object> rule) {
		this.rule = rule;
	}

	public boolean isNotEmpty() {
		return StringUtils.isNoneBlank(fieldName) &&
				MapUtils.isNotEmpty(rule);
	}

	public class RuleDetail {
		private String ruleId;

		public RuleDetail(String ruleId) {
			this.ruleId = ruleId;
		}

		public String getRuleId() {
			return ruleId;
		}

		public void setRuleId(String ruleId) {
			this.ruleId = ruleId;
		}

		public boolean isNotEmpty() {
			return StringUtils.isNoneBlank(ruleId);
		}
	}
}
