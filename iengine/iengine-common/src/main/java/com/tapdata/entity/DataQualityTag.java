package com.tapdata.entity;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataQualityTag {

	public final static String SUB_COLUMN_NAME = "__tapd8";

	public final static String INVALID_RESULT = "invalid";

	public final static String RESULT_FIELD = "result";

	public final static String HITRULES_FIELD = "hitRules";
	public final static String PASSRULES_FIELD = "passRules";

	private String result;

	private List<HitRules> hitRules;

	private List<HitRules> passRules;

	public DataQualityTag() {

	}

	public DataQualityTag(String result) {
		this.result = result;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public List<HitRules> getHitRules() {
		return hitRules;
	}

	public void setHitRules(List<HitRules> hitRules) {
		this.hitRules = hitRules;
	}

	public List<HitRules> getPassRules() {
		return passRules;
	}

	public void setPassRules(List<HitRules> passRules) {
		this.passRules = passRules;
	}

	@Override
	public String toString() {
		return "DataQualityTag{" +
				"result='" + result + '\'' +
				", hitRules=" + hitRules +
				", passRules=" + passRules +
				'}';
	}

	public boolean isNotEmpty() {
		return StringUtils.isNoneBlank(result) &&
				(CollectionUtils.isNotEmpty(hitRules) ||
						CollectionUtils.isNotEmpty(passRules));
	}

	public static class HitRules {
		private String fieldName;

		private String rules;

		private Map<String, Object> keys;

		public HitRules() {

		}

		public String getFieldName() {
			return fieldName;
		}

		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}

		public Map<String, Object> getKeys() {
			return keys;
		}

		public void setKeys(Map<String, Object> keys) {
			this.keys = keys;
		}

		public String getRules() {
			return rules;
		}

		public void setRules(String rules) {
			this.rules = rules;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			HitRules hitRules = (HitRules) o;
			return Objects.equals(fieldName, hitRules.fieldName) &&
					Objects.equals(rules, hitRules.rules) &&
					Objects.equals(keys, hitRules.keys);
		}

		@Override
		public int hashCode() {
			return Objects.hash(fieldName, rules, keys);
		}
	}
}
