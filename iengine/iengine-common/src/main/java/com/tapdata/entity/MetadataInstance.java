package com.tapdata.entity;

import java.util.List;
import java.util.Map;

public class MetadataInstance {

	private String id;

	private String qualified_name;

	private String meta_type;

	private String original_name;

	private Map<String, Object> data_rules;

	private List<Map<String, Object>> indexes;

	private List<RelateDatabaseField> fields;

	private Map<String, Object> source;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getQualified_name() {
		return qualified_name;
	}

	public void setQualified_name(String qualified_name) {
		this.qualified_name = qualified_name;
	}

	public String getMeta_type() {
		return meta_type;
	}

	public void setMeta_type(String meta_type) {
		this.meta_type = meta_type;
	}

	public String getOriginal_name() {
		return original_name;
	}

	public void setOriginal_name(String original_name) {
		this.original_name = original_name;
	}

	public Map<String, Object> getData_rules() {
		return data_rules;
	}

	public void setData_rules(Map<String, Object> data_rules) {
		this.data_rules = data_rules;
	}

	public List<Map<String, Object>> getIndexes() {
		return indexes;
	}

	public void setIndexes(List<Map<String, Object>> indexes) {
		this.indexes = indexes;
	}

	public List<RelateDatabaseField> getFields() {
		return fields;
	}

	public void setFields(List<RelateDatabaseField> fields) {
		this.fields = fields;
	}

	public Map<String, Object> getSource() {
		return source;
	}

	public void setSource(Map<String, Object> source) {
		this.source = source;
	}
}
