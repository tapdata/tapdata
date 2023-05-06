package com.tapdata.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.constant.FileProperty;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by tapdata on 16/11/2017.
 */
public class RelateDataBaseTable implements Serializable {

	private Set<String> sourceTypes;

	private String table_name;

	private List<RelateDatabaseField> fields;

	private Boolean cdc_enabled;

	private Map<String, Object> meta_data;

	private String tableId;

	private String type;

	private List<TableIndex> indices;

	private boolean isLast = false;

	private String schemaVersion;

	private FileProperty fileProperty;

	/**
	 * kafka队列的分区列表
	 */
	private Set<Integer> partitionSet;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String comment;

	public Set<String> getSourceTypes() {
		return sourceTypes;
	}

	public void setSourceTypes(Set<String> sourceTypes) {
		this.sourceTypes = sourceTypes;
	}

	public void addSourceType(String... sourceTypes) {
		if (null != sourceTypes) {
			if (null == this.sourceTypes) {
				this.sourceTypes = new LinkedHashSet<>();
			}
			this.sourceTypes.addAll(Arrays.asList(sourceTypes));
		}
	}

	public boolean containsSourceType(String sourceType) {
		if (null != this.sourceTypes) {
			return this.sourceTypes.contains(sourceType);
		}
		return true;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public List<RelateDatabaseField> getFields() {
		return fields;
	}

	public void setFields(List<RelateDatabaseField> fields) {
		this.fields = fields;
	}

	public RelateDataBaseTable() {
	}

	public RelateDataBaseTable(String table_name) {
		this.table_name = table_name;
	}

	public RelateDataBaseTable(String table_name, String type) {
		this.table_name = table_name;
		this.type = type;
	}

	public RelateDataBaseTable(String table_name, String type, String comment) {
		this.table_name = table_name;
		this.type = type;
		this.comment = comment;
	}

	public RelateDataBaseTable(boolean isLast) {
		this.isLast = isLast;
	}

	public Boolean getCdc_enabled() {
		return cdc_enabled;
	}

	public void setCdc_enabled(Boolean cdc_enabled) {
		this.cdc_enabled = cdc_enabled;
	}

	public void cdc_enabled(Set<String> tables) {
		if (CollectionUtils.isNotEmpty(tables) && StringUtils.isNotBlank(this.table_name) && tables.contains(this.table_name)) {
			this.cdc_enabled = true;
		} else {
			this.cdc_enabled = false;
		}
	}

	public Map<String, Object> getMeta_data() {
		return meta_data;
	}

	public void setMeta_data(Map<String, Object> meta_data) {
		this.meta_data = meta_data;
	}

	public String getTableId() {
		return tableId;
	}

	public void setTableId(String tableId) {
		this.tableId = tableId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<TableIndex> getIndices() {
		return indices;
	}

	public void setIndices(List<TableIndex> indices) {
		this.indices = indices;
	}

	public boolean isLast() {
		return isLast;
	}

	public void setLast(boolean last) {
		isLast = last;
	}

	public String getSchemaVersion() {
		return schemaVersion;
	}

	public void setSchemaVersion(String schemaVersion) {
		this.schemaVersion = schemaVersion;
	}

	public FileProperty getFileProperty() {
		return fileProperty;
	}

	public void setFileProperty(FileProperty fileProperty) {
		this.fileProperty = fileProperty;
	}

	public Set<Integer> getPartitionSet() {
		return partitionSet;
	}

	public void setPartitionSet(Set<Integer> partitionSet) {
		this.partitionSet = partitionSet;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public boolean hasPrimaryKey() {
		if (CollectionUtils.isEmpty(fields)) {
			return false;
		}
		return fields.stream().filter(field -> field.getPrimary_key_position() > 0).findFirst().orElse(null) != null;
	}

	public List<RelateDatabaseField> getAllPrimaryKeys() {
		if (CollectionUtils.isEmpty(fields)) {
			return new ArrayList<>();
		}
		return fields.stream().filter(field -> field.getPrimary_key_position() > 0)
				.sorted(Comparator.comparingInt(RelateDatabaseField::getColumnPosition))
				.collect(Collectors.toList());
	}

	@Override
	public String toString() {
		return "RelateDataBaseTable{" +
				"table_name='" + table_name + '\'' +
				", fields=" + fields +
				", cdc_enabled=" + cdc_enabled +
				'}';
	}
}
