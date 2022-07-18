package com.tapdata.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TableIndex implements Serializable, Cloneable {

	private static final long serialVersionUID = 8643657627271087006L;

	private String indexName;

	private String indexType;

	private String indexSourceType;

	private boolean isUnique;
	private boolean isClustered;
	private boolean isPrimaryKey;

	private List<TableIndexColumn> columns;

	private String dbIndexDescriptionJson;

	public TableIndex() {
	}

	public TableIndex(String indexName, String indexType, String indexSourceType, boolean isUnique, List<TableIndexColumn> columns) {
		this.indexName = indexName;
		this.indexType = indexType;
		this.indexSourceType = indexSourceType;
		this.isUnique = isUnique;
		this.columns = columns;
	}

	public TableIndex(String indexName, String indexType, String indexSourceType, boolean isUnique, List<TableIndexColumn> columns, String dbIndexDescriptionJson) {
		this.indexName = indexName;
		this.indexType = indexType;
		this.indexSourceType = indexSourceType;
		this.isUnique = isUnique;
		this.columns = columns;
		this.dbIndexDescriptionJson = dbIndexDescriptionJson;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getIndexType() {
		return indexType;
	}

	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	public String getIndexSourceType() {
		return indexSourceType;
	}

	public void setIndexSourceType(String indexSourceType) {
		this.indexSourceType = indexSourceType;
	}

	public boolean isUnique() {
		return isUnique;
	}

	public void setUnique(boolean unique) {
		isUnique = unique;
	}

	public boolean isClustered() {
		return isClustered;
	}

	public void setClustered(boolean clustered) {
		isClustered = clustered;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setPrimaryKey(boolean primaryKey) {
		isPrimaryKey = primaryKey;
	}

	public List<TableIndexColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<TableIndexColumn> columns) {
		this.columns = columns;
	}

	public String getDbIndexDescriptionJson() {
		return dbIndexDescriptionJson;
	}

	public void setDbIndexDescriptionJson(String dbIndexDescriptionJson) {
		this.dbIndexDescriptionJson = dbIndexDescriptionJson;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		TableIndex ins = (TableIndex) super.clone();
		if (null != this.columns) {
			ins.columns = new ArrayList<>();
			for (TableIndexColumn tic : this.columns) {
				ins.columns.add((TableIndexColumn) tic.clone());
			}
		}
		return ins;
	}

	@Override
	public String toString() {
		return "TableIndex{" +
				"indexName='" + indexName + '\'' +
				", indexType='" + indexType + '\'' +
				", indexSourceType='" + indexSourceType + '\'' +
				", isUnique=" + isUnique +
				", isClustered=" + isClustered +
				", isPrimaryKey=" + isPrimaryKey +
				", columns=" + columns +
				", dbIndexDescriptionJson='" + dbIndexDescriptionJson + '\'' +
				'}';
	}
}
