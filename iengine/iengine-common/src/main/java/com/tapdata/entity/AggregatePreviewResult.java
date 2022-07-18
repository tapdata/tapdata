package com.tapdata.entity;

import org.bson.Document;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2020-10-30 00:05
 **/
public class AggregatePreviewResult implements Serializable {

	private static final long serialVersionUID = 8376929705722593581L;

	private List<Document> previewResult;

	private RelateDataBaseTable relateDataBaseTable;

	private Connections connections;

	private String tableName;

	public AggregatePreviewResult(List<Document> previewResult, RelateDataBaseTable relateDataBaseTable, Connections connections, String tableName) {
		this.previewResult = previewResult;
		this.relateDataBaseTable = relateDataBaseTable;
		this.connections = connections;
		this.tableName = tableName;
	}

	public List<Document> getPreviewResult() {
		return previewResult;
	}

	public RelateDataBaseTable getRelateDataBaseTable() {
		return relateDataBaseTable;
	}

	public Connections getConnections() {
		return connections;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public String toString() {
		return "AggregatePreviewResult{" +
				"previewResult=" + previewResult +
				", relateDataBaseTable=" + relateDataBaseTable +
				", connections=" + connections +
				", tableName='" + tableName + '\'' +
				'}';
	}
}
