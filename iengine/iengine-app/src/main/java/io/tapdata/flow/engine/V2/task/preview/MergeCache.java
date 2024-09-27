package io.tapdata.flow.engine.V2.task.preview;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-09-27 17:34
 **/
public class MergeCache {
	private MergeTableProperties mergeTableProperties;
	private List<Map<String, Object>> data;
	private String tableName;
	private Node node;

	public static MergeCache create(MergeTableProperties mergeTableProperties) {
		MergeCache mergeCache = new MergeCache();
		mergeCache.mergeTableProperties = mergeTableProperties;
		mergeCache.data = new ArrayList<>();
		return mergeCache;
	}

	public MergeCache node(Node node) {
		this.node = node;
		return this;
	}

	public void data(Map<String, Object> data) {
		this.data.add(data);
	}

	public void tableName(String tableName) {
		this.tableName = tableName;
	}

	public MergeTableProperties getMergeTableProperties() {
		return mergeTableProperties;
	}

	public List<Map<String, Object>> getData() {
		return data;
	}

	public String getTableName() {
		return tableName;
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}
}
