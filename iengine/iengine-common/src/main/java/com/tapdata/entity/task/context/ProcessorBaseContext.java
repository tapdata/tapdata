package com.tapdata.entity.task.context;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.schema.TapTableMap;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-03-17 13:30
 **/
public class ProcessorBaseContext implements Serializable {

	private static final long serialVersionUID = -8383020637450262788L;
	private final TaskDto taskDto;
	private Node<?> node;
	private List<Node> nodes;
	private List<Edge> edges;
	private final ConfigurationCenter configurationCenter;
	private final List<RelateDataBaseTable> nodeSchemas;
	private final TapTableMap<String, TapTable> tapTableMap;
	private final TaskConfig taskConfig;

	private String pdkAssociateId;

	protected <T extends ProcessorBaseContextBuilder<T>> ProcessorBaseContext(ProcessorBaseContextBuilder<T> builder) {
		taskDto = builder.taskDto;
		node = builder.node;
		nodes = builder.nodes;
		edges = builder.edges;
		configurationCenter = builder.configurationCenter;
		nodeSchemas = builder.nodeSchemas;
		tapTableMap = builder.tapTableMap;
		taskConfig = builder.taskConfig;
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}

	public Node<?> getNode() {
		return node;
	}

	public void setNode(Node<?> node) {
		this.node = node;
	}

	public List<Edge> getEdges() {
		return edges;
	}

	public ConfigurationCenter getConfigurationCenter() {
		return configurationCenter;
	}

	public List<RelateDataBaseTable> getNodeSchemas() {
		return nodeSchemas;
	}

	public TapTableMap<String, TapTable> getTapTableMap() {
		return tapTableMap;
	}

	public List<Node> getNodes() {
		return nodes;
	}

	public void setNodes(List<Node> nodes) {
		this.nodes = nodes;
	}

	public void setEdges(List<Edge> edges) {
		this.edges = edges;
	}

	public static class ProcessorBaseContextBuilder<T extends ProcessorBaseContextBuilder<T>> {
		private TaskDto taskDto;
		private Node<?> node;
		private List<Node> nodes;
		private List<Edge> edges;
		private ConfigurationCenter configurationCenter;
		private List<RelateDataBaseTable> nodeSchemas;
		private TapTableMap<String, TapTable> tapTableMap;
		private TaskConfig taskConfig;

		public T withTaskDto(TaskDto taskDto) {
			this.taskDto = taskDto;
			return (T) this;
		}

		public T withNode(Node<?> node) {
			this.node = node;
			return (T) this;
		}

		public T withNodes(List<Node> nodes) {
			this.nodes = nodes;
			return (T) this;
		}

		public T withEdges(List<Edge> edges) {
			this.edges = edges;
			return (T) this;
		}

		public T withConfigurationCenter(ConfigurationCenter configurationCenter) {
			this.configurationCenter = configurationCenter;
			return (T) this;
		}

		public T withNodeSchemas(List<RelateDataBaseTable> nodeSchemas) {
			this.nodeSchemas = nodeSchemas;
			return (T) this;
		}

		public T withTapTableMap(TapTableMap<String, TapTable> tapTableMap) {
			this.tapTableMap = tapTableMap;
			return (T) this;
		}

		public T withTaskConfig(TaskConfig taskConfig) {
			this.taskConfig = taskConfig;
			return (T) this;
		}

		public ProcessorBaseContext build() {
			return new ProcessorBaseContext(this);
		}
	}

	public static ProcessorBaseContextBuilder<?> newBuilder() {
		return new ProcessorBaseContextBuilder<>();
	}

	public String getPdkAssociateId() {
		return pdkAssociateId;
	}

	public void setPdkAssociateId(String pdkAssociateId) {
		this.pdkAssociateId = pdkAssociateId;
	}

	public TaskConfig getTaskConfig() {
		return taskConfig;
	}
}
