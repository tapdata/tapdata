package io.tapdata.flow.engine.V2.sharecdc;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 15:29
 **/
public class ShareCdcTaskContext extends ShareCdcContext implements Serializable {

	private static final long serialVersionUID = 6587811104432940573L;

	private TaskDto taskDto;
	private Node node;
	private Connections connections;
	private List<String> tableNames;

	public ShareCdcTaskContext(Long cdcStartTs, ConfigurationCenter configurationCenter, TaskDto taskDto, Node node, Connections connections) {
		this(cdcStartTs, configurationCenter, taskDto, node, connections, Collections.emptyList());
	}

	public ShareCdcTaskContext(Long cdcStartTs, ConfigurationCenter configurationCenter, TaskDto taskDto, Node node,
							   Connections connections, List<String> tableNames) {
		super(cdcStartTs, configurationCenter);
		this.taskDto = taskDto;
		this.node = node;
		this.connections = connections;
		this.tableNames = tableNames == null
				? Collections.emptyList()
				: Collections.unmodifiableList(new ArrayList<>(tableNames));
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}

	public Connections getConnections() {
		return connections;
	}

	public Node getNode() {
		return node;
	}

	public List<String> getTableNames() {
		return tableNames;
	}
}
