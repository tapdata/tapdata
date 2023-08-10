package io.tapdata.flow.engine.V2.sharecdc;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.io.Serializable;
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

	public ShareCdcTaskContext(Long cdcStartTs, ConfigurationCenter configurationCenter, TaskDto taskDto, Node node, Connections connections, List<String> tables) {
		super(cdcStartTs, configurationCenter, tables);
		this.taskDto = taskDto;
		this.node = node;
		this.connections = connections;
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
}
