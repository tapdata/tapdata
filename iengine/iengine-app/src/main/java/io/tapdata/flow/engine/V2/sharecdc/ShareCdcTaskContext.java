package io.tapdata.flow.engine.V2.sharecdc;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.SubTaskDto;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 15:29
 **/
public class ShareCdcTaskContext extends ShareCdcContext implements Serializable {

	private static final long serialVersionUID = 6587811104432940573L;

	private SubTaskDto subTaskDto;
	private Node node;
	private Connections connections;

	public ShareCdcTaskContext(Long cdcStartTs, ConfigurationCenter configurationCenter, SubTaskDto subTaskDto, Node node, Connections connections) {
		super(cdcStartTs, configurationCenter);
		this.subTaskDto = subTaskDto;
		this.node = node;
		this.connections = connections;
	}

	public SubTaskDto getSubTaskDto() {
		return subTaskDto;
	}

	public Connections getConnections() {
		return connections;
	}

	public Node getNode() {
		return node;
	}
}
