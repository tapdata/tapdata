package io.tapdata.milestone;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2020-12-23 19:50
 **/
public class MilestoneContext implements Serializable {

	private static final long serialVersionUID = -2618557274297812561L;
	private Job job;
	private DataFlow dataFlow;
	private ClientMongoOperator clientMongoOperator;
	private Connections sourceConn;
	private Connections targetConn;
	private Stage sourceStage;
	private Stage destStage;
	private MilestoneType milestoneType;
	private List<String> baseUrls;
	private int retryTime;
	private ConfigurationCenter configurationCenter;
	private TaskDto taskDto;
	private Node sourceNode;
	private Node destNode;
	private Map<String, EdgeMilestone> edgeMilestones;
	private String sourceVertexName;
	private String destVertexName;
	private List<String> sourceVertexNames;
	private List<String> destVertexNames;

	public MilestoneContext(Job job, ClientMongoOperator clientMongoOperator, Connections sourceConn, Connections targetConn, MilestoneType milestoneType) {
		if (null == job || null == clientMongoOperator || null == sourceConn || null == targetConn) {
			throw new IllegalArgumentException("Input parameters is invalid");
		}
		this.job = job;
		this.clientMongoOperator = clientMongoOperator;
		this.sourceConn = sourceConn;
		this.targetConn = targetConn;
		this.milestoneType = milestoneType;
	}

	public MilestoneContext(DataFlow dataFlow, ClientMongoOperator clientMongoOperator, MilestoneType milestoneType) {
		this.dataFlow = dataFlow;
		this.clientMongoOperator = clientMongoOperator;
		this.milestoneType = milestoneType;
	}

	public MilestoneContext(DataFlow dataFlow, MilestoneType milestoneType, List<String> baseUrls, int retryTime, ConfigurationCenter configurationCenter) {
		this.dataFlow = dataFlow;
		this.milestoneType = milestoneType;
		this.baseUrls = baseUrls;
		this.retryTime = retryTime;
		this.configurationCenter = configurationCenter;
	}

	public MilestoneContext(DataFlow dataFlow, ClientMongoOperator clientMongoOperator, Stage sourceStage, Stage destStage, MilestoneType milestoneType) {
		this.dataFlow = dataFlow;
		this.clientMongoOperator = clientMongoOperator;
		this.sourceStage = sourceStage;
		this.destStage = destStage;
		this.milestoneType = milestoneType;
	}

	public MilestoneContext(DataFlow dataFlow, Stage sourceStage, Stage destStage, MilestoneType milestoneType, List<String> baseUrls, int retryTime, ConfigurationCenter configurationCenter) {
		this.dataFlow = dataFlow;
		this.sourceStage = sourceStage;
		this.destStage = destStage;
		this.milestoneType = milestoneType;
		this.baseUrls = baseUrls;
		this.retryTime = retryTime;
		this.configurationCenter = configurationCenter;
	}

	public MilestoneContext(TaskDto taskDto, Node sourceNode, Node destNode, MilestoneType milestoneType, List<String> baseUrls, int retryTime, ConfigurationCenter configurationCenter, String sourceVertexName, String destVertexName) {
		this.taskDto = taskDto;
		this.sourceNode = sourceNode;
		this.destNode = destNode;
		this.milestoneType = milestoneType;
		this.baseUrls = baseUrls;
		this.retryTime = retryTime;
		this.configurationCenter = configurationCenter;
		this.sourceVertexName = sourceVertexName;
		this.destVertexName = destVertexName;
	}

	public MilestoneContext(TaskDto taskDto, Node node, MilestoneType milestoneType, List<String> baseUrls, int retryTime, ConfigurationCenter configurationCenter, String vertexName, List<String> vertexNames, VertexType type) {
		this.taskDto = taskDto;
		this.milestoneType = milestoneType;
		this.baseUrls = baseUrls;
		this.retryTime = retryTime;
		this.configurationCenter = configurationCenter;
		switch (type) {
			case SOURCE:
				this.sourceNode = node;
				this.sourceVertexName = vertexName;
				this.destVertexNames = vertexNames;
				break;
			case DEST:
				this.destNode = node;
				this.destVertexName = vertexName;
				this.sourceVertexNames = vertexNames;
				break;
		}
	}

	public MilestoneContext(TaskDto taskDto, MilestoneType milestoneType, List<String> baseUrls, int retryTime, ConfigurationCenter configurationCenter) {
		this.taskDto = taskDto;
		this.milestoneType = milestoneType;
		this.baseUrls = baseUrls;
		this.retryTime = retryTime;
		this.configurationCenter = configurationCenter;
	}

	public Job getJob() {
		return job;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}

	public Connections getSourceConn() {
		return sourceConn;
	}

	public Connections getTargetConn() {
		return targetConn;
	}

	public DataFlow getDataFlow() {
		return dataFlow;
	}

	public MilestoneType getMilestoneType() {
		return milestoneType;
	}

	public Stage getSourceStage() {
		return sourceStage;
	}

	public Stage getDestStage() {
		return destStage;
	}

	public void setSourceConn(Connections sourceConn) {
		this.sourceConn = sourceConn;
	}

	public void setTargetConn(Connections targetConn) {
		this.targetConn = targetConn;
	}

	public List<String> getBaseUrls() {
		return baseUrls;
	}

	public int getRetryTime() {
		return retryTime;
	}

	public ConfigurationCenter getConfigurationCenter() {
		return configurationCenter;
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}

	public void setTaskDto(TaskDto taskDto) {
		this.taskDto = taskDto;
	}

	public Node getSourceNode() {
		return sourceNode;
	}

	public void setSourceNode(Node sourceNode) {
		this.sourceNode = sourceNode;
	}

	public Node getDestNode() {
		return destNode;
	}

	public void setDestNode(Node destNode) {
		this.destNode = destNode;
	}

	public Map<String, EdgeMilestone> getEdgeMilestones() {
		return edgeMilestones;
	}

	public void setEdgeMilestones(Map<String, EdgeMilestone> edgeMilestones) {
		this.edgeMilestones = edgeMilestones;
	}

	public String getSourceVertexName() {
		return sourceVertexName;
	}

	public void setSourceVertexName(String sourceVertexName) {
		this.sourceVertexName = sourceVertexName;
	}

	public String getDestVertexName() {
		return destVertexName;
	}

	public void setDestVertexName(String destVertexName) {
		this.destVertexName = destVertexName;
	}

	public List<String> getDestVertexNames() {
		return destVertexNames;
	}

	public List<String> getSourceVertexNames() {
		return sourceVertexNames;
	}

	public enum MilestoneType {
		JOB,
		DATAFLOW_V1,
		DATAFLOW_V2,
		DATAFLOW_EDGE,
		SUBTASK,
		SUBTASK_EDGE,
		;
	}

	public enum VertexType {
		SOURCE,
		DEST,
		;
	}
}
