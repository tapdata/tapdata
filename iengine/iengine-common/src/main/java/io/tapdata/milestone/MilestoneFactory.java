package io.tapdata.milestone;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2020-12-25 16:18
 **/
public class MilestoneFactory {

	public static MilestoneFlowService getMilestoneFlowService(DataFlow dataFlow, ClientMongoOperator clientMongoOperator) {
		if (null == dataFlow || null == clientMongoOperator) {
			throw new IllegalArgumentException("Get dataflow milestone service failed, dataflow or clientMongoOperator cannot be empty");
		}

		MilestoneContext milestoneContext = new MilestoneContext(dataFlow, clientMongoOperator, MilestoneContext.MilestoneType.DATAFLOW_V1);
		return new MilestoneFlowService(milestoneContext);
	}

	public static MilestoneJobService getJobMilestoneService(Job job, ClientMongoOperator clientMongoOperator) {
		if (null == job || null == clientMongoOperator) {
			throw new IllegalArgumentException("Get job milestone service failed, job or clientMongoOperator cannot be emtpy");
		}
		Connections sourceConn = job.getConn(true, clientMongoOperator);
		Connections targetConn = job.getConn(false, clientMongoOperator, sourceConn);
		MilestoneContext milestoneContext = new MilestoneContext(job, clientMongoOperator, sourceConn, targetConn, MilestoneContext.MilestoneType.JOB);
		return new MilestoneJobService(milestoneContext);
	}

	public static MilestoneFlowServiceJetV2 getJetMilestoneService(DataFlow dataFlow, List<String> baseURLs, int retryTime, ConfigurationCenter configurationCenter) {
		if (null == dataFlow) {
			throw new IllegalArgumentException("Get jet dataflow milestone service failed, dataflow or clientMongoOperator cannot be empty");
		}

		MilestoneContext milestoneContext = new MilestoneContext(dataFlow, MilestoneContext.MilestoneType.DATAFLOW_V2, baseURLs, retryTime, configurationCenter);
		return new MilestoneFlowServiceJetV2(milestoneContext);
	}

	public static MilestoneJetEdgeService getJetEdgeMilestoneService(DataFlow dataFlow, List<String> baseURLs, int retryTime, ConfigurationCenter configurationCenter, Stage sourceStage, Stage destStage) {
		if (null == dataFlow) {
			throw new IllegalArgumentException("Get jet dataflow milestone service failed, dataflow or clientMongoOperator cannot be empty");
		}

		MilestoneContext milestoneContext = new MilestoneContext(dataFlow, sourceStage, destStage, MilestoneContext.MilestoneType.DATAFLOW_EDGE,
				baseURLs, retryTime, configurationCenter);
		return new MilestoneJetEdgeService(milestoneContext, null);
	}

	public static MilestoneFlowServiceJetV2 getJetMilestoneService(TaskDto taskDto, List<String> baseURLs, int retryTime, ConfigurationCenter configurationCenter) {
		if (null == taskDto) {
			throw new IllegalArgumentException("Get jet subTaskDto milestone service failed, subTaskDto or clientMongoOperator cannot be empty");
		}

		MilestoneContext milestoneContext = new MilestoneContext(taskDto, MilestoneContext.MilestoneType.SUBTASK, baseURLs, retryTime, configurationCenter);
		return new MilestoneFlowServiceJetV2(milestoneContext);
	}

	public static MilestoneJetEdgeService getJetEdgeMilestoneService(
			TaskDto taskDto,
			List<String> baseURLs,
			int retryTime,
			ConfigurationCenter configurationCenter,
			Node sourceNode,
			Node destNode,
			String sourceVertexName,
			String destVertexName,
			MilestoneContext taskMilestoneContext
	) {
		if (null == taskDto) {
			throw new IllegalArgumentException("Get jet subTaskDto milestone service failed, subTaskDto or clientMongoOperator cannot be empty");
		}

		MilestoneContext milestoneContext = new MilestoneContext(taskDto, sourceNode, destNode, MilestoneContext.MilestoneType.SUBTASK_EDGE,
				baseURLs, retryTime, configurationCenter, sourceVertexName, destVertexName);
		return new MilestoneJetEdgeService(milestoneContext, taskMilestoneContext);
	}

	public static MilestoneJetEdgeService getJetEdgeMilestoneService(
			TaskDto taskDto,
			List<String> baseURLs,
			int retryTime,
			ConfigurationCenter configurationCenter,
			Node sourceNode,
			String sourceVertexName,
			List<String> destVertexNames,
			MilestoneContext taskMilestoneContext,
			MilestoneContext.VertexType vertexType
	) {
		if (null == taskDto) {
			throw new IllegalArgumentException("Get jet subTaskDto milestone service failed, subTaskDto or clientMongoOperator cannot be empty");
		}

		MilestoneContext milestoneContext = new MilestoneContext(taskDto, sourceNode, MilestoneContext.MilestoneType.SUBTASK_EDGE,
				baseURLs, retryTime, configurationCenter, sourceVertexName, destVertexNames, vertexType);
		return new MilestoneJetEdgeService(milestoneContext, taskMilestoneContext);
	}
}
