package com.tapdata.constant;

import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.debug.DebugConstant;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class JobUtil {
	private static Logger logger = LogManager.getLogger(JobUtil.class);

	public static List<JavaScriptFunctions> getJavaScriptFunctions(ClientMongoOperator clientMongoOperator) {
		if (clientMongoOperator == null) return new ArrayList<>();

		return clientMongoOperator.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))), ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);
	}

	public static void setThreadContext(Job job) {
		ThreadContext.clearAll();
		ThreadContext.put("userId", job.getUser_id());
		ThreadContext.put("jobId", job.getId());
		ThreadContext.put("jobName", job.getName());
		ThreadContext.put("app", ConnectorConstant.APP_TRANSFORMER);
		if (StringUtils.isNotBlank(job.getDataFlowId())) {
			ThreadContext.put(DebugConstant.SUB_DATAFLOW_ID, job.getDataFlowId());
		}
	}


	/**
	 * 重置包含聚合节点的任务的offset
	 *
	 * @param job
	 */
	public static void resetJobOffset(Job job, ClientMongoOperator clientMongoOperator) {
		if (job == null) {
			return;
		}

		final List<Stage> stages = job.getStages();
		if (CollectionUtils.isNotEmpty(stages)) {
			for (Stage stage : stages) {
				if (Stage.StageTypeEnum.fromString(stage.getType()) == Stage.StageTypeEnum.AGGREGATION_PROCESSOR) {
					clientMongoOperator.update(
							new Query(where("_id").is(job.getId())),
							new Update().set("offset", null),
							ConnectorConstant.JOB_COLLECTION
					);
					job.setOffset(null);
					return;
				}
			}
		}
	}
}
