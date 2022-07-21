package io.tapdata.task;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.CronUtil;
import com.tapdata.entity.Job;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * Created by xj
 * 2020-03-18 16:10
 **/
@TaskType(type = "TIMING_SYNC")
public class TimingSyncTask implements Task {

	private TaskContext taskContext;
	private Logger logger = LogManager.getLogger(TimingSyncTask.class);

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();

		TaskResult taskResult = new TaskResult();
		taskResult.setPassResult();
		taskResult.setTaskResult("succeed");

		if (clientMongoOperator != null) {
			// 调度老的任务
//            scheduleJobs(clientMongoOperator);

			// 调度编排任务
			scheduleDataFlow(clientMongoOperator);

		} else {
			taskResult.setFailedResult("Mongo client is null");
		}
		callback.accept(taskResult);
	}


	private void scheduleDataFlow(ClientMongoOperator clientMongoOperator) {
		// isSchedule为true的任务 且 只有initial sync的任务需要运行
		Criteria isScheduledCriteria = where("setting.isSchedule").is(true)
				.and("setting.sync_type").is(ConnectorConstant.SYNC_TYPE_INITIAL_SYNC);

		//遍历得到所有状态不是draft/scheduled/running
		isScheduledCriteria.andOperator(
				where("status").ne(ConnectorConstant.DRAFT),
				where("status").ne(ConnectorConstant.SCHEDULED),
				where("status").ne(ConnectorConstant.RUNNING)
		);

		final Query scheduledQuery = new Query(isScheduledCriteria);
		scheduledQuery.fields()
				.include("id")
				.include("nextSyncTime")
				.include("setting")
				.include("name")
				.include("status");

		List<DataFlow> dataFlows = clientMongoOperator.find(
				scheduledQuery,
				ConnectorConstant.DATA_FLOW_COLLECTION,
				DataFlow.class);

		//遍历dataflows表
		for (DataFlow dataFlow : dataFlows) {
			Long nextSyncTime = dataFlow.getNextSyncTime();
			Long newNextSyncTime = CronUtil.getNextTriggerTime(dataFlow.getSetting().getCronExpression());
			// job的nextSyncTime为0，则为首次进入，更新nextSyncTime；
			// 如果计算出的下次执行时间小于已经存放的下次执行时间，同样要更新
			if (nextSyncTime.equals(0L) || newNextSyncTime < nextSyncTime) {
				//需要更新到中间库
				clientMongoOperator.update(new Query(where("_id").is(dataFlow.getId()))
						, new Update().set("nextSyncTime", newNextSyncTime)
						, ConnectorConstant.DATA_FLOW_COLLECTION);
				continue;
			}
			Date date = new Date();
			Long currDateTime = date.getTime();
			//查询当前时间是否达到了下次执行时间（nextSyncTime）,如果达到了,则开启任务,并更新nextSyncTime
			if (currDateTime > nextSyncTime) {

				// 删除子任务的offset
				clientMongoOperator.update(
						new Query(where("dataFlowId").is(dataFlow.getId())),
						new Update().unset("offset"), ConnectorConstant.JOB_COLLECTION
				);

				Query query = new Query(
						where("_id").is(dataFlow.getId())
								// 避免状态被其他端修改后，导致的执行错误
								.andOperator(isScheduledCriteria)
				);
				Update update = new Update();
				update.set("nextSyncTime", newNextSyncTime);
				update.set("status", ConnectorConstant.SCHEDULED);

//                DataFlow modifiedDataFlow = clientMongoOperator.findAndModify(query, update, DataFlow.class, ConnectorConstant.DATA_FLOW_COLLECTION);
				UpdateResult updateResult = clientMongoOperator.update(query, update, ConnectorConstant.DATA_FLOW_COLLECTION);
				if (updateResult.getModifiedCount() > 0) {
					logger.info("TIMING_SYNC task started, data flow '{}' is running", dataFlow.getName());
				}
			}
		}
	}

	private void scheduleJobs(ClientMongoOperator clientMongoOperator) {
		//todo：遍历得到所有状态不是draft，且isSchedule为true的任务
		Document statusFilter = new Document() {{
			put("$ne", "draft");
		}};
		Document timmingSyncRecord = new Document() {{
			put("status", statusFilter);
			put("isSchedule", true);
		}};

		List<Job> jobs = clientMongoOperator.find(timmingSyncRecord, ConnectorConstant.JOB_COLLECTION,
				Job.class);

		//todo：遍历job表
		for (Job job : jobs) {
			Long jobNextSyncTime = job.getNextSyncTime();
			Long newNextSyncTime = CronUtil.getNextTriggerTime(job.getCronExpression());
			// job的nextSyncTime为0，则为首次进入，更新nextSyncTime；
			// 如果计算出的下次执行时间小于已经存放的下次执行时间，同样要更新
			if (jobNextSyncTime.equals(0L) || newNextSyncTime < jobNextSyncTime) {
				//需要更新到中间库
				clientMongoOperator.update(new Query(where("_id").is(job.getId()))
						, new Update().set("nextSyncTime", newNextSyncTime)
						, ConnectorConstant.JOB_COLLECTION);
				continue;
			}
			Date date = new Date();
			Long currDateTime = date.getTime();
			//todo 查询当前时间是否达到了下次执行时间（nextSyncTime）,如果达到了,则开启任务,并更新nextSyncTime
			if (currDateTime > jobNextSyncTime) {

				Query query = new Query(where("_id").is(job.getId()));
				Update update = new Update().set("nextSyncTime", newNextSyncTime)
						.set("status", ConnectorConstant.SCHEDULED);
				clientMongoOperator.update(query, update, ConnectorConstant.JOB_COLLECTION);

				logger.info("TIMING_SYNC task started, job '{}' is running", job.getName());
			}
		}
	}

}
