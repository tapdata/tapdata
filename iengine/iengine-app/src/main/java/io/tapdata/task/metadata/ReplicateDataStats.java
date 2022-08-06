package io.tapdata.task.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.task.Task;
import io.tapdata.task.TaskContext;
import io.tapdata.task.TaskResult;
import io.tapdata.task.TaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

@TaskType(type = "REPLICATE_DATA_STATS")
public class ReplicateDataStats implements Task {

	private TaskContext taskContext;
	private Logger logger = LogManager.getLogger(ReplicateDataStats.class);

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
			List<Job> jobs = clientMongoOperator.find(new Document("status", new Document("$ne", "draft")),
					ConnectorConstant.JOB_COLLECTION,
					Job.class);

			Document replicateDataStatsRecord = new Document() {{
				put("stats_granularity", "minute");
				put("stats_name", "replicate_data_stats");
				put("stats_time", "");
				put("data", new Document() {{
					put("total_data_size", 0l);
					put("total_file_size", 0l);
				}});
				put("update_time", new Date());
			}};

			replicateDataStatsRecord.put("stats_time",
					new SimpleDateFormat("yyyyMMddHHmm00")
							.format(replicateDataStatsRecord.getDate("update_time")));

			for (Job job : jobs) {
				String targetConnId = job.getConnections().getTarget();
				Document data = (Document) replicateDataStatsRecord.get("data");
				Connections connection;

				List<Connections> connections;
				try {
					Query query = new Query(new Criteria("_id").is(targetConnId));
					query.fields().exclude("schema");
					connections = clientMongoOperator.find(
							query,
							ConnectorConstant.CONNECTION_COLLECTION,
							Connections.class
					);

					if (CollectionUtils.isNotEmpty(connections)) {
						connection = connections.get(0);
					} else {
						continue;
					}


					if (connection != null
							&& job.getStats() != null
							&& MapUtils.isNotEmpty(job.getStats().getTotal())
							&& job.getStats().getTotal().get("total_data_size") != null) {

						if (connection.getDatabase_type().equalsIgnoreCase("mongodb")) {
							data.put("total_data_size",
									(Long) data.get("total_data_size") + job.getStats().getTotal().get("total_data_size"));
						} else if (connection.getDatabase_type().equalsIgnoreCase("gridfs")) {
							data.put("total_file_size",
									(Long) data.get("total_file_size") + job.getStats().getTotal().get("total_data_size"));
						}
					}
				} catch (Exception e) {
					logger.error("replicat data error: {}", e.getMessage(), e);
					continue;
				}
			}

			try {
				clientMongoOperator.getMongoTemplate().upsert(new Query(
						new Criteria().andOperator(
								Criteria.where("stats_name").is(replicateDataStatsRecord.get("stats_name")),
								Criteria.where("stats_time").is(replicateDataStatsRecord.get("stats_time"))
						)
				), getUpdate(replicateDataStatsRecord), "Insights");
			} catch (Exception e) {
				taskResult.setFailedResult("update data to collection Insights error: " + e.getMessage());
				logger.error("update data to collection Insights error: {}", e.getMessage(), e);
			}

			try {
				logger.info("REPLICATE_DATA_STATS task finished, data: \n{}",
						JSONUtil.map2JsonPretty(replicateDataStatsRecord));
			} catch (JsonProcessingException e) {
				logger.info("REPLICATE_DATA_STATS task finished, data: \n{}", replicateDataStatsRecord);
			}
		} else {
			taskResult.setFailedResult("Mongo client is null");
		}

		callback.accept(taskResult);
	}

	private static Update getUpdate(Document document) {
		Update update = new Update();
		if (MapUtils.isNotEmpty(document)) {
			document.forEach((k, v) -> update.set(k, v));
		}

		return update;
	}
}
