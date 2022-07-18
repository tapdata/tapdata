package io.tapdata.task;

import com.mongodb.client.FindIterable;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@TaskType(type = "ALARM_LOG_OF_DK")
public class AlarmLogOfDK36 implements Task {

	private Logger logger = LogManager.getLogger(getClass());

	private TaskContext taskContext;

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();

		TaskResult taskResult = new TaskResult();
		taskResult.setPassResult();
		List<Document> workers = new ArrayList<>();
		try {
			for (String processId : mongoTemplate.getDb().getCollection(ConnectorConstant.WORKER_COLLECTION).distinct("process_id", new Document("worker_type", "system"), String.class)) {
				for (
						Document worker : mongoTemplate.getDb().getCollection(ConnectorConstant.WORKER_COLLECTION).find(
						new Document().append("worker_type", "system").append("process_id", processId)
				).sort(new Document("ping_time", -1)).limit(1)
				) {
					setNameZH(worker);

					checkHealth(worker, mongoTemplate);
					workers.add(worker);
				}
			}

			for (Document worker : mongoTemplate.getDb().getCollection(ConnectorConstant.WORKER_COLLECTION).find(new Document("worker_type", new Document("$ne", "system")))) {
				setNameZH(worker);
				checkHealth(worker, mongoTemplate);
				workers.add(worker);
			}

			for (Document worker : workers) {
				FindIterable<Document> latestAlarmRecord = mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).find(
						new Document().append("stats_category", "dk")
								.append("stats_name", "alarmLog")
								.append("data.serviceName", worker.getString("worker_type"))
								.append("data.process_id", worker.getString("process_id"))
				).sort(new Document("data.time", -1)).limit(1);

				boolean needNewLog = false;
				Document latestAlarmLog = null;
				for (Document alarmLog : latestAlarmRecord) {
					latestAlarmLog = alarmLog;
				}

				if (latestAlarmLog != null && latestAlarmLog.containsKey("data")) {
					Map<String, Object> data = (Map<String, Object>) latestAlarmLog.get("data");
					needNewLog = !worker.getString("health").equals(data.get("type"));
				} else {
					needNewLog = true;
				}

				if (needNewLog) {

					Object port = worker.get("port");
					port = port == null ? "" : ":" + port;

					mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).insertOne(
							new Document()
									.append("stats_category", "dk")
									.append("stats_name", "alarmLog")
									.append("stats_granularity", "all")
									.append("stats_time", "")
									.append("data",
											new Document().append("type", worker.getString("health"))
													.append("time", worker.getDate("serverTime"))
													.append("hostname", worker.getString("hostname"))
													.append("process_id", worker.getString("process_id"))
													.append("ipPort", worker.getString("worker_ip") + port)
													.append("serviceName", worker.getString("worker_type"))
													.append("serviceNameZh", worker.getString("nameZh"))
													.append("desc", getHealthDesc(worker))
									)
					);
				}
			}


			taskResult.setTaskResult(200);
		} catch (Exception e) {
			logger.error("Process alarm log for dk failed.", e);
			taskResult.setTaskResult(201);
			taskResult.setFailedResult("Process alarm log for dk failed " + e.getMessage());
		}

		callback.accept(taskResult);
	}

	private String getHealthDesc(Document worker) {
		return "red".equals(worker.getString("health")) ? "服务[" + worker.getString("process_id") + "], 不可用" : "服务[" + worker.getString("process_id") + "], 运行正常";
	}

	private void checkHealth(Document worker, MongoTemplate mongoTemplate) {
		long serverTimestamp = MongodbUtil.mongodbServerTimestamp(mongoTemplate.getDb());
		Date serverTime = new Date(serverTimestamp);
		worker.put("serverTime", serverTime);
		long pingTime = worker.get("ping_time") instanceof Long ? worker.getLong("ping_time") : new BigDecimal(String.valueOf(worker.get("ping_time"))).longValue();
		if ((serverTime.getTime() - pingTime) > 120 * 1000) {
			worker.put("health", "red");
		} else {
			worker.put("health", "green");
		}
	}

	private void setNameZH(Document worker) {
		switch (worker.getString("worker_type")) {
			case "tapdata-manager":
				worker.put("nameZh", "管理平台");
				break;
			case "connector":
				worker.put("nameZh", "数据连接器");
				break;
			case "transformer":
				worker.put("nameZh", "数据处理器");
				break;
			case "api-server":
				worker.put("nameZh", "数据发布平台");
				break;
			case "MongoDB":
				worker.put("nameZh", "MongoDB");
				break;
			case "system":
				worker.put("nameZh", "系统资源监视器");
				break;
			case "Device":
				worker.put("nameZh", "重要设备");
				break;
			default:
				break;
		}
	}
}
