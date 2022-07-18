package io.tapdata.task;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.JSONUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@TaskType(type = "API_STATS_ALLTIME_ALLUSER_EVERYAPI")
public class ApiStatsAllTimeAllUserEveryApi implements Task {

	private Logger logger = LogManager.getLogger(getClass());

	private TaskContext taskContext;

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult taskResult = new TaskResult();
		try {
			ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
			MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
			if (mongoTemplate != null) {

				long serverTimestamp = MongodbUtil.mongodbServerTimestamp(mongoTemplate.getDb());
				Date serverTime = new Date(serverTimestamp);
				List<org.bson.Document> initData = initData();
				initData.get(0).put("aggregate_time", serverTime);

				Document lastat = new Document();
				for (Document document : mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).find(
						new Document().append("stats_category", "openapi")
								.append("stats_name", "ALL-time:ALL-user:EVERY-api")
				).limit(1)) {
					lastat = document;
				}

				Document aggregateTimeMatch = new Document("$match", new Document().append("api_path", new Document("$exists", true)));
				if (MapUtils.isNotEmpty(lastat) && lastat.containsKey("aggregate_time")) {
					((Map) aggregateTimeMatch.get("$match")).put("received_date",
							new Document().append("$gt", lastat.get("aggregate_time"))
									.append("$lte", initData.get(0).get("aggregate_time"))
					);
				} else {
					((Map) aggregateTimeMatch.get("$match")).put("received_date",
							new Document().append("$lte", initData.get(0).get("aggregate_time"))
					);
				}

				mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).updateMany(
						new Document().append("stats_category", "openapi").append("stats_name", "ALL-time:ALL-user:EVERY-api"),
						new Document().append("$set",
								new Document().append("aggregate_time", initData.get(0).get("aggregate_time"))
						)
				);
				for (Document api : mongoTemplate.getDb().getCollection(ConnectorConstant.API_CALL_COLLECTION).aggregate(Arrays.asList(
						aggregateTimeMatch,
						new Document().append("$group",
								new Document().append("_id",
												new Document().append("allPathId", "$allPathId")
														.append("api_path", "$api_path")
														.append("method", "$method")
										).append("api_uids", new Document("$addToSet", "$user_id"))
										.append("api_calls", new Document("$sum", 1))
										.append("time_of_api_last_used", new Document("$max", "$received_date"))
										.append("req_bytes", new Document("$sum", "$req_bytes"))
										.append("res_bytes", new Document("$sum", "$res_bytes"))
										.append("res_rows", new Document("$sum", "$res_rows"))
										.append("total_call_time", new Document("$sum", "$latency"))
						),
						new Document().append("$project",
								new Document().append("api_method", "$_id.method")
										.append("api_path", "$_id.api_path")
										.append("api_all_path_id", "$_id.allPathId")
										.append("_id", false)
										.append("api_uids", "$api_uids")
										.append("api_calls", "$api_calls")
										.append("time_of_api_last_used", "$time_of_api_last_used")
										.append("req_bytes", "$req_bytes")
										.append("res_bytes", "$res_bytes")
										.append("res_rows", "$res_rows")
										.append("total_call_time", "$total_call_time")
						)
				)).allowDiskUse(true)) {
					api.put("api_status", "deleted");
					if (api != null && api.containsKey("api_all_path_id") && StringUtils.isNotBlank(api.getString("api_all_path_id"))) {
						for (Document apiInfo : mongoTemplate.getDb().getCollection(ConnectorConstant.MODULES_COLLECTION).find(
								new Document().append("_id", new ObjectId(api.getString("api_all_path_id")))
						)) {
							api.put("api_status", apiInfo.get("status"));
						}
					}

					Document top = new Document();
					top.putAll(initData.get(3));
					top.putAll(initData.get(0));

					Document data = new Document();
					data.putAll(api);

					Document inc = new Document();
					inc.append("data.api_calls", data.get("api_calls"));
					inc.append("data.req_bytes", data.get("req_bytes"));
					inc.append("data.res_bytes", data.get("res_bytes"));
					inc.append("data.res_rows", data.get("res_rows"));
					inc.append("data.total_call_time", data.get("total_call_time"));

					data.remove("api_calls");
					data.remove("req_bytes");
					data.remove("res_bytes");
					data.remove("res_rows");
					data.remove("total_call_time");

					List<String> apiUids = (List<String>) data.get("api_uids");
					data.remove("api_uids");

					Document addPrefix = new Document();
					for (Map.Entry<String, Object> entry : data.entrySet()) {
						addPrefix.append("data." + entry.getKey(), entry.getValue());
					}

					top.remove("data");
					top.putAll(addPrefix);

					Document updatedDocument = mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).findOneAndUpdate(
							new Document().append("stats_category", "openapi")
									.append("stats_name", "ALL-time:ALL-user:EVERY-api")
									.append("data.api_method", api.getString("api_method"))
									.append("data.api_path", api.getString("api_path"))
									.append("data.api_all_path_id", api.getString("api_all_path_id")
									),
							new Document().append("$set", top).append("$inc", inc)
									.append("$addToSet", new Document("data.api_uids", new Document("$each", apiUids)))
							, new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true)
					);

					Object totalCallTime = ((Document) updatedDocument.get("data")).get("total_call_time");
					Object apiCalls = ((Document) updatedDocument.get("data")).get("api_calls");

					if (totalCallTime != null && apiCalls != null) {
						addPrefix.put("data.avg_call_time", new BigDecimal(String.valueOf(totalCallTime)).longValue() / new BigDecimal(String.valueOf(apiCalls)).longValue());
					}
					List totalApiUids = (List) ((Document) updatedDocument.get("data")).get("api_uids");
					addPrefix.put("data.api_users", totalApiUids == null ? 0 : totalApiUids.size());


					mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).updateOne(
							new Document().append("_id", updatedDocument.getObjectId("_id")),
							new Document().append("$set", addPrefix)
									.append("$currentDate", new Document("data.update_time", true))
					);
				}
			}

			taskResult.setPassResult();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			taskResult.setFailedResult(e.getMessage());
		}

		callback.accept(taskResult);
	}

	private List<org.bson.Document> initData() throws IOException {
//        Document serverStatus = taskContext.getClientMongoOperator().getMongoTemplate().getDb().runCommand(new Document("serverStatus", 1));
//        Date serverTime = serverStatus.getDate("localTime");

//        List<Document> data = new ArrayList<>();
//        data.add(new Document().append("stats_category", "openapi").append("aggregate_time", serverTime));
//        data.add(new Document().append("api_calls", 0).append("req_bytes", 0).append("res_bytes", 0).append("res_rows", 0).append("total_call_time", 0).append("avg_call_time", 0));
//        data.add(
//                new Document().append("stats_name", "ALL-time:ALL-user:ALL-api").append("stats_granularity", "all").append("stats_time", "")
//                        .append("stats_keys", new Document()
//                                .append("time_span", "ALL").append("user", "ALL").append("api", "ALL")
//                        )
//                        .append("data", new Document()
//                                .append("api_users", 0).append("sum_of_used_api", 0).append("sum_of_published_api", 0).append("sum_of_api", 0)
//                        )
//        );
//
//        data.add(
//                new Document().append("stats_name", "ALL-time:ALL-user:EVERY-api").append("stats_granularity", "all").append("stats_time", "")
//                        .append("stats_keys", new Document()
//                                .append("time_span", "ALL").append("user", "ALL").append("api", "EVERY")
//                        )
//                        .append("data", new Document()
//                                .append("api_method", "").append("api_path", "").append("api_all_path_id", "").append("api_status", "").append("api_update_time", null)
//                        )
//        );

		String initDataJson = "[" +
				"  {" +
				"   \"stats_category\":\"openapi\"," +
				"   \"aggregate_time\" : null" +
				"  }," +
				" " +
				"  {" +
				"   \"api_calls\": 0," +
				"   \"req_bytes\": 0," +
				"   \"res_bytes\": 0," +
				"   \"res_rows\":0," +
				"   \"total_call_time\": 0," +
				"   \"avg_call_time\": 0" +
				"  }," +
				"  {" +
				"   \"stats_name\":\"ALL-time:ALL-user:ALL-api\"," +
				"   \"stats_granularity\":\"all\"," +
				"   \"stats_time\":\"\"," +
				"   \"stats_keys\":{" +
				"   \"time_span\":\"ALL\",  " +
				"   \"user\":\"ALL\", " +
				"   \"api\":\"ALL\" " +
				"   }," +
				"   \"data\":{" +
				"   \"api_users\":0," +
				"   \"sum_of_used_api\":0," +
				"   \"sum_of_published_api\":0," +
				"   \"sum_of_api\":0" +
				"   }" +
				"  }," +
				"  {" +
				"   \"stats_name\":\"ALL-time:ALL-user:EVERY-api\"," +
				"   \"stats_granularity\":\"all\"," +
				"   \"stats_time\":\"\"," +
				"   \"stats_keys\":{" +
				"   \"time_span\":\"ALL\",  " +
				"   \"user\":\"ALL\", " +
				"   \"api\":\"EVERY\" " +
				"   }," +
				"   \"data\":{" +
				"   \"api_method\":\"\"," +
				"   \"api_path\":\"\"," +
				"   \"api_all_path_id\":\"\"," +
				"   \"api_status\":\"\"," +
				"   \"api_update_time\":null," +
				"   \"api_users\":0," +
				"   \"time_of_api_last_used\":null" +
				"   }" +
				"  }," +
				"  {" +
				"   \"stats_name\":\"ALL-time:EVERY-user:ALL-api\"," +
				"   \"stats_granularity\":\"all\",    " +
				"   \"stats_time\":\"\"," +
				"   \"stats_keys\":{" +
				"   \"time_span\":\"ALL\",  " +
				"   \"user\":\"EVERY\", " +
				"   \"api\":\"ALL\" " +
				"   }," +
				" " +
				" " +
				"   \"data\":{" +
				"   \"user_id\":\"23023298843028343\"," +
				"   \"user_name\":\"gaoxiaozu\"," +
				"   \"sum_of_used_api\":30," +
				"   \"sum_of_can_be_accessed_api\":40," +
				"   \"time_of_api_last_used\": null" +
				"   }" +
				"  }," +
				"  {" +
				"   \"stats_name\":\"ALL-time:EVERY-user:THIS-api\"," +
				"   \"stats_granularity\":\"all\",    " +
				"   \"stats_time\":\"\"," +
				"   \"stats_keys\":{" +
				"   \"time_span\":\"ALL\"," +
				"   \"user\":\"EVERY\", " +
				"   \"api\":{" +
				"    \"api_method\":\"GET\"," +
				"    \"api_path\":\"api/orders\"," +
				"    \"api_all_path_id\":\"4329329239432823032\"" +
				"   }" +
				"   }," +
				" " +
				"   \"data\":{" +
				"   \"user_id\":\"23023298843028343\"," +
				"   \"user_name\":\"gaoxiaozu\"," +
				"   \"time_of_api_last_used\": null" +
				"   }" +
				"  }," +
				"  {" +
				"   \"stats_name\":\"ALL-time:THIS-user:EVERY-api\"," +
				"   \"stats_granularity\":\"all\",    " +
				"   \"stats_time\":\"\"," +
				"   \"stats_keys\":{" +
				"   \"time_span\":\"ALL\"," +
				"   \"user\":{" +
				"    \"user_id\":\"23023298843028343\"," +
				"    \"user_name\":\"gaoxiaozu\"" +
				"   }," +
				"   \"api\":\"EVERY\" " +
				"   }," +
				"   \"data\":{" +
				"   \"api_method\":\"GET\"," +
				"   \"api_path\":\"api/orders\"," +
				"   \"api_all_path_id\":\"4329329239432823032\"," +
				"   \"time_of_api_last_used\": null" +
				"   }" +
				"  }" +
				"  ]";
		List<org.bson.Document> documents = JSONUtil.json2List(initDataJson, org.bson.Document.class);

		return documents;
	}
}
