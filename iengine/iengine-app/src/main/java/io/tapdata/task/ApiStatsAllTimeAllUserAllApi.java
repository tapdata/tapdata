package io.tapdata.task;

import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.JSONUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@TaskType(type = "API_STATS_ALLTIME_ALLUSER_ALLAPI")
public class ApiStatsAllTimeAllUserAllApi implements Task {

	private Logger logger = LogManager.getLogger(getClass());

	private TaskContext taskContext;

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult taskResult = new TaskResult();
		taskResult.setPassResult();
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
		if (mongoTemplate != null) {
			try {
				List<Document> initData = initData();

				for (Document document : mongoTemplate.getDb().getCollection(ConnectorConstant.MODULES_COLLECTION).aggregate(Arrays.asList(
						new Document().append("$match", new Document("status", "active")),
						new Document().append("$project",
								new Document().append("numberOfPaths",
										new Document("$cond",
												new Document("if", new Document("$isArray", "$paths")).append("then", new Document("$size", "$paths")).append("else", 0)
										)
								)
						),
						new Document().append("$group", new Document("_id", null)
								.append("sum_of_published_api",
										new Document().append("$sum", "$numberOfPaths")
								)
						)
				))) {
					initData.get(2).get("data", Map.class).put("sum_of_published_api", document.get("sum_of_published_api"));
				}

				for (Document document : mongoTemplate.getDb().getCollection(ConnectorConstant.MODULES_COLLECTION).aggregate(Arrays.asList(
						new Document().append("$project",
								new Document().append("numberOfPaths",
										new Document().append("$cond",
												new Document().append("if",
														new Document().append("$isArray", "$paths")
												).append("then", new Document("$size", "$paths")).append("else", 0)
										)
								)
						),
						new Document().append("$group", new Document("_id", null).append("sum_of_api", new Document("$sum", "$numberOfPaths")))
				))) {
					initData.get(2).get("data", Map.class).put("sum_of_api", document.get("sum_of_api"));
				}

				long serverTimestamp = MongodbUtil.mongodbServerTimestamp(mongoTemplate.getDb());
				Date serverTime = new Date(serverTimestamp);
				List<Document> documents = statsAllApiEveryAccessPipeline();
				for (Document api : mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).aggregate(documents)) {
					Document top = new Document();
					top.putAll(initData.get(2));

					initData.get(0).put("aggregate_time", serverTime);
					top.putAll(initData.get(0));

					Document data = new Document();
					data.putAll(api);
					data.put("update_time", serverTime);

					top.put("data", data);
					mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).findOneAndUpdate(
							new Document("stats_category", "openapi").append("stats_name", "ALL-time:ALL-user:ALL-api"),
							new Document("$set", top),
							new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
					);
				}

			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				taskResult.setFailedResult(e.getMessage());
			}
		}

		callback.accept(taskResult);
	}

	private List<Document> statsAllApiEveryAccessPipeline() {
		List<Document> documents = Arrays.asList(
				new Document().append("$match",
						new Document("stats_category", "openapi").append("stats_name", "ALL-time:ALL-user:EVERY-api")
				),
				new Document().append("$group",
						new Document().append("_id", null)
								.append("sum_of_used_api", new Document("$sum", 1))
								.append("api_uids", new Document("$push", "$data.api_uids"))
								.append("api_calls", new Document("$sum", "$data.api_calls"))
								.append("req_bytes", new Document("$sum", "$data.req_bytes"))
								.append("res_bytes", new Document("$sum", "$data.res_bytes"))
								.append("res_rows", new Document("$sum", "$data.res_rows"))
								.append("total_call_time", new Document("$sum", "$data.total_call_time"))
								.append("time_of_api_last_used", new Document("$max", "$data.time_of_api_last_used"))
				),
				new Document().append("$project",
						new Document().append("_id", 0)
								.append("sum_of_used_api", 1)
								.append("api_calls", 1)
								.append("req_bytes", 1)
								.append("res_bytes", 1)
								.append("res_rows", 1)
								.append("total_call_time", 1)
								.append("time_of_api_last_used", 1)
								.append("avg_call_time",
										new Document().append("$divide", Arrays.asList("$total_call_time", "$api_calls"))
								)
								.append("api_uids",
										new Document().append("$reduce",
												new Document().append("input", "$api_uids")
														.append("initialValue", new ArrayList<>())
														.append("in",
																new Document("$setUnion", Arrays.asList("$$value", "$$this"))
														)
										)
								)
				),
				new Document().append("$project",
						new Document().append("sum_of_used_api", 1)
								.append("api_calls", 1)
								.append("req_bytes", 1)
								.append("res_bytes", 1)
								.append("res_rows", 1)
								.append("total_call_time", 1)
								.append("time_of_api_last_used", 1)
								.append("avg_call_time", 1)
								.append("api_uids", 1)
								.append("api_path", "T O T A L :")
								.append("api_users", new Document("$size", "$api_uids"))
				)
		);

		return documents;
	}

	private List<Document> initData() throws IOException {
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
		List<Document> documents = JSONUtil.json2List(initDataJson, Document.class);

		return documents;
	}

	public static void main(String[] args) throws IOException {

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


		List<Document> documents = JSONUtil.json2List(initDataJson, Document.class);

		System.out.println(documents.size());
	}
}
