package io.tapdata.task;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MetadataUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.Stats;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

@TaskType(type = "DASHBOARD_STATS")
public class DashboardTask implements Task {

	private TaskContext taskContext;

	private Logger logger = LogManager.getLogger(getClass());

	private static final String LOG_PREFIX = "DASHBOARD_STATS - ";

	private static final int BATCH_SIZE = 1000;

	private static List<String> totalTypesDatabaseTypes;

	static {
		totalTypesDatabaseTypes = new ArrayList<String>() {{
			add("mongodb");
		}};
	}

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

				long startTs = System.currentTimeMillis();
				publishDataStats();
				logger.info("{}publish data stats spend: {} ms", LOG_PREFIX, (System.currentTimeMillis() - startTs));

				startTs = System.currentTimeMillis();
				dataOverview();
				logger.info("{}data over view spend: {} ms", LOG_PREFIX, (System.currentTimeMillis() - startTs));

				startTs = System.currentTimeMillis();
				trendStats();
				logger.info("{}trend stats spend: {} ms", LOG_PREFIX, (System.currentTimeMillis() - startTs));
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			taskResult.setFailedResult(e.getMessage());
		}

		callback.accept(taskResult);
	}

	private void publishDataStats() {
		logger.info("Start stats publish stats data.");
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();

		try {
			int updated = 0;
//            Document serverStatus = mongoTemplate.getDb().runCommand(new Document("serverStatus", 1));
			long serverTimestamp = MongodbUtil.mongodbServerTimestamp(mongoTemplate.getDb());
			Date serverTime = new Date(serverTimestamp);

			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
			String hourString = sdf.format(serverTime) + "0000";

			mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).deleteMany(
					new Document().append("stats_granularity", "minute")
							.append("stats_name", "api_stats")
			);

			FindIterable<Document> metadataDefinitions = mongoTemplate.getDb().getCollection("MetadataDefinition").find(
					new Document("item_type", "database")
							.append("$or", Arrays.asList(
									new Document("parent_id", new Document("$exists", false)),
									new Document("parent_id", null),
									new Document("parent_id", "")
							))
			).projection(new Document("_id", 1).append("value", 1));

			List<WriteModel<Document>> writeModels = new ArrayList<>();

			for (Document metadataDefinition : metadataDefinitions) {
				String metadataDefinitionId = metadataDefinition.getObjectId("_id").toHexString();

				Document apiDataStats = buildApiDataStats(metadataDefinition.getString("value"), hourString);

				Document apiDataStatsData = (Document) apiDataStats.get("data");

				publishConnectionStats(metadataDefinitionId, apiDataStatsData);

				addWriteModels(writeModels, apiDataStats, apiDataStatsData);

				if (writeModels.size() % BATCH_SIZE == 0) {
					updated += bulkWriteInsights(mongoTemplate, writeModels);
				}

				logger.info("Completed stats definition {} publish data stats.", metadataDefinition.getString("value"));
			}

			// handle connections not have definition
			Document apiDataStats = buildApiDataStats("", hourString);

			Document apiDataStatsData = (Document) apiDataStats.get("data");

			publishConnectionStats("", apiDataStatsData);

			addWriteModels(writeModels, apiDataStats, apiDataStatsData);

			updated += bulkWriteInsights(mongoTemplate, writeModels);

			logger.info("Completed stats definition {} publish data stats.", "no definition");

			logger.info("Total created publish stats record updated {}", updated);

		} catch (Exception e) {
			throw new RuntimeException("Publish data stats for connection failed", e);
		}
	}

	private int bulkWriteInsights(MongoTemplate mongoTemplate, List<WriteModel<Document>> writeModels) {
		int updated;
		if (CollectionUtils.isEmpty(writeModels)) {
			return 0;
		}
		BulkWriteResult bulkWriteResult = mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).bulkWrite(writeModels, new BulkWriteOptions().ordered(true));
		updated = bulkWriteResult.getModifiedCount();
		writeModels.clear();
		return updated;
	}

	private void addWriteModels(List<WriteModel<Document>> writeModels, Document apiDataStats, Document apiDataStatsData) {
		writeModels.add(new UpdateOneModel(new Document().append("stats_name", apiDataStats.getString("stats_name"))
				.append("data.name", apiDataStatsData.getString("name")), new Document("$set", apiDataStats), new UpdateOptions().upsert(true)));
	}

	private void dataOverview() {
		logger.info("Start stats data overview data.");
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();

		try {
			int updated = 0;
			long serverTimestamp = MongodbUtil.mongodbServerTimestamp(mongoTemplate.getDb());
			Date serverTime = new Date(serverTimestamp);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String dayString = sdf.format(serverTime) + "000000";

			Document totalPublishStatsData = new Document().append("total_records", 0L)
					.append("total_data_size", 0L)
					.append("total_types", 0L)
					.append("published_types", 0L);
			Document totalPublishStatsRecord = new Document();
			totalPublishStatsRecord.append("stats_granularity", "daily");
			totalPublishStatsRecord.append("stats_name", "total_publish_stats");
			totalPublishStatsRecord.append("stats_time", dayString);
			totalPublishStatsRecord.append("data", totalPublishStatsData);

//			for (Document totalTypesCount : mongoTemplate.getDb().getCollection(ConnectorConstant.CONNECTION_COLLECTION).aggregate(Arrays.asList(
//					new Document("$match",
//							new Document()
//									.append("$or", Arrays.asList(
//											new Document("connection_type", "target"),
//											new Document("connection_type", "source_and_target")
//									))
//									.append("status", "ready")
//									.append("database_type", new Document("$in", totalTypesDatabaseTypes))
//					),
//					new Document("$unwind", "$schema.tables"),
//					new Document("$count", "count")
//			))) {
//				Long totalTypes = totalPublishStatsData.getLong("total_types");
//				totalTypes += new BigDecimal(String.valueOf(totalTypesCount.get("count"))).longValue();
//				totalPublishStatsData.put("total_types", totalTypes);
//			}

			long totalPublishTypes = mongoTemplate.getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).count(
					new Document("meta_type", "collection").append("is_deleted", false).append("$or", Arrays.asList(
							new Document("source.connection_type", "source_and_target"),
							new Document("source.connection_type", "target")
					))
			);
			totalPublishStatsData.put("total_types", totalPublishTypes);

			for (Document apiModules : mongoTemplate.getDb().getCollection(ConnectorConstant.MODULES_COLLECTION).find(new Document()).projection(
					new Document("tablename", 1).append("name", 1).append("basePath", 1).append("apiVersion", 1).append("connection", 1).append("status", 1)
			)) {
				if ("active".equals(apiModules.getString("status")) && apiModules.get("connection") != null) {
					logger.info("Start stats active api module {}.", apiModules.getString("tablename"));

					Query query = new Query(where("_id").is(apiModules.getObjectId("connection").toHexString()).and("database_type").is("mongodb"));
					query.fields().exclude("schema");
					List<Connections> connections = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);

					if (CollectionUtils.isNotEmpty(connections)) {
						List<WriteModel<Document>> writeModels = new ArrayList<>();
						for (Connections apiConnection : connections) {

							logger.info("Start stats apiConnection {} publish stats.", apiConnection.getName());

							Document connection = new Document();
							connection.put("database_uri", apiConnection.getDatabase_uri());
							apiModules.put("connection", connection);

							totalPublishStatsData.put("published_types", totalPublishStatsData.getLong("published_types") + 1);

							Document publishStatsData = new Document().append("total_records", 0L)
									.append("total_data_size", 0L)
									.append("api_name", "");
							Document publishStatsRecord = new Document();
							publishStatsRecord.append("stats_granularity", "daily");
							publishStatsRecord.append("stats_name", "publish_stats");
							publishStatsRecord.append("stats_time", dayString);
							publishStatsRecord.append("data", publishStatsData);

							if (StringUtils.isNotBlank(connection.getString("database_uri"))) {
								try (MongoClient mongoClient = MongodbUtil.createMongoClient(apiConnection)) {
									String database = MongodbUtil.getDatabase(apiConnection);
									String tableName = apiModules.getString("tablename");
									if (StringUtils.isNotBlank(database) && StringUtils.isNotBlank(tableName)) {
										Document collSatats = null;
										try {
											collSatats = mongoClient.getDatabase(database).runCommand(new Document("collStats", tableName));
										} catch (Throwable e) {
											logger.error("Execute command collStats failed on " + database + "." + tableName, e);
										}
										if (MapUtils.isNotEmpty(collSatats) && collSatats.containsKey("count")) {
											long count = new BigDecimal(String.valueOf(collSatats.get("count"))).longValue();
											publishStatsData.put("total_records", count);

											count += totalPublishStatsData.getLong("total_records");
											totalPublishStatsData.put("total_records", count);
										}

										if (MapUtils.isNotEmpty(collSatats) && collSatats.containsKey("size")) {
											long size = new BigDecimal(String.valueOf(collSatats.get("size"))).longValue();
											publishStatsData.put("total_data_size", size);

											size += totalPublishStatsData.getLong("total_data_size");
											totalPublishStatsData.put("total_data_size", size);
										}

										publishStatsData.put("api_name", tableName);

										String qualifiedName = MetadataUtil.formatQualifiedName("API_" + apiModules.getString("basePath") + "_" + apiModules.getString("apiVersion") + "_" + apiModules.getObjectId("_id").toHexString());
										for (Document document : mongoTemplate.getDb().getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).find(new Document("qualified_name", qualifiedName))) {
											String apiName = StringUtils.isNotBlank(document.getString("name")) ? document.getString("name") : document.getString("original_name");
											if (StringUtils.isNotBlank(apiName)) {
												publishStatsData.put("api_name", apiName);
											}
											break;
										}

										publishStatsData.put("update_time", serverTime);

										writeModels.add(new UpdateOneModel<>(new Document().append("stats_name", publishStatsRecord.getString("stats_name"))
												.append("stats_time", publishStatsRecord.getString("stats_time"))
												.append("api_id", apiModules.getObjectId("_id")),
												new Document("$set", publishStatsRecord), new UpdateOptions().upsert(true)));
										if (writeModels.size() % BATCH_SIZE == 0) {
											updated += bulkWriteInsights(mongoTemplate, writeModels);
										}

										logger.info("Completed stats api {} data overview stats.", tableName);
									}
								}
							}
						}

						updated += bulkWriteInsights(mongoTemplate, writeModels);
					}

				}
			}

			totalPublishStatsRecord.put("update_time", serverTime);
			UpdateResult updateResult = mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).updateOne(
					new Document().append("stats_name", totalPublishStatsRecord.getString("stats_name"))
							.append("stats_time", totalPublishStatsRecord.getString("stats_time")),
					new Document("$set", totalPublishStatsRecord),
					new UpdateOptions().upsert(true)
			);

			updated += updateResult.getModifiedCount();

			logger.info("Total created overview stats record updated {}", updated);

		} catch (Exception e) {
			logger.error("Stats dashboard data overview failed.", e);
			throw new RuntimeException("Stats dashboard data overview failed.", e);
		}
	}

	private void trendStats() {
		try {
			ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
			MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();

			Document trendStatsRecord = new Document();
			trendStatsRecord.append("stats_granularity", "minute");
			trendStatsRecord.append("stats_name", "trend_stats");
			trendStatsRecord.append("stats_time", "");
			trendStatsRecord.append("data", new Document("create", 0l).append("update", 0l));

			List<Job> jobs = clientMongoOperator.find(new Query(where("status").ne("draft")), ConnectorConstant.JOB_COLLECTION, Job.class);

			long serverTimestamp = MongodbUtil.mongodbServerTimestamp(mongoTemplate.getDb());
			Date serverTime = new Date(serverTimestamp);

			if (CollectionUtils.isNotEmpty(jobs)) {
				for (Job job : jobs) {
					if (job.getStats() != null && MapUtils.isNotEmpty(job.getStats().getTotal())) {
						if (job.getStats().getTotal().get("target_inserted") != null) {
							Long create = trendStatsRecord.get("data", Document.class).getLong("create");
							create += job.getStats().getTotal().get("target_inserted");
							trendStatsRecord.get("data", Document.class).put("create", create);
						}

						if (job.getStats().getTotal().get("total_updated") != null) {
							Long update = trendStatsRecord.get("data", Document.class).getLong("update");
							update += job.getStats().getTotal().get("total_updated");
							trendStatsRecord.get("data", Document.class).put("update", update);
						}
					}
				}
			}

			trendStatsRecord.put("update_time", serverTime);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
			String hourString = sdf.format(serverTime) + "0000";
			trendStatsRecord.put("stats_time", hourString);

			Document previousStats = new Document("data", new Document().append("create", 0).append("update", 0));
			for (Document document : mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).find(
					new Document().append("stats_name", "trend_stats")
							.append("stats_granularity", "minute")
							.append("stats_time", new Document("$ne", trendStatsRecord.getString("stats_time")))
			).sort(new Document("_id", -1)).limit(1)) {
				previousStats = document;
			}

			UpdateResult updateResult = mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).updateOne(
					new Document().append("stats_name", trendStatsRecord.getString("stats_name"))
							.append("stats_time", trendStatsRecord.getString("stats_time")),
					new Document("$set", trendStatsRecord),
					new UpdateOptions().upsert(true)
			);

			logger.info("Total created trend stats record {} updated.", updateResult.getModifiedCount());

			long created = trendStatsRecord.get("data", Document.class).getLong("create") - new BigDecimal(String.valueOf(previousStats.get("data", Document.class).get("create"))).longValue();
			long updated = trendStatsRecord.get("data", Document.class).getLong("update") - new BigDecimal(String.valueOf(previousStats.get("data", Document.class).get("update"))).longValue();

			mongoTemplate.getDb().getCollection(ConnectorConstant.INSIGHTS_COLLECTION).updateOne(
					new Document().append("stats_granularity", "minute")
							.append("stats_name", "trend_stats_increment")
							.append("stats_time", trendStatsRecord.getString("stats_time")),
					new Document("$set", new Document().append("stats_granularity", "minute")
							.append("stats_name", "trend_stats_increment")
							.append("stats_time", trendStatsRecord.getString("stats_time"))
							.append("update_time", trendStatsRecord.getDate("update_time"))
							.append("data",
									new Document("create", created < 0 ? 0 : created)
											.append("update", updated < 0 ? 0 : updated)
							)),
					new UpdateOptions().upsert(true)
			);

			logger.info("Create increment trend stats record {} updated", updateResult.getModifiedCount());
		} catch (Exception e) {
			logger.error("Stats dashboard trend stats failed.", e);
			throw new RuntimeException("Stats dashboard trend stats failed.", e);
		}

	}

	private void publishConnectionStats(String metadataDefinitionId, Document apiDataStatsData) {
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();

		Document connectionsQuery = new Document("status", "ready");

		if (StringUtils.isNotBlank(metadataDefinitionId)) {
			connectionsQuery.append("listtags.id", metadataDefinitionId)
					.append("$or", Arrays.asList(new Document("connection_type", "target"), new Document("connection_type", "source_and_target")));
		} else {
			connectionsQuery.append("$and", Arrays.asList(
					new Document("$or", Arrays.asList(new Document("connection_type", "target"), new Document("connection_type", "source_and_target"))),
					new Document("$or", Arrays.asList(new Document("listtags", null), new Document("listtags", new ArrayList<>())))
			));
		}

		for (Document connection : mongoTemplate.getDb().getCollection(ConnectorConstant.CONNECTION_COLLECTION).find(connectionsQuery).projection(new Document("_id", 1).append("name", 1))) {
			try {
				Query query = new Query(where("status").ne("draft").and("connections.target").is(connection.getObjectId("_id").toHexString()));
				query.fields().include("stats");
				List<Job> jobs = clientMongoOperator.find(query, ConnectorConstant.JOB_COLLECTION, Job.class);
				if (CollectionUtils.isNotEmpty(jobs)) {
					for (Job job : jobs) {
						Stats stats = job.getStats();
						if (stats != null && MapUtils.isNotEmpty(stats.getTotal())) {
							long inserted = stats.getTotal().get("processed") == null ? 0L : stats.getTotal().get("processed");
							inserted += apiDataStatsData.getLong("total_record");
							apiDataStatsData.put("total_record", inserted);

							long totalDataSize = stats.getTotal().get("total_data_size") == null ? 0L : stats.getTotal().get("total_data_size");
							totalDataSize += apiDataStatsData.getLong("total_data_size");
							apiDataStatsData.put("total_data_size", totalDataSize);

							long totalDataQuality = stats.getTotal().get("total_data_quality") == null ? 0L : stats.getTotal().get("total_data_quality");
							totalDataQuality += apiDataStatsData.getLong("total_violation");
							apiDataStatsData.put("total_violation", totalDataQuality);
						}
					}
				}
			} catch (Exception e) {
				logger.error("Publish data stats for connection {} failed.", connection.getString("name"), e);
			}
		}
	}

	private Document buildApiDataStats(String value, String hourString) {
		Document apiDataStatsData = new Document();
		apiDataStatsData.append("classifications", new ArrayList<>());
		apiDataStatsData.append("name", value);
		apiDataStatsData.append("total_record", 0l);
		apiDataStatsData.append("total_data_size", 0l);
		apiDataStatsData.append("total_violation", 0l);

		Document apiDataStats = new Document();
		apiDataStats.append("stats_granularity", "minute");
		apiDataStats.append("stats_name", "api_stats");
		apiDataStats.append("stats_time", hourString);
		apiDataStats.append("data", apiDataStatsData);

		return apiDataStats;
	}
}
