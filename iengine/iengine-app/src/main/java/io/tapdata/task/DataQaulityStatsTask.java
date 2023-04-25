package io.tapdata.task;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MetadataUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;


@TaskType(type = "BG_JOBS")
public class DataQaulityStatsTask implements Task {

	private Logger logger = LogManager.getLogger(getClass());

	protected TaskContext taskContext;
	protected List<String> connectionIds;
	private List<String> connectFailedUri;

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
		connectFailedUri = new ArrayList<>();
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult result = new TaskResult();
		result.setPassResult();
		try {
			qualityAnalysis();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			result.setTaskResultCode(201);
			result.setTaskResult(e.getMessage());
		}
		callback.accept(result);
	}

	protected void qualityAnalysis() {
		logger.info("Starting quality analytics");
		ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		MongoTemplate mongoTemplate = clientMongoOperator.getMongoTemplate();
		if (mongoTemplate != null) {
			try {
				Criteria criteria = where("database_type").is("mongodb");
				if (CollectionUtils.isNotEmpty(connectionIds)) {
					logger.info(String.format("Analyze quality by connection id: %s", connectionIds));
					criteria.and("_id").in(connectionIds);
				}
				Query query = new Query(criteria);
				query.fields().exclude("schema");
				List<Connections> connections = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);

				for (Connections connection : connections) {
					try {
						logger.info("Running quality analysis for connection {} at {}.", connection.getName(), new Date());
						analyzeTable(connection);
						logger.info("Finished quality analysis for connection {} at {}", connection.getName(), new Date());
					} catch (Exception e) {
						logger.error("Error processing target connection {}.", connection, e);
					}
				}

				DeleteResult deleteResult;
				FindIterable<Document> dataCatalogResult = mongoTemplate.getDb().getCollection(ConnectorConstant.DATA_CATALOG_COLLECTION).find(
						new Document()
				).projection(new Document("_id", 1).append("conn_info", 1).append("violated_docs", 1));
				Set<String> uriSet = new HashSet<>();
				for (Document dataCatalog : dataCatalogResult) {
					Map<String, Object> connInfo = (Map<String, Object>) dataCatalog.get("conn_info");
					if (dataCatalog.containsKey("conn_info") && connInfo != null) {
						String uri = (String) connInfo.get("uri");
						uriSet.add(uri);
					}
				}

				for (String uri : uriSet) {
					long countDocuments = mongoTemplate.getDb().getCollection(ConnectorConstant.CONNECTION_COLLECTION).countDocuments(
							new Document("database_uri", uri)
					);

					if (countDocuments <= 0) {
						logger.info("Found mongodb uri already deleted {}.", uri);
						mongoTemplate.getDb().getCollection(ConnectorConstant.DATA_CATALOG_COLLECTION).deleteMany(
								new Document("conn_info.uri", uri)
						);
					}
				}

				List<Document> orList = new ArrayList<>();
				orList.add(new Document("violated_docs", 0));
				orList.add(new Document("total_docs", 0));
				if (CollectionUtils.isNotEmpty(connectFailedUri)) {
					orList.add(new Document("conn_info.uri", new Document("$in", connectFailedUri)));
				}
				Document deleteFilter = new Document("$or", orList);
				deleteResult = mongoTemplate.getDb().getCollection(ConnectorConstant.DATA_CATALOG_COLLECTION)
						.deleteMany(deleteFilter);
				logger.info("Delete data catalog, filter: {}, result: {}", deleteFilter.toJson(), deleteResult);

			} catch (Exception e) {
				throw new RuntimeException("Stats data quality failed.", e);
			}
		}
	}

	private void analyzeTable(Connections connections) {
		String uri = connections.getDatabase_uri();
		String database = MongodbUtil.getDatabase(connections);
		String databaseType = connections.getDatabase_type();
		Map<String, List<RelateDataBaseTable>> schema = connections.getSchema();

		MongoDatabase metaDatabase = taskContext.getClientMongoOperator().getMongoTemplate().getDb();
		if (MapUtils.isNotEmpty(schema)) {

			// read secondary preferred
			MongoClientOptions.Builder builder = MongoClientOptions.builder();
			builder.readPreference(ReadPreference.secondaryPreferred());

			MongoClient mongoClient = null;

			try {
				try {
					mongoClient = MongodbUtil.createMongoClient(connections, builder.serverSelectionTimeout(2000).build());
				} catch (Exception e) {
					connectFailedUri.add(uri);
				}

				List<RelateDataBaseTable> relateDataBaseTables = schema.get("tables");
				for (RelateDataBaseTable relateDataBaseTable : relateDataBaseTables) {
					if (relateDataBaseTable == null) {
						continue;
					}
					String tableName = relateDataBaseTable.getTable_name();
					Document connectionInfo = new Document("uri", uri).append("collection", tableName);
					Document matchCondition = new Document("__tapd8.result", "invalid");
					try {
						long totalCount = MongodbUtil.getCollectionNotAggregateCountByTableName(mongoClient, database, tableName, null);
						long violatesCount = MongodbUtil.getCollectionNotAggregateCountByTableName(mongoClient, database, tableName, matchCondition);

						violatesCount = violatesCount < 0 ? 0 : violatesCount;

						double violatePercentage = 0d;
						if (totalCount > 0) {
							violatePercentage = new BigDecimal(violatesCount)
									.divide(new BigDecimal(totalCount), 2, BigDecimal.ROUND_HALF_UP)
									.multiply(new BigDecimal(100)).doubleValue();
						}

						String assetDesc = tableName;

						String qualifiedName = MetadataUtil.formatQualifiedName("MC_" + databaseType + "_" + database + "_" + tableName + "_" + connections.getId());
						for (Document result : metaDatabase.getCollection(ConnectorConstant.METADATA_INSTANCE_COLLECTION).find(new Document("qualified_name", qualifiedName))) {
							String name = result.getString("name");
							assetDesc = StringUtils.isNotBlank(name) ? name : assetDesc;
							logger.info("Metadata name {}, asset desc {}", name, assetDesc);
						}

						metaDatabase.getCollection(ConnectorConstant.DATA_CATALOG_COLLECTION).updateMany(
								new Document("conn_info", connectionInfo),
								new Document("$set",
										new Document().append("connection_id", new ObjectId(connections.getId()))
												.append("total_docs", totalCount)
												.append("violated_docs", violatesCount)
												.append("database", database)
												.append("collection", tableName)
												.append("user_id", connections.getUser_id())
												.append("tags", connections.getTags())
												.append("asset_desc", assetDesc)
												.append("violated_percentage", violatePercentage)
								).append("$currentDate",
										new Document("create_time", true).append("lastModified", true)
								),
								new UpdateOptions().upsert(true)
						);

					} catch (Exception e) {
						logger.error("Data quality stats failed, uri: {}, collection: {}, err: {}, stack: {}", uri, tableName, e.getMessage(), Log4jUtil.getStackString(e));
						connectFailedUri.add(uri);
						break;
					}
				}
			} finally {
				Optional.ofNullable(mongoClient).ifPresent(m -> m.close());
			}
		}
	}
}
