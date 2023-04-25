package io.tapdata.task.dataflow;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DataQualityTag;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.task.CleanResult;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.dataflow.aggregation.LRUAggregationProcessor;
import io.tapdata.task.Task;
import io.tapdata.task.TaskContext;
import io.tapdata.task.TaskResult;
import io.tapdata.task.TaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * 清理聚合目标表数据
 *
 * @author jackin
 * @date 2021/2/6 3:04 PM
 **/
@TaskType(type = "CLEAR_AGG_TARGET_TTL_DATA")
public class ClearAggregationTargetTTLDataTask implements Task {

	private final static int CLEAR_MAX_TRY_COUNT = 10;

	private Logger logger = LogManager.getLogger(ClearAggregationTargetTTLDataTask.class);

	private TaskContext taskContext;

	@Override
	public void initialize(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	@Override
	public void execute(Consumer<TaskResult> callback) {
		TaskResult taskResult = new TaskResult();
		taskResult.setPassResult();

		final ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		final Query query = new Query(
				where("status").is(DataFlow.STATUS_RUNNING).and("stages.type").is(Stage.StageTypeEnum.AGGREGATION_PROCESSOR.getType())
		);
		query.fields()
				.include("id")
				.include("stages");

		final List<DataFlow> dataFlows = clientMongoOperator.find(
				query,
				ConnectorConstant.DATA_FLOW_COLLECTION,
				DataFlow.class
		);

		Map<String, String> failedCollection = new HashMap<>();
		if (CollectionUtils.isEmpty(dataFlows)) {
			return;
		}

		Set<String> cleanedCollection = new HashSet<>();
		List<CleanResult> cleanResults = new ArrayList<>();
		for (DataFlow dataFlow : dataFlows) {
			List<Stage> stages = dataFlow.getStages();
			if (CollectionUtils.isEmpty(stages)) {
				continue;
			}

			for (Stage stage : stages) {

				try {
					if (!DataFlowStageUtil.isDataStage(stage.getType())) {
						continue;
					}

					final List<String> inputLanes = stage.getInputLanes();
					if (CollectionUtils.isNotEmpty(inputLanes)) {

						final String connectionId = stage.getConnectionId();
						final String collectionId = connectionId + "_" + stage.getName();
						if (!cleanedCollection.contains(collectionId)) {
							// 找到所有写入到这张表的任务
							final List<DataFlow> allRelateDataFlows = findAllRelateDataFlowByTargetTableName(connectionId, stage.getTableName());
							// 执行清理逻辑
							final CleanResult cleanResult = cleanTargetTableExpiredData(allRelateDataFlows, stage.getTableName(), connectionId);
							if (cleanResult != null) {
								cleanResults.add(cleanResult);
							}
							cleanedCollection.add(collectionId);
						}
					}
				} catch (Exception e) {
					logger.error("Clear aggregation target ttl data failed {}", e.getMessage(), e);
					failedCollection.put(stage.getName(), e.getMessage());
				}
			}
		}

		if (CollectionUtils.isNotEmpty(cleanResults)) {
			taskResult.setTaskResult(cleanResults);
		}

		callback.accept(taskResult);
	}

	/**
	 * 找出所有目标表是targetTableName的data flow
	 *
	 * @param connectionId
	 * @param targetTableName
	 * @return
	 */
	private List<DataFlow> findAllRelateDataFlowByTargetTableName(String connectionId, String targetTableName) {
		final ClientMongoOperator clientMongoOperator = taskContext.getClientMongoOperator();
		Query query = new Query(
				where("status").is(DataFlow.STATUS_RUNNING)
						.and("stages.connectionId").is(new Document("$regex", connectionId))
						.and("stages.tableName").is(targetTableName)
		);

		query.fields()
				.include("id")
				.include("stages");

		final List<DataFlow> dataFlows = clientMongoOperator.find(
				query,
				ConnectorConstant.DATA_FLOW_COLLECTION,
				DataFlow.class
		);

		// 过滤出所有满足targetTableName为目标的任务
		final List<DataFlow> tgtTableDataFlows = dataFlows.stream().filter(dataFlow -> {
			final List<Stage> stages = dataFlow.getStages();
			return stages.stream().filter(
					stage -> CollectionUtils.isNotEmpty(stage.getInputLanes())
							&& connectionId.equals(stage.getConnectionId())
							&& targetTableName.equals(stage.getTableName())
			).findFirst().orElse(null) != null;
		}).collect(Collectors.toList());

		return tgtTableDataFlows;
	}

	private CleanResult cleanTargetTableExpiredData(List<DataFlow> dataFlows, String collectionName, String connectionId) {
		CleanResult cleanResult = null;
		if (CollectionUtils.isEmpty(dataFlows)) {
			return cleanResult;
		}
		List<String> aggDataVersionSeqIds = new ArrayList<>();
		dataFlows.stream().forEach(dataFlow -> {
			final List<Stage> stages = dataFlow.getStages();
			stages.forEach(stage -> {

				final List<String> inputLanes = stage.getInputLanes();
				if (CollectionUtils.isNotEmpty(inputLanes)
						&& connectionId.equals(stage.getConnectionId())
						&& collectionName.equals(stage.getTableName())
				) {
					List<Stage> recentSourceStages = DataFlowStageUtil.findRecentSourceStage(stage, stages);

					for (Stage recentSourceStage : recentSourceStages) {
						if (Stage.StageTypeEnum.AGGREGATION_PROCESSOR.getType().equals(recentSourceStage.getType()) && !recentSourceStage.getKeepAggRet()) {
							aggDataVersionSeqIds.add(
									LRUAggregationProcessor.getAggDataVersionSeqId(
											dataFlow.getId(), recentSourceStage.getId()
									)
							);
						}
					}
				}

			});
		});

		if (CollectionUtils.isEmpty(aggDataVersionSeqIds)) {
			return cleanResult;
		}
		Query query = new Query(where("_id").is(connectionId));
		query.fields().exclude("schema");
		final List<Connections> connections = taskContext.getClientMongoOperator().find(
				query,
				ConnectorConstant.CONNECTION_COLLECTION,
				Connections.class
		);
		if (CollectionUtils.isEmpty(connections)) {
			return cleanResult;
		}

		final Connections conn = connections.get(0);
		if (DatabaseTypeEnum.MONGODB != DatabaseTypeEnum.fromString(conn.getDatabase_type())
				|| DatabaseTypeEnum.ALIYUN_MONGODB != DatabaseTypeEnum.fromString(conn.getDatabase_type())
		) {
			return cleanResult;
		}

		try (final MongoClient mongoClient = MongodbUtil.createMongoClient(conn)) {
			final String database = MongodbUtil.getDatabase(conn);
			final MongoDatabase mongoDatabase = mongoClient.getDatabase(database);

			final long collCount = MongodbUtil.getCollectionNotAggregateCountByTableName(
					mongoClient,
					database,
					collectionName,
					null
			);
			if (collCount == 0) {
				return cleanResult;
			}

			// 找出版本号最小的id和版本号
			Map<String, Long> seqIdVersionMap = getMinAggDataVersion(aggDataVersionSeqIds, mongoDatabase);

			if (MapUtils.isNotEmpty(seqIdVersionMap)) {

//				boolean foundCleanVersion = false;
//				int tryCount = 0;
//				while (!foundCleanVersion && tryCount <= CLEAR_MAX_TRY_COUNT) {
//					tryCount++;
//					for (Map.Entry<String, Long> entry : seqIdVersionMap.entrySet()) {
//						try (
//							MongoCursor<Document> mongoCursor = mongoDatabase.getCollection(collectionName).find(
//								new Document(
//									DataQualityTag.SUB_COLUMN_NAME + "." + entry.getKey(),
//									new Document("$gte", entry.getValue())
//								)
//							).limit(1).iterator()
//						) {
//							if (mongoCursor.hasNext()) {
//								foundCleanVersion = true;
//								break;
//							}
//							entry.setValue(entry.getValue() - 1);
//						}
//					}
//				}

//				if (foundCleanVersion) {
				for (Map.Entry<String, Long> entry : seqIdVersionMap.entrySet()) {
					final Document deleteFilter = new Document(
							DataQualityTag.SUB_COLUMN_NAME + "." + entry.getKey(),
							new Document("$lt", entry.getValue() - 1)
					);
					final DeleteResult deleteResult = mongoDatabase.getCollection(collectionName).deleteMany(deleteFilter);

					cleanResult = new CleanResult(
							deleteFilter.toJson(),
							collectionName,
							conn.getName(),
							deleteResult.getDeletedCount(),
							collCount,
							null
					);
					break;
				}
//				}
			}
		} catch (Exception e) {
			cleanResult = new CleanResult(
					null,
					collectionName,
					conn.getName(),
					null,
					null,
					e.getMessage()
			);
		}

		return cleanResult;
	}

	private Map<String, Long> getMinAggDataVersion(List<String> aggDataVersionSeqIds, MongoDatabase mongoDatabase) {
		Map<String, Long> seqIdVersionMap = new HashMap<>(1);
		aggDataVersionSeqIds.forEach(aggDataVersionSeqId -> {
			Document filter = new Document();
			filter.append(
					LRUAggregationProcessor.AGG_DATA_VERSION_SEQ_TYPE_FIELD,
					LRUAggregationProcessor.STAGE_AGG_DATA_VERSION_TYPE
			);
			filter.append(
					LRUAggregationProcessor.AGG_DATA_VERSION_SEQ_ID_FIELD,
					aggDataVersionSeqId
			);

			try (
					MongoCursor<Document> mongoCursor = mongoDatabase.getCollection(ConnectorConstant.DATA_VERSION_SEQ).find(filter).iterator()
			) {
				if (mongoCursor.hasNext()) {
					Document aggDataVersionDoc = mongoCursor.next();
					final Object seqObj = aggDataVersionDoc.get(LRUAggregationProcessor.AGG_DATA_VERSION_SEQ_FIELD);
					if (seqObj != null) {
						final long currDataVersion = new BigDecimal(seqObj.toString()).longValue();

						if (MapUtils.isEmpty(seqIdVersionMap)) {
							seqIdVersionMap.put(
									aggDataVersionSeqId,
									currDataVersion
							);
							return;
						}

						// 保留数据版本号最小
						final long count = seqIdVersionMap.values().stream().filter(preDataVersion -> preDataVersion <= currDataVersion).count();
						if (count == 0) {
							seqIdVersionMap.clear();
							seqIdVersionMap.put(
									aggDataVersionSeqId,
									currDataVersion
							);
						}
					}
				}
			}
		});
		return seqIdVersionMap;
	}

	private void cleanTargetTable(String collectionName, String connectionId) throws UnsupportedEncodingException {
		Query query = new Query(where("_id").is(connectionId));
		query.fields().exclude("schema");
		List<Connections> connections = taskContext.getClientMongoOperator().find(
				query,
				ConnectorConstant.CONNECTION_COLLECTION,
				Connections.class
		);

		if (CollectionUtils.isNotEmpty(connections)) {
			final Connections conn = connections.get(0);
			if (DatabaseTypeEnum.MONGODB == DatabaseTypeEnum.fromString(conn.getDatabase_type())
					|| DatabaseTypeEnum.ALIYUN_MONGODB == DatabaseTypeEnum.fromString(conn.getDatabase_type())
			) {
				try (final MongoClient mongoClient = MongodbUtil.createMongoClient(conn)) {
					final String database = MongodbUtil.getDatabase(conn);
					final MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
					mongoDatabase.getCollection(collectionName).deleteMany(new Document());
				}
			}
		}
	}
}
