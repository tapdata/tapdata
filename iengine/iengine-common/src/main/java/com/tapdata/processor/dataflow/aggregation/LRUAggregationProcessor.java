package com.tapdata.processor.dataflow.aggregation;

import com.mongodb.client.MongoCursor;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.constant.TapdataOffset;
import com.tapdata.entity.DataQualityTag;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.dataflow.Aggregation;
import com.tapdata.entity.dataflow.Aggregation.AggregationFunction;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.MongoDBScriptConnectionHandler;
import com.tapdata.processor.ProcessorException;
import com.tapdata.processor.dataflow.DataFlowProcessor;
import com.tapdata.processor.dataflow.ProcessorContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * LRU优化聚合处理器，
 * 将聚合中间结果缓存到内存中，实现增量聚合统计，提升计算的性能、
 * 当缓存中间结果超过配置size时，会将超出的记录持久化到MongoDB中
 *
 * @author jackin
 */
public class LRUAggregationProcessor implements DataFlowProcessor {

	public static final String AGG_RET_TAP_SUB_NAME_FIELD = "_tapd8_sub_name";

	public static final String AGG_DATA_VERSION_SEQ_FIELD = "aggDataVersionSeq";

	public static final String AGG_DATA_VERSION_SEQ_ID_FIELD = "id";

	public static final String AGG_DATA_VERSION_SEQ_TYPE_FIELD = "type";

	/**
	 * 聚合数据版本号，全局自增
	 */
	public static final String AGG_DATA_VERSION_SEQ_TYPE = "AGG_DATA_VERSION_SEQ";

	/**
	 * 存放全局每个聚合节点当前获取到的最新的版本号
	 */
	public static final String STAGE_AGG_DATA_VERSION_TYPE = "STAGE_AGG_DATA_VERSION";

	private Logger logger = LogManager.getLogger(LRUAggregationProcessor.class);

	private Stage stage;

	private List<AggregationConfig> aggregationConfig;

	private MongoDBScriptConnectionHandler targetScriptConnection;

	/**
	 * 缓存聚合中间结果表数据
	 */
	private String cacheCollectionName;

	private String dataVersionSeqCollection = ConnectorConstant.DATA_VERSION_SEQ;

	private ProcessorContext context;


	/**
	 * key: table name
	 * value:
	 * -- key: aggregation sub name
	 * -- value:
	 * -- key: pks value
	 * -- value: record
	 */
	private Map<String, Map<String, Map<Object, Map<String, Object>>>> data;

	/**
	 * 聚合缓存的最大的数据量
	 */
	private int aggCacheMaxSize = 200000;

//	private Lock lock = new ReentrantLock();

	private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.stage = stage;
		this.context = context;

		this.targetScriptConnection = (MongoDBScriptConnectionHandler) context.getTargetScriptConnection();
		this.aggregationConfig = new ArrayList<>();
		List<Aggregation> aggregations = stage.getAggregations();
		if (CollectionUtils.isEmpty(aggregations)) {
			throw new ProcessorException(
					"Aggregation processor config cannot not be empty",
					true
			);
		}

		this.cacheCollectionName = cacheCollectionName();

		// 开始前清空目标已存在的中间结果数据，为了避免任务被异常中断（比如：failover），任务重启时目标还存在中间结果数据
		cleanCacheCollection();

		createCollectionIndex();

		for (Aggregation aggregation : aggregations) {
			aggregation.setJsEngineName(stage.getJsEngineName());
			aggregationConfig.add(new AggregationConfig(
					aggregation
			));

		}

		executorService.scheduleWithFixedDelay(() -> {
			try {
				Thread.currentThread().setName("LRU aggregation processor -[" + context.getJob().getId() + "]-" + context.getJob().getName());
				Log4jUtil.setThreadContext(context.getJob());
//				lock.lock();
				logger.info("Starting initial aggregate for stage {}, stageId {}.", stage.getName(), stage.getId());
				initialAggregate();
				logger.info("Completed initial aggregate for stage {}, stageId {}.", stage.getName(), stage.getId());
			} catch (InterruptedException e) {
				// nothing to do
			} catch (Exception e) {
				throw new ProcessorException(
						String.format(
								"Aggregate processor %s, initial aggregate failed %s",
								stage,
								e.getMessage()
						),
						e,
						true
				);
			}
		}, 5000, 5000, TimeUnit.MILLISECONDS);
	}

	/**
	 * 清空目标库的缓存表，启动和停止任务前都会执行
	 */
	public void cleanCacheCollection() {
		boolean collectionExists = targetScriptConnection.tableExists(cacheCollectionName);
		if (collectionExists) {
			logger.info("Starting drop aggregation {}'s cache collection {}.", stage.getName(), cacheCollectionName);
			// 清空缓存聚合中间结果目标表
			targetScriptConnection.collection(cacheCollectionName).drop();
			logger.info("Completed drop aggregation {}'s cache collection {}.", stage.getName(), cacheCollectionName);
		}
	}

	private void createCollectionIndex() {
		// 创建缓存表索引
		targetScriptConnection.collection(cacheCollectionName).createIndex(
				new Document("cacheType", 1)
						.append("cacheId", 1)
		);

		// 创建数据版本集合表的所以
		targetScriptConnection.collection(dataVersionSeqCollection).createIndex(
				new Document(AGG_DATA_VERSION_SEQ_TYPE_FIELD, 1)
						.append(AGG_DATA_VERSION_SEQ_ID_FIELD, 1)
		);

		// 创建节点版本字段索引
		final List<Stage> stages = context.getJob().getStages();
		if (CollectionUtils.isNotEmpty(stages)) {
			final List<Stage> targetDataStages = DataFlowStageUtil.findTargetDataStageByStageId(stage.getId(), stages);
			for (Stage targetDataStage : targetDataStages) {
				final String tableName = targetDataStage.getTableName();
				targetScriptConnection.collection(tableName).createIndex(
						new Document(
								DataQualityTag.SUB_COLUMN_NAME + "." + getAggDataVersionSeqId(context.getJob().getDataFlowId(), stage.getId()),
								1
						)
				);
			}
		}
	}

	@Override
	public Stage getStage() {
		return stage;
	}

	/**
	 * 聚合处理器入口方法
	 * 执行步骤：
	 * 1）将事件消息转换成可聚合计算的items
	 * 2）根据聚合计算的items，批量去目标库中查询不在缓存中的中间结果。并添加到缓存中
	 * 3）根据聚合items执行聚合统计
	 *
	 * @param msgs
	 * @return
	 */
	@Override
	public List<MessageEntity> process(List<MessageEntity> msgs) {

		List<MessageEntity> aggregateData = new ArrayList<>();
		String sourceStageId = null;
		if (CollectionUtils.isNotEmpty(msgs)) {

			for (MessageEntity msg : msgs) {
				try {

					sourceStageId = msg.getSourceStageId();

					// 只处理消息包含before或after的消息
					if (MapUtils.isNotEmpty(msg.getAfter()) || MapUtils.isNotEmpty(msg.getBefore())) {
						String op = msg.getOp();

						if (StringUtils.equalsAny(
								op,
								ConnectorConstant.MESSAGE_OPERATION_INSERT,
								ConnectorConstant.MESSAGE_OPERATION_ABSOLUTE_INSERT,
								ConnectorConstant.MESSAGE_OPERATION_UPDATE,
								ConnectorConstant.MESSAGE_OPERATION_DELETE
						)) {

							String tableName = msg.getTableName();
							Map<String, Object> value = msg.getAfter();
							if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op)) {
								value = msg.getBefore();
							}
							if (MapUtils.isNotEmpty(value)) {
								// LRU方式 缓存所有数据到内存中，用于定时全量聚合计算
								putData2MongoDBLRUCacheMap(msg, op, tableName, value, sourceStageId);
							}

						}
					}
				} catch (Exception e) {
					throw new ProcessorException(
							String.format(
									"Aggregate record %s failed %s.",
									msg,
									e.getMessage()
							),
							e,
							true
					);
				}
			}

		}

		return CollectionUtils.isNotEmpty(aggregateData) ? aggregateData : null;
	}

	/**
	 * 聚合计算方法，将源端输入的一条消息，根据其参与聚合计算的个数，转成多个聚合计算项
	 * eg: 聚合处理器中配置了5个聚合统计函数，改方法会输出5个聚合计算项
	 * 执行逻辑：
	 * 1) 按js filter 过滤
	 * 2）缓存中不存在聚合中间结果的id添加到数组中，用于后面进行批量从MongoDB中查询
	 *
	 * @param sourceStageId
	 * @param after
	 * @param before
	 * @param op
	 * @param tableName
	 * @return
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	private List<AggregationItem> aggregateItems(
			String sourceStageId,
			Map<String, Object> after,
			Map<String, Object> before,
			String op,
			String tableName
	) throws Exception {

		List<AggregationItem> aggregationItems = new ArrayList<>();
		for (AggregationConfig aggregationRuntimeBundle : aggregationConfig) {

			boolean keepOnGoing = aggregationFilter(
					after,
					before,
					op,
					aggregationRuntimeBundle.getFilterEval()
			);

			if (keepOnGoing) {
				Aggregation aggregation = aggregationRuntimeBundle.getAggregation();

				Object aggId = getAggregationId(after, aggregation);
				Object beforeAggId = null;
				if (MapUtils.isNotEmpty(before)) {
					beforeAggId = getAggregationId(before, aggregation);
				}

				if (beforeAggId == null || aggId.equals(beforeAggId)) {
					Map<String, Object> calculatedRecord = convert2CalculateRecord(op, after, before, aggregation);
					AggregationItem aggregationItem = new AggregationItem(
							calculatedRecord,
							aggId,
							aggregation,
							op,
							sourceStageId,
							tableName
					);
					aggregationItems.add(aggregationItem);
				} else {

					// 删除以前的分组统计
					Map<String, Object> deletedCal = convert2CalculateRecord(
							ConnectorConstant.MESSAGE_OPERATION_DELETE,
							null,
							before,
							aggregation
					);
					AggregationItem delAggItem = new AggregationItem(
							deletedCal,
							beforeAggId,
							aggregation,
							ConnectorConstant.MESSAGE_OPERATION_DELETE,
							sourceStageId,
							tableName
					);
					aggregationItems.add(delAggItem);

					// 新增新的分组统计
					Map<String, Object> insertCal = convert2CalculateRecord(
							ConnectorConstant.MESSAGE_OPERATION_INSERT,
							after,
							null,
							aggregation
					);
					AggregationItem insertAggItem = new AggregationItem(
							insertCal,
							aggId,
							aggregation,
							ConnectorConstant.MESSAGE_OPERATION_INSERT,
							sourceStageId,
							tableName
					);
					aggregationItems.add(insertAggItem);
				}

			}

		}

		return aggregationItems;
	}

	private void initialAggregate() throws Exception {
		List<AggregationItem> aggregationItems = new ArrayList<>();
		PersistentLRUMap mongoDBLRUAggregateResultMap = new PersistentLRUMap(
				Math.max(
						context.getJob().getReadBatchSize(),
						aggCacheMaxSize
				),
				this::onRemoveLRU
		);

		Map<String, Map<String, Map<Object, Map<String, Object>>>> data = new HashMap<>();
		MongoCursor<Document> cursor = null;
		final long dataVersionSeq = getDataVersionSeq();
		try {
			cursor = targetScriptConnection.collection(cacheCollectionName).find(
					new Document(
							cacheFilter(null, CacheType.SOURCE_DATA)
					)
			).cursor();

			while (cursor.hasNext() && ConnectorConstant.RUNNING.equals(context.getJob().getStatus())) {
				final Document document = cursor.next();
				final Document dataPayload = document.get("data", Document.class);
				final Document cacheId = document.get("cacheId", Document.class);
				if (MapUtils.isNotEmpty(cacheId)) {
					final String sourceTableName = (String) cacheId.get("sourceTable");
					final String sourceStageId = (String) cacheId.get("sourceStageId");
					aggregationItems.addAll(
							// 将消息转换成可聚合计算的item
							aggregateItems(
									sourceStageId,
									dataPayload,
									null,
									OperationType.INSERT.getOp(),
									sourceTableName
							)
					);
				}

				if (aggregationItems.size() % context.getJob().getReadBatchSize() == 0) {
					// 执行聚合计算
					aggregate(aggregationItems, mongoDBLRUAggregateResultMap, data);

					aggregationItems.clear();

				}
			}

			if (!ConnectorConstant.RUNNING.equals(context.getJob().getStatus())) {
				return;
			}

			if (CollectionUtils.isNotEmpty(aggregationItems)) {

				// 执行聚合计算
				aggregate(aggregationItems, mongoDBLRUAggregateResultMap, data);

				aggregationItems.clear();

			}

			List<MessageEntity> aggregatedMsgs = new ArrayList<>();
			for (Object object : mongoDBLRUAggregateResultMap.entrySet()) {
				Map.Entry entry = (Map.Entry) object;
				final Document key = (Document) entry.getKey();
				final String sourceTable = key.get("sourceTable", String.class);
				final String sourceStageId = key.get("sourceStageId", String.class);
				final Map aggValue = (Map) entry.getValue();

				MessageEntity messageEntity = new MessageEntity(
						ConnectorConstant.MESSAGE_OPERATION_INSERT,
						aggValue,
						sourceTable
				);
				messageEntity.setSourceStageId(sourceStageId);
				messageEntity.setProcessorStageId(stage.getId());
				messageEntity.setOffset(new TapdataOffset(TapdataOffset.SYNC_STAGE_CDC, null));

				Map<String, Object> tapd8MetaData = new HashMap<>();
				tapd8MetaData.put(
						getAggDataVersionSeqId(context.getJob().getDataFlowId(), stage.getId()),
						dataVersionSeq
				);
				messageEntity.setTapd8MetaData(tapd8MetaData);
				aggregatedMsgs.add(messageEntity);

				if (aggregatedMsgs.size() % context.getJob().getReadBatchSize() == 0) {
					context.getProcessorHandle().accept(aggregatedMsgs);
					aggregatedMsgs.clear();
				}
			}

			if (CollectionUtils.isNotEmpty(aggregatedMsgs)) {
				context.getProcessorHandle().accept(aggregatedMsgs);
			}
		} finally {
			MongodbUtil.releaseConnection(null, cursor);
		}

	}

	private boolean aggregationFilter(Map<String, Object> after, Map<String, Object> before, String op, FilterEval filterEval) {
		boolean keepOnGoing = true;
		if (filterEval != null) {
			switch (op) {
				case ConnectorConstant.MESSAGE_OPERATION_ABSOLUTE_INSERT:
				case ConnectorConstant.MESSAGE_OPERATION_INSERT:
				case ConnectorConstant.MESSAGE_OPERATION_UPDATE:
					keepOnGoing = filterEval.filter(after);
					break;
				case ConnectorConstant.MESSAGE_OPERATION_DELETE:
					keepOnGoing = filterEval.filter(before);
					break;
			}
		}
		return keepOnGoing;
	}

	/**
	 * 获取聚合计算的_id，规则：
	 * 1）分组统计：{
	 * _tapd8_sub_name: <聚合计算的子名称>
	 * <分组字段1>: <分组字段1的值>,
	 * <分组字段2>: <分组字段2的值>,
	 * ...
	 * }
	 * 2）不需要分组：聚合计算的子名称
	 *
	 * @param value
	 * @param aggregation
	 * @return
	 */
	private Object getAggregationId(Map<String, Object> value, Aggregation aggregation) {
		Object aggId = aggregation.getName();
		List<String> groupByExpression = aggregation.getGroupByExpression();
		if (CollectionUtils.isNotEmpty(groupByExpression)) {
			Map<String, Object> aggRetId = new LinkedHashMap<>();
			aggRetId.put(AGG_RET_TAP_SUB_NAME_FIELD, aggregation.getName());
			for (String groupFieldName : groupByExpression) {
				Object valueByKey = MapUtil.getValueByKey(value, groupFieldName);

				aggRetId.put(groupFieldName, valueByKey);
			}

			aggId = aggRetId;
		}
		return aggId;
	}

	/**
	 * 根据聚合计算item，执行聚合计算，然后转成消息事件
	 *
	 * @param aggregationItems
	 * @return
	 */
	private List<MessageEntity> aggregate(List<AggregationItem> aggregationItems, PersistentLRUMap mongoDBLRUAggregateResultMap, Map<String, Map<String, Map<Object, Map<String, Object>>>> data) {

		List<MessageEntity> msgs = new ArrayList<>();

		for (AggregationItem aggregationItem : aggregationItems) {

			final Aggregation aggregation = aggregationItem.getAggregation();
			final String tableName = aggregationItem.getTableName();

			Object aggId = aggregationItem.getAggId();
			Map<String, Object> calculatedRecord = aggregationItem.getCalculatedRecord();
			String op = aggregationItem.getMessageOp();
			String sourceStageId = aggregationItem.getMessageSourceStageId();

			Map<String, Object> aggMidResult = (Map<String, Object>) mongoDBLRUAggregateResultMap.get(new Document(
					"sourceTable", tableName
			).append("sourceStageId", sourceStageId).append("pk", aggId));
			aggMidResult = aggregatorProcess(
					aggMidResult,
					calculatedRecord,
					aggregation,
					op
			);

			if (!aggMidResult.containsKey("_id")) {
				aggMidResult.put("_id", aggId);
			}

//				mongoDBLRUAggregateResultMap.put(aggId, aggMidResult);

			if (MapUtil.containsKey(aggMidResult, aggregation.getAggFunction())) {
				Map<String, Object> aggValue = new HashMap<>();
				aggValue.put("_id", aggId);
				Object result = MapUtil.getValueByKey(aggMidResult, aggregation.getAggFunction());
				aggValue.put(
						aggregation.getAggFunction(),
						result
				);

				List<String> groupByExpression = aggregation.getGroupByExpression();
				if (CollectionUtils.isNotEmpty(groupByExpression)) {
					for (String groupFieldName : groupByExpression) {
						Object groupFieldValue = MapUtil.getValueByKey(calculatedRecord, groupFieldName);
						aggValue.put(groupFieldName, groupFieldValue);
					}
				}

				mongoDBLRUAggregateResultMap.put(new Document(
						"sourceTable", tableName
				).append("sourceStageId", sourceStageId)
						.append("pk", aggId), aggValue);

			}
		}

		return msgs;
	}

	/**
	 * 根据事件类型，将记录转换成可参与聚合计算的记录
	 * 1) update：AVG、SUM计算，根据参与计算的字段，计算出更新后和更新前之间差值
	 * 2）delete: AVG、SUM计算，参与计算的字段的值是 0 - <删除前字段的值>
	 *
	 * @param op
	 * @param after
	 * @param before
	 * @param aggregation
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private Map<String, Object> convert2CalculateRecord(String op, Map<String, Object> after, Map<String, Object> before, Aggregation aggregation) throws Exception {

		Map<String, Object> calculatedRecord = new HashMap<>();

		if (StringUtils.equalsAny(
				op,
				ConnectorConstant.MESSAGE_OPERATION_ABSOLUTE_INSERT,
				ConnectorConstant.MESSAGE_OPERATION_INSERT
		)) {
			MapUtil.deepCloneMap(after, calculatedRecord);
		} else if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op) || ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(op)) {

			if (MapUtils.isEmpty(before) && ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op)) {
				throw new ProcessorException(
						"delete message before cannot be empty.",
						true
				);
			}

			if (MapUtils.isEmpty(after) && ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(op)) {
				throw new ProcessorException(
						"update message after cannot be empty.",
						true
				);
			}

			AggregationFunction aggregationFunction = AggregationFunction.getByFunctionName(aggregation.getAggFunction());
			switch (aggregationFunction) {
				case AVG:
				case SUM:

					String aggExpression = aggregation.getAggExpression();

					double afterValue = 0D;
					if (ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(op)) {

						if (MapUtils.isEmpty(before)) {
							throw new ProcessorException(
									"update message before cannot be empty when use sum/avg aggregate.",
									true
							);
						}
						afterValue = getDoubleValue(aggExpression, after);
						MapUtil.deepCloneMap(after, calculatedRecord);
					} else {
						MapUtil.deepCloneMap(before, calculatedRecord);
					}

					double beforeValue = getDoubleValue(aggExpression, before);

					MapUtil.putValueInMap(calculatedRecord, aggExpression, afterValue - beforeValue);

					break;
				default:

					if (ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(op)) {
						MapUtil.deepCloneMap(after, calculatedRecord);
					} else {
						MapUtil.deepCloneMap(before, calculatedRecord);
					}

					break;
			}
		}

		return calculatedRecord;
	}

	/**
	 * 执行聚合计算
	 *
	 * @param aggMidResult    聚合计算的中间结果
	 * @param calculateRecord 参与计算的记录
	 * @return
	 */
	private Map<String, Object> aggregatorProcess(
			Map<String, Object> aggMidResult,
			Map<String, Object> calculateRecord,
			Aggregation aggregation,
			String messageOp
	) {

		if (aggMidResult == null) {
			aggMidResult = new HashMap<>();
		}

		AggregationFunction functionName = AggregationFunction.getByFunctionName(aggregation.getAggFunction().toUpperCase());
		String aggExpression = aggregation.getAggExpression();
		switch (functionName) {
			case AVG:
				double avgSum = 0D;
				double avgCount = 0D;
				if (MapUtils.isNotEmpty(aggMidResult)) {
					avgSum = (double) aggMidResult.getOrDefault(AggregationFunction.SUM.getFunctionName(), 0D);
					avgCount = (double) aggMidResult.getOrDefault(AggregationFunction.COUNT.getFunctionName(), 0D);
				}
				double avgCalculateValue = getDoubleValue(aggExpression, calculateRecord);
				avgSum += avgCalculateValue;
				if (ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageOp)) {
					avgCount++;
				} else if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageOp)) {
					avgCount--;
				}

				if (avgCount > 0) {
					double avg = BigDecimal.valueOf(avgSum).divide(new BigDecimal(avgCount), 2, BigDecimal.ROUND_CEILING).doubleValue();
					aggMidResult.put(AggregationFunction.AVG.getFunctionName(), avg);
					aggMidResult.put(AggregationFunction.SUM.getFunctionName(), avgSum);
					aggMidResult.put(AggregationFunction.COUNT.getFunctionName(), avgCount);
				} else {
					aggMidResult.put(AggregationFunction.AVG.getFunctionName(), 0D);
					aggMidResult.put(AggregationFunction.SUM.getFunctionName(), 0D);
					aggMidResult.put(AggregationFunction.COUNT.getFunctionName(), 0D);
				}
				break;
			case SUM:
				double sum = 0D;
				if (MapUtils.isNotEmpty(aggMidResult)) {
					sum = (double) aggMidResult.getOrDefault(AggregationFunction.SUM.getFunctionName(), 0D);
				}
				double sumCalculateValue = getDoubleValue(aggExpression, calculateRecord);
				sum += sumCalculateValue;
				aggMidResult.put(AggregationFunction.SUM.getFunctionName(), sum);
				break;
			case MAX:

				Object maxCalculateValue = MapUtil.getValueByKey(calculateRecord, aggExpression);
				if (maxCalculateValue == null) {
					break;
				}

				if (!(maxCalculateValue instanceof Comparable)) {
					throw new ProcessorException(
							String.format(
									"get max value failed, key %s value %s does not comparable.",
									aggExpression,
									maxCalculateValue),
							true
					);
				}

				Object maxValue = aggMidResult.get(AggregationFunction.MAX.getFunctionName());
				if (maxValue == null) {
					aggMidResult.put(AggregationFunction.MAX.getFunctionName(), maxCalculateValue);
					break;
				}

				maxValue = ((Comparable) maxValue).compareTo(maxCalculateValue) == -1 ? maxCalculateValue : maxValue;
				aggMidResult.put(AggregationFunction.MAX.getFunctionName(), maxValue);

				break;
			case MIN:
				Object minCalculateValue = MapUtil.getValueByKey(calculateRecord, aggExpression);

				Object minValue = aggMidResult.get(AggregationFunction.MIN.getFunctionName());
				if (minValue == null) {
					aggMidResult.put(AggregationFunction.MIN.getFunctionName(), minCalculateValue);
					break;
				}

				if (minCalculateValue == null) {
					break;
				}

				if (minCalculateValue instanceof Comparable && minValue instanceof Comparable) {
					minValue = ((Comparable) minValue).compareTo(minCalculateValue) == 1 ? minCalculateValue : minValue;
					aggMidResult.put(AggregationFunction.MIN.getFunctionName(), minValue);

					break;
				}
				break;
			case COUNT:
				long countValue = getLongValue(AggregationFunction.COUNT.getFunctionName(), aggMidResult);

				if (StringUtils.equalsAny(messageOp,
						ConnectorConstant.MESSAGE_OPERATION_INSERT,
						ConnectorConstant.MESSAGE_OPERATION_ABSOLUTE_INSERT)) {
					countValue++;
				} else if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageOp)) {
					countValue--;
				}

				aggMidResult.put(AggregationFunction.COUNT.getFunctionName(), countValue > 0 ? countValue : 0);
				break;
			default:
				break;
		}

		return aggMidResult;
	}

	private String cacheCollectionName() {
		return context.getJob().getDataFlowId() + "_" + stage.getId() + ConnectorConstant.LOOKUP_TABLE_AGG_SUFFIX;
	}

	private double getDoubleValue(String key, Map<String, Object> record) {
		Object valueByKey = MapUtil.getValueByKey(record, key);
		if (valueByKey == null) {
			return 0D;
		}
		try {
			return Double.parseDouble(valueByKey.toString());
		} catch (NumberFormatException e) {
			return 0D;
		}
	}

	private long getLongValue(String key, Map<String, Object> record) {
		Object valueByKey = MapUtil.getValueByKey(record, key);
		if (valueByKey == null) {
			return 0L;
		}
		try {
			return Long.parseLong(valueByKey.toString());
		} catch (NumberFormatException e) {
			return 0L;
		}
	}

	public void onRemoveLRU(Map.Entry entry) {
		this.targetScriptConnection.execute(
				new Document("filter",
						cacheFilter(
								entry.getKey(),
								CacheType.AGGREGATE_MID_RESULT
						)
				)
						.append("collection", cacheCollectionName)
						.append("op", "update")
						.append("opObject", new Document("$set", new Document("data", entry.getValue())))
						.append("upsert", true)
		);
	}

	@Override
	public void stop() {
		cleanCacheCollection();
		executorService.shutdownNow();
	}

	public static String getAggDataVersionSeqId(String dataFlowId, String stageId) {
		return (dataFlowId + "_" + stageId).intern();
	}

	private long getDataVersionSeq() {
		Document filter = new Document();
		filter.append(AGG_DATA_VERSION_SEQ_TYPE_FIELD, AGG_DATA_VERSION_SEQ_TYPE);
		Document update = new Document();
		update.append("$inc", new Document(AGG_DATA_VERSION_SEQ_FIELD, 1));
		final Map<String, Object> ret = targetScriptConnection.collection(dataVersionSeqCollection).findAndModify(filter, update, true, true);
		if (MapUtils.isEmpty(ret)) {
			throw new ProcessorException(
					String.format("get aggregation %s's data version sequence failed, the seq ret is null.", stage.getName()),
					false
			);
		}
		final long aggVersionSeq = new BigDecimal(ret.get(AGG_DATA_VERSION_SEQ_FIELD).toString()).longValue();
		filter.clear();
		filter.append(AGG_DATA_VERSION_SEQ_TYPE_FIELD, STAGE_AGG_DATA_VERSION_TYPE);
		filter.append(
				AGG_DATA_VERSION_SEQ_ID_FIELD,
				getAggDataVersionSeqId(
						context.getJob().getDataFlowId(),
						stage.getId()
				)
		);

		update.clear();
		update.append("$set",
				new Document(AGG_DATA_VERSION_SEQ_FIELD, aggVersionSeq)
						.append("jobName", context.getJob().getName())
						.append("stageName", stage.getName())
		);

		targetScriptConnection.collection(dataVersionSeqCollection).updateOne(filter, update, true);

		return aggVersionSeq;
	}

	private void putData2MongoDBLRUCacheMap(MessageEntity msg, String op, String tableName, Map<String, Object> value, String sourceStageId) {
		try {

			Map<String, Object> pkObject = new LinkedHashMap<>();
			String primaryKeys = stage.getPrimaryKeys();
			if (StringUtils.isNotBlank(primaryKeys)) {
				String[] split = primaryKeys.split(",");
				for (String pk : split) {
					pkObject.put(pk, MapUtil.getValueByKey(value, pk));
				}
			}
			if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op)) {

				this.targetScriptConnection.execute(
						new Document("filter",
								cacheFilter(
										new Document("pk", pkObject)
												.append("sourceTable", tableName)
												.append("sourceStageId", sourceStageId),
										CacheType.SOURCE_DATA
								)
						)
								.append("collection", cacheCollectionName)
								.append("op", "delete")
				);
			} else {

				this.targetScriptConnection.execute(
						new Document("filter",
								cacheFilter(
										new Document("pk", pkObject)
												.append("sourceTable", tableName)
												.append("sourceStageId", sourceStageId),
										CacheType.SOURCE_DATA
								)
						)
								.append("collection", cacheCollectionName)
								.append("op", "update")
								.append("opObject", new Document("$set", new Document("data", value)))
								.append("upsert", true)
				);
			}

		} catch (Exception e) {
			if (context.getJob().getStopOnError()) {
				throw new RuntimeException("Aggregate " + msg + " failed " + e.getMessage() + ", stop on error is true, will stop replicator.", e);
			}
			e.printStackTrace();
			logger.warn("Aggregate {} failed {}, will skip it.", msg, e.getMessage());
		}
	}

	private void putData2MinMaxCacheMap(MessageEntity msg, String op, String tableName, Map<String, Object> value) {
		try {
			if (!data.containsKey(tableName)) {
				data.put(tableName, new HashMap<>());
			}

			Map<String, Object> pkObject = new HashMap<>();
			String primaryKeys = stage.getPrimaryKeys();
			if (StringUtils.isNotBlank(primaryKeys)) {
				String[] split = primaryKeys.split(",");
				for (String pk : split) {
					pkObject.put(pk, MapUtil.getValueByKey(value, pk));
				}
			}
			Map<String, Map<Object, Map<String, Object>>> aggregationCache = data.get(tableName);
			for (AggregationConfig aggregationConfig : aggregationConfig) {
				Aggregation aggregation = aggregationConfig.getAggregation();
				final boolean minMaxAgg = isMinMaxAgg(aggregation);
				if (!minMaxAgg) {
					continue;
				}

				boolean keepOnGoing = aggregationFilter(
						msg.getAfter(),
						msg.getBefore(),
						op,
						aggregationConfig.getFilterEval()
				);
				if (keepOnGoing) {
					String aggExpression = aggregation.getAggExpression();
					Object valueByKey = MapUtil.getValueByKey(value, aggExpression);
					if (valueByKey == null && !(valueByKey instanceof Comparable)) {
						continue;
					}

					String subName = aggregation.getName();

					if (!aggregationCache.containsKey(subName)) {
						aggregationCache.put(subName, new HashMap<>());
					}

					if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op)) {

						aggregationCache.get(subName).remove(pkObject);

					} else {

						Map<String, Object> cacheValue = new HashMap<>();
						if (StringUtils.isNotBlank(primaryKeys)) {
							String[] split = primaryKeys.split(",");
							for (String pk : split) {
								cacheValue.put(pk, MapUtil.getValueByKey(value, pk));
							}
						}
						cacheValue.put(aggExpression, valueByKey);
						List<String> groupByExpressions = aggregation.getGroupByExpression();
						for (String groupByExpression : groupByExpressions) {
							cacheValue.put(groupByExpression, MapUtil.getValueByKey(value, groupByExpression));
						}

						aggregationCache.get(subName).put(pkObject, cacheValue);
					}

				}

			}

		} catch (Exception e) {
			if (context.getJob().getStopOnError()) {
				throw new RuntimeException("Aggregate " + msg + " failed " + e.getMessage() + ", stop on error is true, will stop replicator.", e);
			}
			e.printStackTrace();
			logger.warn("Aggregate {} failed {}, will skip it.", msg, e.getMessage());
		}
	}

	private boolean isMinMaxAgg(Aggregation aggregation) {
		if (aggregation == null) {
			return false;
		}
		final AggregationFunction aggregationFunction = AggregationFunction.getByFunctionName(aggregation.getAggFunction());
		return AggregationFunction.MAX == aggregationFunction || AggregationFunction.MIN == aggregationFunction;
	}

	private Map<String, Object> cacheFilter(Object cacheId, CacheType cacheType) {
		final Document cacheFilter = new Document();
		if (cacheId != null) {
			cacheFilter.append("cacheId", cacheId);
		}

		cacheFilter.append("cacheType", cacheType.getCacheType());

		return cacheFilter;
	}

	enum CacheType {
		AGGREGATE_MID_RESULT("AGG_MID_RESULT"),
		SOURCE_DATA("SOURCE_DATA");

		private String cacheType;

		CacheType(String cacheType) {
			this.cacheType = cacheType;
		}

		public String getCacheType() {
			return cacheType;
		}
	}

	/**
	 * 聚合计算项，用于批量处理聚合计算，为了提升性能，减少查询MongoDB操作
	 */
	class AggregationItem {

		private Map<String, Object> calculatedRecord;

		private Object aggId;

		private Aggregation aggregation;

		private String messageOp;

		private String messageSourceStageId;

		private String tableName;

		public AggregationItem(
				Map<String, Object> calculatedRecord,
				Object aggId,
				Aggregation aggregation,
				String messageOp,
				String messageSourceStageId,
				String tableName
		) {
			this.calculatedRecord = calculatedRecord;
			this.aggId = aggId;
			this.aggregation = aggregation;
			this.messageOp = messageOp;
			this.messageSourceStageId = messageSourceStageId;
			this.tableName = tableName;
		}

		public Map<String, Object> getCalculatedRecord() {
			return calculatedRecord;
		}

		public Object getAggId() {
			return aggId;
		}

		public Aggregation getAggregation() {
			return aggregation;
		}

		public String getMessageOp() {
			return messageOp;
		}

		public String getMessageSourceStageId() {
			return messageSourceStageId;
		}

		public String getTableName() {
			return tableName;
		}

	}

	public static void main(String[] args) throws Exception {
//		Stage config = new Stage();
//		config.setAggregations(Arrays.asList(new Aggregation(
//			"record.age >= 23 && record.sex == '男'",
//			"COUNT",
//			"age",
//			Arrays.asList("sex", "name")
//		)));
//
////		LRUAggregationProcessor processor = new LRUAggregationProcessor();
////		processor.initialize(null, config);
//
//		Map<String, Object> record1 = new HashMap<>();
//		record1.put("age", 25);
//		record1.put("sex", "男");
//		record1.put("name", "n1");
//		record1.put("date", new Date());
//
//		Thread.sleep(1000);
//		Map<String, Object> record2 = new HashMap<>();
//		record2.put("age", 23);
//		record2.put("sex", "男");
//		record2.put("name", "n1");
//		record2.put("date", new Date());
//		Map<String, Object> record3 = new HashMap<>();
//		record3.put("age", 35);
//		record3.put("sex", "男");
//		record3.put("name", "n2");
//		record3.put("date", new Date());
//		Map<String, Object> record4 = new HashMap<>();
//		record4.put("age", 35);
//		record4.put("sex", "女");
//		record4.put("name", "w1");
//		record4.put("date", new Date(System.currentTimeMillis() - 20000));
//		Map<String, Object> record5 = new HashMap<>();
////    record5.put("age", null);
//		record5.put("sex", "女");
//		record5.put("name", "w1");
//		record5.put("date", new Date());
//
//		final List<Map<String, Object>> list = Arrays.asList(record1, record2, record3, record4, record5);
//		final Comparator<Map<String, Object>> comp = (r1, r2) -> {
//			final Object value1 = MapUtil.getValueByKey(r1, "date");
//			final Object value2 = MapUtil.getValueByKey(r2, "date");
//
//			return ((Comparable) value1).compareTo(value2);
//		};
//
//		Map<String, Object> minRecord = list.stream()
//			.min(comp)
//			.get();
//
//		System.out.println(record1);
//		System.out.println(record5);
//		System.out.println(minRecord);
//		List<MessageEntity> messageEntities = Arrays.asList(new MessageEntity(
//			"i", record1, "Persons"
//		), new MessageEntity(
//			"i", record2, "Persons"
//		), new MessageEntity(
//			"i", record3, "Persons"
//		), new MessageEntity(
//			"i", record4, "Persons"
//		));
//
//		List<MessageEntity> process = processor.process(messageEntities);
//
//		for (MessageEntity messageEntity : process) {
//			System.out.println(messageEntity);
//		}

		long ret = Long.MAX_VALUE / 86400;
		System.out.println(NumberUtils.isDigits("09900001"));
	}
}
