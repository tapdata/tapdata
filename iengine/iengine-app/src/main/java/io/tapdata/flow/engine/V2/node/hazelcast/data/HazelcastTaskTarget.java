package io.tapdata.flow.engine.V2.node.hazelcast.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MessageUtil;
import com.tapdata.constant.OffsetUtil;
import com.tapdata.constant.ReflectUtil;
import com.tapdata.constant.TapdataOffset;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.Stats;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.SyncStageEnum;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.Target;
import io.tapdata.common.ClassScanner;
import io.tapdata.entity.OnData;
import io.tapdata.entity.TargetContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.schema.SchemaList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * 负责对接hazelcast的Target节点
 *
 * @author jackin
 * @date 2021/3/10 9:17 PM
 **/
public class HazelcastTaskTarget extends HazelcastBaseNode {

	private static final Logger logger = LogManager.getLogger(HazelcastTaskTarget.class);

	private final static String TARGET_EXTENT_CLASS_NAME = "io.tapdata.TargetExtend";

	private Target target = null;

	private List<String> baseURLs;

	private int retryTime;

	private TargetContext targetContext;

	private Map<String, String> syncProgressMap = new ConcurrentHashMap<>();

	private MergeTableNode mergeTableNode;

	/**
	 * only for data migration
	 * key: source table
	 * value: target table
	 */
	private Map<String, String> sourceTargetTableMap = new HashMap<>();

	protected DataProcessorContext dataProcessorContext;

	public HazelcastTaskTarget(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.dataProcessorContext = dataProcessorContext;
		baseURLs = (List<String>) dataProcessorContext.getConfigurationCenter().getConfig(ConfigurationCenter.BASR_URLS);
		retryTime = (int) dataProcessorContext.getConfigurationCenter().getConfig(ConfigurationCenter.RETRY_TIME);
	}

	public void setMergeTableNode(MergeTableNode mergeTableNode) {
		this.mergeTableNode = mergeTableNode;
	}

	@Override
	protected void doInit(@Nonnull Context context) throws Exception {
		try {
			Thread.currentThread().setName(threadName);
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			Node<?> node = dataProcessorContext.getNode();
			ConfigurationCenter configurationCenter = dataProcessorContext.getConfigurationCenter();
			Log4jUtil.setThreadContext(taskDto);
			this.running = new AtomicBoolean(true);
			super.doInit(context);

			Class<?> targetClazz = ClassScanner.getClazzByDatabaseType(dataProcessorContext.getTargetConn().getDatabase_type(), ClassScanner.TARGET);
			target = (Target) targetClazz.newInstance();
//      MergeTableUtil.mergeTablePropertyFillInMappings(mergeTableNode, targetContext.getJob().getMappings(), dataProcessorContext.getNodes());
			target.targetInit(targetContext);
			targetContext.setSyncStage(TapdataOffset.SYNC_STAGE_SNAPSHOT);
//      initTargetDB();
			targetContext.getJob().setJobErrorNotifier(this::errorHandle);

			if (node instanceof DatabaseNode) {
				DatabaseNode databaseNode = (DatabaseNode) node;
				final List<SyncObjects> syncObjects = databaseNode.getSyncObjects();
				final String tablePrefix = databaseNode.getTablePrefix();
				final String tableSuffix = databaseNode.getTableSuffix();
				final String tableNameTransform = databaseNode.getTableNameTransform();

				for (SyncObjects syncObject : syncObjects) {
					final String type = syncObject.getType();
					if (com.tapdata.entity.dataflow.SyncObjects.TABLE_TYPE.equals(type)) {
						for (String objectName : syncObject.getObjectNames()) {
							String originalObjName = objectName;
							if (StringUtils.isNotBlank(tablePrefix)) {

								objectName = tablePrefix + objectName;
							}
							if (StringUtils.isNotBlank(tableSuffix)) {

								objectName = objectName + tableSuffix;
							}
							objectName = Capitalized.convert(objectName, tableNameTransform);
							sourceTargetTableMap.put(
									originalObjName,
									objectName
							);
						}
					}
				}
			}
		} catch (Exception e) {
			// Milestone-INIT_TRANSFORMER-ERROR
			throw e;
		}
	}

	/**
	 * Implements the boilerplate of dispatching against the ordinal,
	 * taking items from the inbox one by one, and invoking the
	 * processing logic on each.
	 */
	@Override
	public void process(int ordinal, Inbox inbox) {
		try {
			Thread.currentThread().setName(threadName);
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			Node<?> node = dataProcessorContext.getNode();
			Log4jUtil.setThreadContext(taskDto);

			if (!inbox.isEmpty()) {

				SyncProgress syncProgress = new SyncProgress();
				Set<String> syncProgressKey = new LinkedHashSet<>();
				while (running.get()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, targetContext.getJob().getReadBatchSize());
					if (count > 0) {
						List<MessageEntity> msgs = new ArrayList<>();

						MessageEntity messageEntity;
						for (TapdataEvent tapdataEvent : tapdataEvents) {
							if (tapdataEvent.getMessageEntity() != null) {
								messageEntity = tapdataEvent.getMessageEntity();
							} else {
								messageEntity = tapEvent2Message((TapRecordEvent) tapdataEvent.getTapEvent());
							}
							final OperationType operationType = OperationType.fromOp(messageEntity.getOp());
							if (operationType != null && (OperationType.isDdl(operationType) || OperationType.isDml(operationType))) {
								messageEntity.setMapping(createMapping(messageEntity.getTableName()));
							}
							msgs.add(messageEntity);

							syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
							syncProgress.setSourceTime(tapdataEvent.getSourceTime());
							syncProgress.setEventSerialNo(tapdataEvent.getSourceSerialNo());
							if (CollectionUtils.isNotEmpty(tapdataEvent.getNodeIds())) {
								syncProgressKey.add(tapdataEvent.getNodeIds().get(0));
								syncProgressKey.add(node.getId());
							}

							if (tapdataEvent.getSyncStage() == SyncStage.CDC) {
								targetContext.setSyncStage(TapdataOffset.SYNC_STAGE_CDC);
							}
						}

						MessageUtil.dispatcherMessage(
								msgs,
								false,
								processableMessage -> {
									final MessageEntity lastMsg = processableMessage.get(processableMessage.size() - 1);
									processMessage(processableMessage.get(0), processableMessage);
									try {
										syncProgressRecord(lastMsg, syncProgress, syncProgressKey);
									} catch (JsonProcessingException e) {
										String msg = String.format(" tableName: %s, %s", lastMsg.getTableName(), e.getMessage());
										throw new NodeException(msg, e);
									}
								},
								commitMsg -> {
									try {
										syncProgressRecord(commitMsg, syncProgress, syncProgressKey);
									} catch (JsonProcessingException e) {
										String msg = String.format(" tableName: %s, %s", commitMsg.getTableName(), e.getMessage());
										throw new NodeException(msg, e);
									}
								}
						);
					} else {
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("Target process failed {}.", e.getMessage(), e);
			throw e;
		}
	}

	private void syncProgressRecord(MessageEntity lastMsg, SyncProgress syncProgress, Set<String> syncProgressKey) throws JsonProcessingException {
		if (lastMsg != null && lastMsg.getOffset() != null && CollectionUtils.isNotEmpty(syncProgressKey) && syncProgressKey.size() == 2) {
			Object offset = lastMsg.getOffset();
			syncProgress.setOffset(JSONUtil.obj2Json(new TapdataOffset(offset)));
			syncProgress.setEventTime(lastMsg.getTimestamp());
			this.syncProgressMap.put(JSONUtil.obj2Json(syncProgressKey), JSONUtil.obj2Json(syncProgress));
		}
	}

	private Mapping createMapping(String fromTable) {
		Node<?> node = dataProcessorContext.getNode();
		Mapping mapping = new Mapping();
		mapping.setFrom_table(fromTable);
		mapping.setRelationship(ConnectorConstant.RELATIONSHIP_ONE_ONE);
		if (node instanceof TableNode) {
			if (null == mergeTableNode) {
				final List<String> updateConditionFields = ((TableNode) node).getUpdateConditionFields();
				List<Map<String, String>> joinConditions = new ArrayList<>();
				for (String updateConditionField : updateConditionFields) {
					joinConditions.add(new HashMap<String, String>() {{
						put(updateConditionField, updateConditionField);
					}});
				}
				mapping.setJoin_condition(joinConditions);
				mapping.setTo_table(((TableNode) node).getTableName());
				mapping.setFrom_table(fromTable);
			} else {
				mapping = targetContext.getJob().getMappings().stream().filter(m -> m.getFrom_table().equals(fromTable)).findFirst().orElse(null);
			}
		} else if (node instanceof CacheNode) {
			mapping.setTo_table(((CacheNode) node).getCacheName());
		} else if (node instanceof DatabaseNode) {
			final SchemaList relateDataBaseTables = (SchemaList) dataProcessorContext.getTargetConn().getSchema().get("tables");
			final String targetTableName = sourceTargetTableMap.get(fromTable);
			mapping.setTo_table(targetTableName);
			List<String> updateConditionFields = ((DatabaseNode) node).getUpdateConditionFieldMap().get(targetTableName);
			List<RelateDatabaseField> fields = relateDataBaseTables.getFields(targetTableName);

			if (CollectionUtils.isNotEmpty(updateConditionFields)) {
				List<Map<String, String>> joinConditions = new ArrayList<>();
				for (String updateConditionField : updateConditionFields) {
					joinConditions.add(new HashMap<String, String>() {{
						put(updateConditionField, updateConditionField);
					}});
				}
				mapping.setJoin_condition(joinConditions);
			} else if (CollectionUtils.isNotEmpty(fields)) {
				List<RelateDatabaseField> pkFields = fields.stream().filter(field -> field.getPrimary_key_position() > 0)
						.sorted(Comparator.comparing(RelateDatabaseField::getPrimary_key_position))
						.collect(Collectors.toList());
				handleSpecialTargetPkFields(pkFields);
				if (CollectionUtils.isEmpty(pkFields)) {
					pkFields = fields;
				}
				List<Map<String, String>> joinConditions = new ArrayList<>();
				for (RelateDatabaseField pkField : pkFields) {
					joinConditions.add(new HashMap<String, String>() {{
						put(pkField.getField_name(), pkField.getField_name());
					}});
				}

				mapping.setJoin_condition(joinConditions);
			}
		}
		return mapping;
	}

	private List<RelateDatabaseField> handleSpecialTargetPkFields(List<RelateDatabaseField> pkFields) {
		String databaseType = dataProcessorContext.getTargetConn().getDatabase_type();
		DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(databaseType);
		List<String> specialPkFields = new ArrayList<>();
		switch (databaseTypeEnum) {
			case MONGODB:
			case ALIYUN_MONGODB:
				specialPkFields.add("_id");
				break;
			default:
				break;
		}
		if (CollectionUtils.isEmpty(specialPkFields)) {
			return pkFields;
		}
		List<RelateDatabaseField> filterPkFields = pkFields.stream().filter(f -> !specialPkFields.contains(f.getField_name())).collect(Collectors.toList());
		if (CollectionUtils.isNotEmpty(filterPkFields)) {
			pkFields = filterPkFields;
		} else {
			pkFields = pkFields.stream().filter(f -> specialPkFields.contains(f.getField_name())).collect(Collectors.toList());
		}
		return pkFields;
	}

	private void processMessage(MessageEntity firstMsg, List<MessageEntity> msgs) {
		if (OperationType.isDml(firstMsg.getOp())) {
		}

		AtomicInteger dmlCount = new AtomicInteger(0);
		msgs.forEach(message -> {
			if (StringUtils.isNotBlank(message.getOp()) && OperationType.isDml(message.getOp())) {
				dmlCount.incrementAndGet();
			}
			message.setSyncStage(SyncStageEnum.fromSyncStage(OffsetUtil.getSyncStage(message)));
		});

		long start = System.currentTimeMillis();
		final OnData onData = target.onData(msgs);
		if (null != onData) {


			Stats stats = targetContext.getJob().getStats();
			onDataStats(onData, stats);
		}
	}

	@Override
	public void doClose() throws Exception {
		running.compareAndSet(true, false);
		Optional.ofNullable(targetContext).ifPresent(context -> context.getJob().setStatus(ConnectorConstant.STOPPING));
		if (target != null) {
			try {
				target.targetStop(false);
			} catch (Throwable e) {
//        target.targetStop(true);
			}
		}

		// should call super since there are some clean up jobs in super
		super.doClose();
	}

//  private void initTargetDB() throws Exception {
//    // create table
//    createTargetTable();
//
//    // create target indices
//    ImmutableMap<String, Object> params = null;
//    try {
//      params = ImmutableMap.of(
//        "targetName", targetContext.getTargetConn().getName()
//      );
//      targetContext.getCustomerLogger().info(CustomerLogMessagesEnum.AGENT_CREATE_INDEXES_STARTED, params);
//    } catch (Exception ignored) {
//    }
//    createTargetIndicesForSync(target, targetContext);
//    try {
//      targetContext.getCustomerLogger().info(CustomerLogMessagesEnum.AGENT_CREATE_INDEXES_COMPLETED, params);
//    } catch (Exception ignored) {
//    }
//
//    if (target instanceof TargetExtend) {
//      ((TargetExtend) target).afterCreateTargetTable();
//    }
//
//    // clear target table
//    clearTargetTables();
//  }

//  private void createTargetTable() throws Exception {
//    logger.info("Start auto create target table(s)");
//    Connections targetConn = dataProcessorContext.getTargetConn();
//    try {
//      Class<?> ddlMakerClazz;
//      try {
//        ddlMakerClazz = DDLProducer.getDDLMaker(DatabaseTypeEnum.fromString(targetConn.getDatabase_type()));
//      } catch (Exception e) {
//        String msg = "Find ddl maker failed, database type: " + targetConn.getDatabase_type() + ", cause: " + e.getMessage();
//        throw new Exception(msg, e);
//      }
//
//      if (ddlMakerClazz != null) {
//        // Milestone-CREATE_TARGET_TABLE-RUNNING
//        MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.RUNNING);
//        DDLMaker<?> ddlMaker = (DDLMaker<?>) ddlMakerClazz.newInstance();
//        Map<String, List<RelateDataBaseTable>> schema = targetConn.getSchema();
//        Job job = targetContext.getJob();
//        List<RelateDataBaseTable> tables = schema.get("tables");
//        List<Mapping> mappings = job.getMappings();
//
//        if (job.isOnlyInitialAddMapping()) {
//          mappings = job.getAddInitialMapping();
//        }
//
//        final List<RelateDataBaseTable> nodeSchemas = processorBaseContext.getNodeSchemas();
//
//        final List<String> toTables = mappings.stream().map(Mapping::getTo_table).distinct().collect(Collectors.toList());
//        String targetName = targetContext.getTargetConn().getName();
//        targetContext.getCustomerLogger().info(CustomerLogMessagesEnum.AGENT_CREATE_TABLES_STARTED, ImmutableMap.of(
//          "targetName", targetName,
//          "tables", StringUtils.joinWith(", ", toTables)
//        ));
//        ExecutorUtil executorUtil = new ExecutorUtil();
//        executorUtil.queueMultithreading(toTables, null, toTable -> {
//          try {
//            Thread.currentThread().setName(threadName);
//            RelateDataBaseTable table = nodeSchemas.stream().filter(r -> toTable.equals(r.getTable_name())).findFirst().get();
//
//            if (table == null) {
//              throw new Exception("Target connection " + targetConn + " does not contain " + toTable + "'s schema.");
//            }
//
////            table.getFields().stream().forEach(f -> f.setPrimary_key_position(0));
//
//            if (target instanceof JdbcTarget) {
//              targetConn.setDbFullVersion(JdbcUtil.getDbFullVersion(targetConn));
//            }
//            Object createTable = ddlMaker.createTable(targetConn, table);
//            try {
//              target.createTable(createTable, toTable);
//            } catch (UnsupportedOperationException e) {
//              // Try to use the old version of automatic table creation
//              logger.warn(e.getMessage() + "; stacks:\n  " + Log4jUtil.getStackString(e));
//            }
//          } catch (Exception e) {
//            if (targetContext.isRunning()) {
//              if (!job.jobError(e, true, SyncStageEnum.SNAPSHOT.getSyncStage(), logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER, e.getMessage(), null)) {
//                throw new ExecutorUtil.InterruptExecutorException(e);
//              }
//            } else {
//              throw new ExecutorUtil.InterruptExecutorException();
//            }
//          }
//        }, "auto create target table", targetContext, targetContext -> !targetContext.isRunning(), job);
//        targetContext.getCustomerLogger().info(CustomerLogMessagesEnum.AGENT_CREATE_TABLES_COMPLETED, ImmutableMap.of(
//          "targetName", targetName
//        ));
//        // Milestone-CREATE_TARGET_TABLE-FINISH
//        MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.FINISH);
//      } else {
//        logger.warn("Create target table failed; Database type " + targetConn.getDatabase_type() + " not supported");
//      }
//    } catch (Exception e) {
//      String msg = "Failed to automatically create target table. Will stop job, please try to restart the task after creating the table manually, cause: " + e.getMessage() + "\n  " + Log4jUtil.getStackString(e);
//      // Milestone-CREATE_TARGET_TABLE-ERROR
//      MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.CREATE_TARGET_TABLE, MilestoneStatus.ERROR, msg);
//      throw new Exception(msg, e);
//    }
//  }

	private void clearTargetTables() {
		logger.info("Start auto clear target table(s)");
		Connections targetConn = dataProcessorContext.getTargetConn();
		// Milestone-CLEAR_TARGET_DATA-RUNNING
		if (targetContext != null && targetContext.needCleanTarget() && targetContext.isRunning()) {
			try {
				Job job = targetContext.getJob();
				List<Mapping> mappings = job.getMappings();
				if (job.isOnlyInitialAddMapping()) {
					mappings = job.getAddInitialMapping();
				}
				String toTableStr = mappings.stream().limit(10).map(Mapping::getTo_table).distinct().collect(Collectors.joining(", "));
				if (mappings.size() > 10) {
					toTableStr += "...";
				}
				String op = "truncate table(s)";
				String databaseType = targetConn.getDatabase_type();
				if (DatabaseTypeEnum.MONGODB.getType().equals(databaseType)) {
					op = "drop collection(s)";
				} else if (DatabaseTypeEnum.ELASTICSEARCH.getType().equals(databaseType)) {
					op = "delete index(es)";
				}

				try {
					ReflectUtil.invokeInterfaceMethod(target, TARGET_EXTENT_CLASS_NAME, "deleteTargetTables");
				} catch (InvocationTargetException e) {
					throw e;
				}

				if (StringUtils.isNotBlank(toTableStr)) {
					logger.info("Finished {} in target {} database: {}", op, databaseType, toTableStr);
				}

			} catch (Throwable e) {
				String errMsg = String.format("Automatically clear target data failed, err: %s, stacks: %s", StringUtils.isBlank(e.getMessage()) ? e.getCause().getMessage() : e.getMessage(), Log4jUtil.getStackString(e));
			}
		} else {
			Job job = targetContext.getJob();
			logger.info("Found no need to clear target data, offset: " + job.getOffset() + ", sync type: " + job.getSync_type() + ", drop table: " + job.getDrop_target() + ", keep schema: " + job.getKeepSchema());
		}
	}

	public io.tapdata.entity.Context getOldTapdataContext() {
		return targetContext;
	}

	@Override
	public boolean saveToSnapshot() {
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		String collection = ConnectorConstant.TASK_COLLECTION + "/syncProgress/" + taskDto.getId();
		try {
			clientMongoOperator.insertOne(this.syncProgressMap, collection);
		} catch (Exception e) {
			logger.error("Save to snapshot failed, collection: " + collection + ", object: " + this.syncProgressMap + "Errors: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			return false;
		}
		return true;
	}


	/**
	 * Auto create indices on target database
	 *
	 * @param target
	 * @param targetContext
	 * @throws IllegalAccessException
	 */
	private static void createTargetIndicesForSync(Target target, TargetContext targetContext) throws Exception {
		if (
				target == null
						|| !targetContext.isRunning()
						|| !targetContext.getJob().getNeedToCreateIndex()
						|| targetContext.getJob().is_test_write()
						|| targetContext.getJob().is_null_write()
		) {
			return;
		}

		try {
			logger.info("Start auto create target table's index");

//      Object instance = getObjectFromTransfomer(transformer);
			Job job = targetContext.getJob();
			List<Mapping> mappings = job.getMappings();

			if (mappings == null) {
				return;
			}

			if (job.isOnlyInitialAddMapping()) {
				mappings = job.getAddInitialMapping();
			}

			ExecutorUtil executorUtil = new ExecutorUtil();
			executorUtil.queueMultithreading(mappings,
					Objects::nonNull,
					mapping -> {
						try {
							String toTable = mapping.getTo_table();
							String relationship = mapping.getRelationship();
							List<Map<String, String>> joinCondition = mapping.getJoin_condition();
							List<Map<String, String>> matchCondition = mapping.getMatch_condition();

							if (ConnectorConstant.RELATIONSHIP_ONE_ONE.equals(relationship)
									&& CollectionUtils.isNotEmpty(joinCondition) && !mapping.getNoPrimaryKey()) {

								createIndex(target, targetContext, toTable, joinCondition);

							} else if (ConnectorConstant.RELATIONSHIP_ONE_MANY.equals(relationship)
									&& CollectionUtils.isNotEmpty(joinCondition)) {

								createIndex(target, targetContext, toTable, joinCondition);

							} else if (ConnectorConstant.RELATIONSHIP_MANY_ONE.equals(relationship)
									&& CollectionUtils.isNotEmpty(joinCondition)
									&& CollectionUtils.isNotEmpty(matchCondition)) {

								joinCondition.addAll(matchCondition);
								createIndex(target, targetContext, toTable, joinCondition);

							}

							if (targetContext.getTargetConn().getDatabase_type().equals(DatabaseTypeEnum.LOG_COLLECT.getType())) {
								// create log collect index
								List<Map<String, String>> timestampCondition = buildIndexCondition(new HashSet<String>() {{
									add("timestamp");
								}});
								createIndex(target, targetContext, toTable, timestampCondition);

								List<Map<String, String>> xidCondition = buildIndexCondition(new HashSet<String>() {{
									add("data.XID");
								}});
								createIndex(target, targetContext, toTable, xidCondition);
							}
						} catch (Exception e) {
							if (job.isRunning()) {
								job.jobError(e, true, SyncStageEnum.SNAPSHOT.getSyncStage(), logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER,
										"Create target index failed, connection name: " + targetContext.getTargetConn().getName() + ", table name: " + mapping.getTo_table() + "; " + e.getMessage(), null);
							} else {
								throw new ExecutorUtil.InterruptExecutorException();
							}
						}
					},
					"auto create target index", targetContext, transformerContext -> !targetContext.isRunning(), targetContext.getJob()
			);

		} catch (Exception e) {
			String errMsg = String.format("Automatically create target index(s) failed, job name: %s, err: %s, stacks: %s",
					targetContext.getJob().getName(), e.getMessage() == null ? e.getCause().getMessage() : e.getMessage(), Log4jUtil.getStackString(e));

			targetContext.getJob().jobError(e, true, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER, errMsg, null);
		}
	}


	private static void createIndex(Target target, TargetContext targetContext, String toTable, List<Map<String, String>> condition) throws IllegalAccessException, InvocationTargetException {
		long startTs = System.currentTimeMillis();

		Object needCreateIndex = ReflectUtil.invokeInterfaceMethod(target,
				TARGET_EXTENT_CLASS_NAME, "needCreateIndex",
				toTable, condition, targetContext.getTargetConn());
		if (!Boolean.valueOf(needCreateIndex + "")) {
			return;
		}

		if (logger.isDebugEnabled()) {
			logger.debug(TapLog.TRAN_LOG_0035.getMsg(), targetContext.getTargetConn().getDatabase_type(), toTable, getIndexDoc(condition).toJson());
		}
		try {
			ReflectUtil.invokeInterfaceMethod(
					target, TARGET_EXTENT_CLASS_NAME, "createIndex", toTable, condition
			);
			if (logger.isDebugEnabled()) {
				logger.debug(TapLog.TRAN_LOG_0036.getMsg(), System.currentTimeMillis() - startTs);
			}
		} catch (InvocationTargetException e) {
			throw e;
		}
	}

	private static Document getIndexDoc(List<Map<String, String>> list) {
		Document doc = new Document();
		list.forEach(map -> map.entrySet().stream().forEachOrdered(e -> doc.put(e.getKey(), 1)));
		return doc;
	}

	private static List<Map<String, String>> buildIndexCondition(Set<String> keys) {
		List<Map<String, String>> condition = new ArrayList<>();
		Map<String, String> keyMap = new HashMap<>();
		condition.add(keyMap);

		if (CollectionUtils.isEmpty(keys)) {
			return null;
		}

		keys.forEach(key -> keyMap.put(key, key));

		return condition;
	}
}
