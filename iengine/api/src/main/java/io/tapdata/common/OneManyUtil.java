package io.tapdata.common;

import com.mongodb.MongoClient;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.tapdata.constant.*;
import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.DataQualityTagProcess;
import io.tapdata.exception.OneManyLookupException;
import io.tapdata.exception.SourceException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class OneManyUtil {

	private Connections targetConnections;
	private List<MessageEntity> msgs;
	private static Logger logger = LogManager.getLogger(OneManyUtil.class);
	private List<Mapping> mappings;
	private Job job;
	private SettingService settingService;

	private boolean isRunning() {
		return ConnectorConstant.RUNNING.equals(job.getStatus()) && !Thread.currentThread().isInterrupted();
	}

	public OneManyUtil(Connections targetConnections, List<MessageEntity> msgs, List<Mapping> mappings, Job job, SettingService settingService) {
		this.targetConnections = targetConnections;
		this.msgs = msgs == null ? new ArrayList<>() : msgs;
		this.mappings = mappings == null ? new ArrayList<>() : mappings;
		this.job = job == null ? new Job() : job;

		this.settingService = settingService;
	}

	public static void cdcOneManyHandle(Job job, Connections targetConn, SettingService settingService, List<MessageEntity> msgs) {
		if (job == null || targetConn == null || settingService == null) {
			throw new IllegalArgumentException("Failed to init oneManyUtil, missing args");
		}

		if (CollectionUtils.isEmpty(msgs)) {
			return;
		}

		List<Mapping> mappings = job.getMappings();

		OneManyUtil oneManyUtil;
		try {
			oneManyUtil = new OneManyUtil(
					targetConn,
					msgs,
					mappings,
					job,
					settingService
			);
		} catch (Exception e) {
			throw new SourceException(String.format("Init one many util failed %s", e.getMessage()), e, true);
		}
		if (oneManyUtil.checkIfNeedLookup()) {

			// 将one many临时表写入到目标表
			oneManyUtil.oneManyTPORIGWrite();

			int lookupRetryTime = 3;
			int lookupFailTime = 0;
			long retryInterval;

			while (job.isRunning()) {
				try {
					oneManyUtil.lookup();
					break;
				} catch (OneManyLookupException e) {
					// 不可重试的错误，直接抛出
					if (!e.isRetryException()) {
						throw e;
					}

					// 重试逻辑
					if (++lookupFailTime > lookupRetryTime) {
						throw e;
					} else {
						// 告警重试
						retryInterval = lookupFailTime * 5;
						logger.warn(e.getMessage() + ", will retry after " + retryInterval + " second, fail time: " + lookupFailTime);
						try {
							Thread.sleep(retryInterval * 1000);
						} catch (InterruptedException interruptedException) {
							break;
						}
						continue;
					}
				}
			}
		}
	}

	private boolean validateLookupMessage(MessageEntity messageEntity) {
		if (messageEntity == null || StringUtils.isBlank(messageEntity.getOp())) {
			return false;
		}

		if (!StringUtils.equalsAny(messageEntity.getOp(), ConnectorConstant.MESSAGE_OPERATION_INSERT, ConnectorConstant.MESSAGE_OPERATION_UPDATE)) {
			return false;
		}

		return true;
	}

	public void lookup() {
		MongoClient mongoClient = null;
		MongoDatabase mongoDatabase;

		try {
			try {
				mongoClient = MongodbUtil.createMongoClient(targetConnections);
				mongoDatabase = mongoClient.getDatabase(MongodbUtil.getDatabase(targetConnections));
			} catch (UnsupportedEncodingException e) {
				String err = "Connect to mongodb when one many look up failed, connection name: " + targetConnections.getName() + ", uri: " +
						MongodbUtil.maskUriPassword(targetConnections.getDatabase_uri()) + ", err: " + e.getMessage() + ", stack: " + Log4jUtil.getStackString(e);
				throw new OneManyLookupException(err, e, true);
			}

			for (MessageEntity msg : msgs) {
				if (!isRunning()) {
					break;
				}

				if (!validateLookupMessage(msg)) {
					continue;
				}

				// data quality
				DataQualityTagProcess.handleDataQualityTag(msg);

				Map<String, Map<String, Object>> subMap = new HashMap<>();
				Mapping msgMapping = msg.getMapping();
				if (msgMapping != null) {
					List<Mapping> lookUpMappings = findMappingsAndSortByInitialSyncOrder(msgMapping);
					for (Mapping mapping : lookUpMappings) {
						lookupByMapping(mongoDatabase,
								msg.getDataQualityTag().getHitRules(),
								msg.getDataQualityTag().getPassRules(), msg, mapping, subMap);
					}
				} else {
					List<Mapping> lookUpMappings = findMappings(msg.getTableName());
					if (CollectionUtils.isNotEmpty(lookUpMappings)) {

						for (Mapping mapping : lookUpMappings) {

							lookupByMapping(mongoDatabase,
									msg.getDataQualityTag().getHitRules(),
									msg.getDataQualityTag().getPassRules(), msg, mapping, subMap);
						}
					}
				}
				msg.setSubMap(subMap);
			}
		} finally {
			if (mongoClient != null) {
				mongoClient.close();
			}
		}
	}

	public void oneManyTPORIGWrite() {
		if (CollectionUtils.isNotEmpty(msgs)) {
			ClientMongoOperator targetMongoOperator = null;
			try {
				String replacement = settingService.getString(ConnectorConstant.SETTINGS_JOB_REPLACEMENT);
				Map<String, List<WriteModel<Document>>> writeModelsMap = new HashMap<>();
				for (int i = 0; i < msgs.size() && ConnectorConstant.RUNNING.equals(job.getStatus()); i++) {
					MessageEntity msg = msgs.get(i);
					Mapping mapping = msg.getMapping();
					try {

						if (mapping != null) {
							String toTable = mapping.getTo_table();
							if (StringUtils.endsWith(toTable, ConnectorConstant.LOOKUP_TABLE_SUFFIX)) {

								if (targetMongoOperator == null) {
									try {
										MongoClient mongoClient = MongodbUtil.createMongoClient(targetConnections);
										String database = MongodbUtil.getDatabase(targetConnections);
										targetMongoOperator = new ClientMongoOperator(new MongoTemplate(mongoClient, database), mongoClient);
									} catch (UnsupportedEncodingException e) {
										job.jobError(e, false, OffsetUtil.getSyncStage(msg), logger, ConnectorConstant.WORKER_TYPE_TRANSFORMER,
												TapLog.CONN_ERROR_0016.getMsg(), null, targetConnections.toString(), e.getMessage());
									}
								}

								Document filter = new Document();

								String op = msg.getOp();
								switch (op) {
									case ConnectorConstant.MESSAGE_OPERATION_INSERT:
									case ConnectorConstant.MESSAGE_OPERATION_UPDATE:
									case ConnectorConstant.MESSAGE_OPERATION_ABSOLUTE_INSERT:

										Map<String, Object> after = msg.getAfter();

										if (MapUtils.isNotEmpty(after)) {
											// 关联条件中不包含_id 且 写入路劲为空，需要将_id移除
											MongodbUtil.removeIdIfNeed(mapping.getJoin_condition(), mapping.getTarget_path(), after);

											MongodbUtil.constructFilterInTarget(mapping.getJoin_condition(), after, replacement, filter);

											if (!writeModelsMap.containsKey(toTable)) {
												writeModelsMap.put(toTable, new ArrayList<>());
											}
											writeModelsMap.get(toTable).add(new UpdateOneModel<>(filter, new Document("$set", after), new UpdateOptions().upsert(true)));
											msgs.remove(i);
											i--;
										}
										break;
									case ConnectorConstant.MESSAGE_OPERATION_DELETE:

										final Map<String, Object> before = msg.getBefore();
										if (MapUtils.isNotEmpty(before)) {
											MongodbUtil.constructFilterInTarget(mapping.getJoin_condition(), before, replacement, filter);

											if (!writeModelsMap.containsKey(toTable)) {
												writeModelsMap.put(toTable, new ArrayList<>());
											}

											writeModelsMap.get(toTable).add(new DeleteOneModel<>(filter));
											msgs.remove(i);
											i--;
										}
										break;
									default:
										logger.warn("Unknown message op {} for mapping {}, msg {}", op, mapping, msg);
										break;
								}
							}


						}
					} catch (Exception e) {
						throw new SourceException(
								String.format(
										"Write tapdata origin event %s to table %s failed %s, mapping %s",
										msg,
										mapping.getTo_table(),
										e.getMessage(),
										mapping
								),
								e,
								true
						);
					}
				}

				if (MapUtils.isNotEmpty(writeModelsMap)) {
					for (Map.Entry<String, List<WriteModel<Document>>> entry : writeModelsMap.entrySet()) {

						if (!ConnectorConstant.RUNNING.equals(job.getStatus())) {
							break;
						}

						String toCollection = entry.getKey();
						List<WriteModel<Document>> writeModels = entry.getValue();
						targetMongoOperator.executeBulkWrite(writeModels, new BulkWriteOptions().ordered(true), toCollection, Job.ERROR_RETRY, Job.ERROR_RETRY_INTERVAL, job);
					}
				}
			} catch (Exception e) {
				if (e instanceof SourceException) {
					throw (SourceException) e;
				}
				throw new SourceException(String.format("Write tapdata origin events failed %s", e.getMessage()), e, true);
			} finally {
				if (targetMongoOperator != null) {
					targetMongoOperator.releaseResource();
				}
			}
		}
	}

	private void lookupByMapping(MongoDatabase mongoDatabase,
								 List<DataQualityTag.HitRules> hitRules,
								 List<DataQualityTag.HitRules> passRules,
								 MessageEntity msg,
								 Mapping mapping,
								 Map<String, Map<String, Object>> subMap) {

		String collectionName = mapping.getFrom_table();
		String target_path = mapping.getTarget_path();

		String stageId = findOneManyStageById(mapping);
		String oneManyCollection =
				Mapping.need2CreateTporigMapping(job, mapping) ?
						DataFlowUtil.getOneManyTporigTableName(
								collectionName,
								job.getDataFlowId(),
								stageId
						) :
						collectionName;
		MongoCollection mongoCollection = mongoDatabase.getCollection(oneManyCollection);
		if (mongoCollection != null) {
			Document document = new Document();
			List<Map<String, String>> joinCondition = mapping.getJoin_condition();

			for (Map<String, String> condition : joinCondition) {
				for (Map.Entry<String, String> entry : condition.entrySet()) {

					String source = entry.getValue();
					String target = entry.getKey();

					if (source == null) {
						throw new OneManyLookupException(
								String.format(
										"Look up condition source field cannot be null, mapping %s",
										mapping
								)
						);
					}

					Object value = findJoinConditionValue(msg, target, mapping, subMap);

					if (value == null) {
						continue;
					}
					document.append(source, value);
				}
			}

			if (MapUtils.isEmpty(document)) {
				return;
			}

			Map<String, Object> values = new LinkedHashMap<>();
			int foundCount = 0;
			try {
				FindIterable findIterable = mongoCollection.find(document);
				try (MongoCursor mongoCursor = findIterable.iterator()) {
					while (mongoCursor.hasNext()) {
						Document next = (Document) mongoCursor.next();
						foundCount++;

						// 临时方案，解决亚信多张一对多的子表同步时 导致teamSysNo被修改的问题
						if (foundCount > 1) {
							logger.warn("One many lookup found multiple sub table records, mapping {}, message {}, sub map {}.", mapping, msg, subMap);
//							return;
						}

						for (Map.Entry<String, Object> entry : next.entrySet()) {
							String key = entry.getKey();
							Object value = entry.getValue();

							if (key.equals(DataQualityTag.SUB_COLUMN_NAME)) {
								try {
									List<DataQualityTag.HitRules> cloneHitRules = (List<DataQualityTag.HitRules>) ((Document) value).get(DataQualityTag.HITRULES_FIELD);
									List<DataQualityTag.HitRules> clonePassRules = (List<DataQualityTag.HitRules>) ((Document) value).get(DataQualityTag.PASSRULES_FIELD);
									if (cloneHitRules != null) {
										hitRules.addAll(cloneHitRules);
									}
									if (clonePassRules != null) {
										passRules.addAll(clonePassRules);
									}
								} catch (Exception e) {
									continue;
								}
							} else if (!entry.getKey().equals("_id")) {
								values.put(key, value);
							} else {
								// do nothing
							}
						}
					}
				}
			} catch (Exception e) {
				String err = "Look up failed, collection: " + oneManyCollection + ", filter: " + document
						+ ", err: " + e.getMessage() + ", stack: " + Log4jUtil.getStackString(e);
				throw new OneManyLookupException(err, e, true);
			}

			if (MapUtils.isNotEmpty(values)) {
				putLookupValuesInSubMap(collectionName, target_path, subMap, values);
			}
		}
	}

	public static String findOneManyStageById(Mapping mapping) {

		if (mapping == null) {
			return null;
		}

		final List<Stage> stages = mapping.getStages();
		if (CollectionUtils.isNotEmpty(stages)) {
			for (Stage stage : stages) {
				if (CollectionUtils.isEmpty(stage.getOutputLanes())) {
					continue;
				}
				List<String> outputLanes = stage.getOutputLanes();
				for (String outputLane : outputLanes) {
					if (StringUtils.endsWith(outputLane, ConnectorConstant.LOOKUP_TABLE_SUFFIX)) {
						return outputLane;
					}
				}
			}
		}
		return null;
	}

	/**
	 * Find condition value by target
	 * 1. try to get join condition value from after
	 * 2. if not found in after, try to get join condition value from subMap
	 *
	 * @param msg
	 * @param target
	 * @param mapping
	 * @param subMap
	 * @return not found value will return null
	 */
	private Object findJoinConditionValue(MessageEntity msg, String target, Mapping mapping, Map<String, Map<String, Object>> subMap) {
		Object value;
		if (msg == null) {
			return null;
		}
		Map<String, Object> after = msg.getAfter();
		if (MapUtils.isEmpty(after)) {
			return null;
		}
		if (msg.getMapping() != null) {

			// 1
			if (StringUtils.isNotBlank(msg.getMapping().getTarget_path())) {
				value = MapUtil.getValueByKey(after, StringUtils.removeStart(target, msg.getMapping().getTarget_path() + "."));
			} else {
				value = MapUtil.getValueByKey(after, target);
			}

			if (value == null) {
				// 2
				if (target.contains(".")) {
					// if target contains dot, will find join condition mapping first
					// e.g. target="CUSTOMER.CUSTOMER_ID", will find a mapping in mappings that target_path=CUSTOMER
					String tempFieldName = "";
					Mapping joinConditionMapping = null;
					for (String t : target.split("\\.")) {
						tempFieldName = StringUtils.isNotBlank(tempFieldName) ? tempFieldName + "." + t : t;
						String finalTempFieldName = tempFieldName;
						joinConditionMapping = mappings.stream().filter(m -> m.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_MANY)
								&& m.getInitialSyncOrder() >= mapping.getInitialSyncOrder()
								&& m.getTarget_path().equals(finalTempFieldName)).findFirst().orElse(null);

						if (joinConditionMapping != null) {
							break;
						}
					}

					if (joinConditionMapping != null) {
						Map<String, Object> joinValues = subMap.getOrDefault(joinConditionMapping.getFrom_table() + "." + joinConditionMapping.getTarget_path(), new HashMap<>());
						value = MapUtil.getValueByKey(joinValues, StringUtils.removeStart(target, tempFieldName + "."));
					}
				} else {
					// if target not contains dot, will find in subMap one by one
					for (Map<String, Object> subValues : subMap.values()) {
						value = MapUtil.getValueByKey(subValues, target);
						if (value != null) {
							break;
						}
					}
				}
			}

		} else {
			// old job
			value = MapUtil.getValueByKey(after, target);
		}

		return value;
	}

	/**
	 * put look up values in subMap
	 *
	 * @param collectionName
	 * @param target_path
	 * @param subMap
	 * @param values
	 */
	private void putLookupValuesInSubMap(String collectionName, String target_path, Map<String, Map<String, Object>> subMap, Map<String, Object> values) {
		String key = collectionName + "." + target_path;
		if (subMap.containsKey(key)) {
			Map<String, Object> map = subMap.get(key);
			if (map == null) {
				map = values;
			} else {
				map.putAll(values);
			}
			subMap.put(key, map);
		} else {
			subMap.put(key, values);
		}
	}

	private List<Mapping> findMappings(String fromTable) {
		List<Mapping> retMappings = new ArrayList<>();
		Set<String> toTables = findToTableByFromTable(fromTable);
		for (String toTable : toTables) {
			for (Mapping mapping : mappings) {
				if (mapping.getTo_table().equals(toTable) && mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_MANY)) {
					retMappings.add(mapping);
				}
			}
		}

		return retMappings;
	}

	private List<Mapping> findMappingsAndSortByInitialSyncOrder(Mapping msgMapping) {
		List<Mapping> retMappings = new ArrayList<>();
		String msgFromTable = msgMapping.getFrom_table();
		String msgToTable = msgMapping.getTo_table();
		int msgInitialSyncOrder = msgMapping.getInitialSyncOrder();

		mappings.forEach(mapping -> {
			if (mapping.getTo_table().equals(msgToTable)
					&& !mapping.getFrom_table().equals(msgFromTable)
					&& mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_MANY)
					&& mapping.getInitialSyncOrder() >= msgInitialSyncOrder) {
				retMappings.add(mapping);
			}
		});

		retMappings.stream().sorted(Comparator.comparing(Mapping::getInitialSyncOrder));

		return retMappings;
	}

	private Set<String> findToTableByFromTable(String fromTable) {
		Set<String> toTables = new HashSet<>();
		if (StringUtils.isNotBlank(fromTable)) {
			for (Mapping mapping : mappings) {
				if (mapping.getFrom_table().equals(fromTable)) {
					toTables.add(mapping.getTo_table());
				}
			}
		}
		return toTables;
	}

	public boolean checkIfNeedLookup() {
		String sync_type = job.getSync_type();
		if (sync_type.equals(ConnectorConstant.SYNC_TYPE_INITIAL_SYNC)) {
			return false;
		}

		// check mappings have onemany relationship
		if (!mappings.stream().anyMatch(mapping -> mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_MANY))) {
			return false;
		}

		// check msgs offset's sync stage=cdc
		if (msgs.stream().anyMatch(msg -> {
			Object offset = msg.getOffset();
			return offset instanceof TapdataOffset && offset != null
					&& TapdataOffset.SYNC_STAGE_CDC.equals(((TapdataOffset) offset).getSyncStage());
		})) {
			return true;
		}

		return false;
	}

//    @Deprecated
//    public void OneManyFilter() {
//        MongoClient mongoClient = null;
//        MongoDatabase mongoDatabase = null;
//        MongoCollection mongoCollection;
//
//        boolean flag = true;
//        for (Mapping mapping : mappings) {
//            if (mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_MANY)) {
//                flag = false;
//                break;
//            }
//        }
//        if (flag) {
//            return;
//        }
//
//        try {
//            try {
//                mongoClient = MongodbUtil.createMongoClient(targetConnections);
//                mongoDatabase = mongoClient.getDatabase(MongodbUtil.getDatabase(targetConnections));
//            } catch (UnsupportedEncodingException e) {
//
//            }
//
//            for (int i = 0; i < msgs.size(); i++) {
//                MessageEntity msg = msgs.get(i);
//                Map<String, Object> after = msg.getAfter();
//                if (ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(msg.getOp())) {
//                    for (Mapping mapping : mappings) {
//                        if (mapping.getFrom_table().equals(msg.getTableName())
//                                && mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_MANY)) {
//                            Document document = new Document();
//                            for (Map<String, String> conditionMap : mapping.getJoin_condition()) {
//                                String target = conditionMap.get("target");
//                                document.append(target, after.get(target));
//                            }
//
//                            mongoCollection = mongoDatabase.getCollection(mapping.getTo_table());
//                            long count = mongoCollection.count(document);
//
//                            if (count <= 0) {
//                                msgs.remove(msg);
//                                i--;
//                                break;
//                            }
//                        }
//                    }
//                }
//            }
//        } finally {
//            if (mongoClient != null) {
//                mongoClient.close();
//            }
//        }
//    }

	public static boolean deleteIfOneManyNotMatch(BulkWriteResult result, MongoCollection<Document> collection,
												  String collectionName, String fromTable, List<Mapping> mappings) {
		List<WriteModel<Document>> writeModels = new ArrayList<>();
		if (result != null && collection != null && CollectionUtils.isNotEmpty(mappings) && StringUtils.isNotBlank(collectionName)
				&& StringUtils.isNotBlank(fromTable)) {

			for (Mapping mapping : mappings) {
				if (mapping.getTo_table().equals(collectionName) && fromTable.equals(mapping.getFrom_table())
						&& mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_MANY)) {
					List<BulkWriteUpsert> upserts = result.getUpserts();
					if (CollectionUtils.isNotEmpty(upserts)) {

						for (BulkWriteUpsert upsert : upserts) {
							Document where = new Document("_id", upsert.getId());
							writeModels.add(new DeleteManyModel<>(where));
						}
						if (CollectionUtils.isNotEmpty(writeModels)) {
							collection.bulkWrite(writeModels);
						}

						return true;
					}
				}
			}
		}

		return false;
	}

	public static void addTableCloneWhenOneMany(Job job, boolean isTransformer) {
		List<Mapping> mappingsNew = new ArrayList<>();

		List<Mapping> mappings = job.getMappings();
		if (mappings != null) {

			for (Mapping mapping : mappings) {
				if (mapping.getRelationship().equals(ConnectorConstant.RELATIONSHIP_ONE_MANY) &&
						Mapping.need2CreateTporigMapping(job, mapping)) {

					final String stageId = findOneManyStageById(mapping);
					String fromTable = mapping.getFrom_table();
					String toTable = DataFlowUtil.getOneManyTporigTableName(
							fromTable,
							job.getDataFlowId(),
							stageId
					);
					List<Map<String, String>> joinCondition = mapping.getJoin_condition();
					List<Map<String, String>> joinConditionNew = new ArrayList<>();

					if (!isTransformer) {
						joinConditionHandle(joinCondition, joinConditionNew);
					} else {
						joinConditionHandleForTransformer(joinCondition, joinConditionNew);
					}

					List<FieldProcess> fieldsProcess = mapping.getFields_process();
					String script = mapping.getScript();
					Mapping mappingNew = new Mapping(
							fromTable,
							toTable,
							ConnectorConstant.RELATIONSHIP_ONE_ONE,
							joinConditionNew,
							fieldsProcess,
							script,
							999,
							mapping.getFieldFilter(),
							mapping.getFieldFilterType()
					);
					mappingNew.setRules(mapping.getRules());
					mappingsNew.add(mappingNew);
				}
			}

			mappings.addAll(mappingsNew);
		}
	}

	private static void joinConditionHandle(List<Map<String, String>> joinCondition, List<Map<String, String>> joinConditionNew) {
		for (Map<String, String> conditionMap : joinCondition) {
			Map<String, String> contitionMapNew = new HashMap<>();
			String source = conditionMap.get("source");
			contitionMapNew.put("source", source);
			contitionMapNew.put("target", source);
			joinConditionNew.add(contitionMapNew);
		}
	}

	private static void joinConditionHandleForTransformer(List<Map<String, String>> joinCondition, List<Map<String, String>> joinConditionNew) {
		for (Map<String, String> conditionMap : joinCondition) {
			Map<String, String> contitionMapNew = new HashMap<>();
			for (Map.Entry<String, String> entry : conditionMap.entrySet()) {
				String source = entry.getValue();
				contitionMapNew.put(source, source);
			}
			joinConditionNew.add(contitionMapNew);
		}
	}

	public static void subMap2Update(Map<String, Map<String, Object>> subMap, Update update, boolean array) throws Exception {
		subMap2Update(subMap, update, array, MongoUpdateType.SET);
	}

	public static void subMap2Update(Map<String, Map<String, Object>> subMap, Update update, boolean array, MongoUpdateType updateType) throws Exception {
		if (MapUtils.isEmpty(subMap) || update == null) {
			return;
		}
		Document updateObject = update.getUpdateObject();
		Map subUpdate;
		if (!(updateObject.containsKey(updateType.getKeyWord()))) {
			subUpdate = new Document();
			updateObject.put(updateType.getKeyWord(), subUpdate);
		} else {
			if (updateObject.get(updateType.getKeyWord()) instanceof Map) {
				subUpdate = (Map) updateObject.get(updateType.getKeyWord());
			} else {
				return;
			}
		}

		for (Map.Entry<String, Map<String, Object>> subRow : subMap.entrySet()) {
			Map<String, Object> rowValue = subRow.getValue();
			String[] split = subRow.getKey().split("\\.");
			String targetPath = split.length > 1 ? split[1] : "";
			Map subIntoWhere = getSubIntoWhere(updateType, subUpdate, targetPath);
			if (subIntoWhere == null) continue;

			Map<String, Object> flatValue = new HashMap<>();
			MapUtil.recursiveFlatMap(rowValue, flatValue, "");
			rowValue = flatValue;

			for (Map.Entry<String, Object> entry : rowValue.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				switch (updateType) {
					case SET:
						String column = StringUtils.isNotBlank(targetPath) ? targetPath + (array ? ".$" : "") + "." + key : key;
						subUpdate.put(column, value);
						break;
					case ADD_TO_SET:
						subIntoWhere.put(key, value);
				}
			}
		}
	}

	private static Map getSubIntoWhere(MongoUpdateType updateType, Map subUpdate, String targetPath) throws Exception {
		Map subIntoWhere = null;
		switch (updateType) {
			case SET:
				subIntoWhere = subUpdate;
				break;
			case ADD_TO_SET:
				if (StringUtils.isNotBlank(targetPath)) {
					Object targetPathValue = MapUtil.getValueByKey(subUpdate, targetPath);
					if (targetPathValue instanceof Map) {
						subIntoWhere = (Map) targetPathValue;
					} else {
						if (targetPathValue == null) {
							subIntoWhere = new Document();
							MapUtil.putValueInMap(subUpdate, targetPath, subIntoWhere);
						} else {
							throw new Exception("put look up result into addToSet document failed, value type is invalid: " + targetPathValue.getClass().getName() + ", target path: " + targetPath);
						}
					}
				} else {
					subIntoWhere = subUpdate;
				}
				break;
			default:
				break;
		}
		return subIntoWhere;
	}

	public List<MessageEntity> getMsgs() {
		return msgs;
	}
}
