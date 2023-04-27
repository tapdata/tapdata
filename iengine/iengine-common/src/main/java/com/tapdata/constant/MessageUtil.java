package com.tapdata.constant;

import com.tapdata.entity.DataQualityTag;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.ProcessResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author jackin
 */
public class MessageUtil {

	private static Logger logger = LogManager.getLogger(MessageUtil.class);

	/**
	 * @param msgs
	 * @param groupByTargetStage  按照target stage 分组，兼容targetStageId未空的场景
	 * @param processableConsumer
	 * @param commitMSGConsumer
	 */
	public static void dispatcherMessage(
			List<MessageEntity> msgs,
			boolean groupByTargetStage,
			Consumer<List<MessageEntity>> processableConsumer,
			Consumer<MessageEntity> commitMSGConsumer
	) {
		if (CollectionUtils.isEmpty(msgs)) {
			return;
		}
		while (msgs.size() > 0) {
			String previousStageId = null;
			List<MessageEntity> processableMsgs = new ArrayList<>();
			for (int i = 0; i < msgs.size(); i++) {
				MessageEntity messageEntity = msgs.get(i);
				String targetStageId = messageEntity.getTargetStageId();

				if (OperationType.isDdl(messageEntity.getOp()) || OperationType.isNotify(messageEntity.getOp())) {
					List<MessageEntity> newMsgs = new ArrayList<>();
					newMsgs.add(messageEntity);
					processableConsumer.accept(newMsgs);
					msgs.remove(i);
					i--;
				} else if (MessageUtil.isProcessableMessage(messageEntity)) {

					final Mapping mapping = messageEntity.getMapping();
					if (mapping != null && mapping.getNoPrimaryKey() && OperationType.UPDATE.getOp().equals(messageEntity.getOp()) && MapUtils.isEmpty(messageEntity.getBefore())) {
						throw new RuntimeException("Update event before cannot be empty when use 'no primary key' mode.");
					}

					// 按target stage 分组再分发
					if (groupByTargetStage && StringUtils.isNotBlank(targetStageId)) {
						if (StringUtils.isNotBlank(previousStageId) &&
								!previousStageId.equals(targetStageId)) {
							break;
						}

						previousStageId = targetStageId;
						processableMsgs.add(messageEntity);
						msgs.remove(i);
						i--;
					}
					// 不分组
					else {
						processableMsgs.add(messageEntity);
						msgs.remove(i);
						i--;
					}
				}

				// 将缓存中可处理的消息先处理完成
				else if (CollectionUtils.isNotEmpty(processableMsgs)) {
					break;
				}
				// 没有缓存可处理的消息，记录commitOffset消息的offset
				else {
					commitMSGConsumer.accept(messageEntity);
					msgs.remove(i);
					i--;
				}
			}

			if (CollectionUtils.isNotEmpty(processableMsgs)) {

				processableConsumer.accept(processableMsgs);
				processableMsgs.clear();
			}

		}
	}

	public static void setTargetBeforeForProcessResult(ProcessResult processResult, List<Map<String, Object>> targetBefores) {
		for (Map<String, Object> targetBefore : targetBefores) {
			if (targetBefore.containsKey("_id")) {
				MapUtil.removeValueByKey(targetBefore, DataQualityTag.SUB_COLUMN_NAME);
				Update update = processResult.getUpdate();
				if (update != null && org.apache.commons.collections.MapUtils.isNotEmpty(targetBefore)) {
					update.set(DataQualityTag.SUB_COLUMN_NAME + ".before", targetBefore);
				}
			}
		}
	}

	public static boolean isProcessableMessage(MessageEntity messageEntity) {
		if (messageEntity == null) {
			return true;
		}
		String op = messageEntity.getOp();
		if (StringUtils.isBlank(op)) {
			return false;
		}

		OperationType operationType = OperationType.fromOp(op);
		if (operationType == null) {
			return false;
		}

		return operationType.isProcessable();
	}

	public static boolean isInitialSyncStage(MessageEntity msg) {
		if (msg.getOffset() != null && msg.getOffset() instanceof TapdataOffset) {
			final TapdataOffset tapdataOffset = (TapdataOffset) msg.getOffset();
			return TapdataOffset.SYNC_STAGE_SNAPSHOT.equals(tapdataOffset.getSyncStage());
		}

		return false;
	}

	public static void judgeMessageProcess(List<MessageEntity> msgs, List<Mapping> mappings, Consumer<MessageEntity> consumer) {
		for (MessageEntity msg : msgs) {
			Mapping msgMapping = msg.getMapping();

			if (msgMapping != null) {

				consumer.accept(msg);


			} else {
				boolean foundMapping = false;
				String tableName = msg.getTableName();
				for (Mapping mapping : mappings) {
					String fromTable = mapping.getFrom_table();
					if (!fromTable.equals(tableName)) {
						continue;
					}

					msg.setMapping(mapping);
					consumer.accept(msg);
					foundMapping = true;
				}

				if (!foundMapping && (OperationType.isDdl(msg.getOp()) || OperationType.isNotify(msg.getOp()))) {
					consumer.accept(msg);
				}
			}
		}
	}

	/**
	 * 按照指定规则拆分消息事件
	 *
	 * @param messageEntities
	 * @param byDelete        按删除操作拆分，删除操作拆分到同一批消息中
	 * @param byJoinKey       按照关联条件拆分，关联条件值不相同的事件拆分到同一批消息中
	 * @return
	 */
	public static List<List<MessageEntity>> splitMessages(
			List<MessageEntity> messageEntities,
			boolean byDelete,
			boolean byJoinKey,
			String replacement
	) {
		return splitMessages(messageEntities, byDelete, byJoinKey, false, replacement);
	}

	public static List<List<MessageEntity>> splitMessages(
			List<MessageEntity> messageEntities,
			boolean byDelete,
			boolean byJoinKey,
			boolean byInsert,
			String replacement
	) {

		if (CollectionUtils.isEmpty(messageEntities)) {
			throw new IllegalArgumentException("Missing input arg");
		}
		List<List<MessageEntity>> splitMessageEntities = new LinkedList<>();
		List<MessageEntity> tempList = new LinkedList<>();

		boolean preIsDelete = false;
		boolean preIsInsert = false;
		Set<Map<String, Object>> pksSet = new HashSet<>();
		Iterator<MessageEntity> iterator = messageEntities.iterator();
		while (iterator.hasNext()) {
			MessageEntity messageEntity = iterator.next();

			Map<String, Object> recordIdMap = getRecordIdMap(
					replacement,
					messageEntity.getAfter(),
					messageEntity.getMapping()
			);
			if (deletePredicate(preIsDelete, messageEntity, byDelete) ||
					insertPredicate(preIsInsert, messageEntity, byInsert) ||
					joinKeyPredicate(
							pksSet,
							messageEntity,
							byJoinKey,
							recordIdMap
					)
			) {
				pksSet.clear();
				tempList = new LinkedList<>();
				tempList.add(messageEntity);
				splitMessageEntities.add(tempList);
				pksSet.add(recordIdMap);
			} else {
				pksSet.add(recordIdMap);
				tempList.add(messageEntity);
				if (CollectionUtils.isEmpty(splitMessageEntities)) {
					splitMessageEntities.add(tempList);
				}
			}
			preIsDelete = ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageEntity.getOp());
			preIsInsert = ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageEntity.getOp());
		}

		return splitMessageEntities;
	}

	public static String messagePks(MessageEntity message) {
		StringBuilder pks = new StringBuilder();
		Map<String, Object> record = ConnectorConstant.MESSAGE_OPERATION_DELETE.endsWith(message.getOp()) ? message.getBefore() : message.getAfter();
		Mapping mapping = message.getMapping();
		if (MapUtils.isNotEmpty(record) && mapping != null) {
			List<Map<String, String>> matchCondition = mapping.getMatch_condition();
			List<Map<String, String>> joinCondition = mapping.getJoin_condition();
			if (CollectionUtils.isNotEmpty(matchCondition)) {
				for (Map<String, String> condition : matchCondition) {
					for (String key : condition.keySet()) {
						Object valueByKey = MapUtil.getValueByKey(record, key);
						pks.append(key).append("=").append(valueByKey);
					}
				}
			} else if (CollectionUtils.isNotEmpty(joinCondition)) {
				for (Map<String, String> condition : joinCondition) {
					for (String key : condition.keySet()) {
						Object valueByKey = MapUtil.getValueByKey(record, key);
						pks.append(key).append("=").append(valueByKey);
					}
				}
			}
		}

		return pks.toString();
	}

	public static void convertMessageOp(MessageEntity fromMessage, String toOp) {
		if (fromMessage == null || StringUtils.isEmpty(toOp)) {
			return;
		}

		final OperationType toOpType = OperationType.fromOp(toOp);
		if (toOpType == null) {
			return;
		}

		if (toOp.equals(fromMessage.getOp())) {
			return;
		}
		final Map<String, Object> after = fromMessage.getAfter();
		final Map<String, Object> before = fromMessage.getBefore();
		switch (toOpType) {
			case INSERT:
				if (MapUtils.isNotEmpty(after) || MapUtils.isNotEmpty(before)) {
					fromMessage.setOp(OperationType.INSERT.getOp());
					fromMessage.setBefore(null);
					fromMessage.setAfter(
							MapUtils.isNotEmpty(after) ? after : before
					);
				} else {
					throw new RuntimeException(
							String.format(
									"Message before and after is null, cannot be convert to insert event, message %s",
									fromMessage.toString()
							)
					);
				}
				break;
			case UPDATE:

				if (MapUtils.isNotEmpty(after) || MapUtils.isNotEmpty(before)) {
					fromMessage.setOp(OperationType.UPDATE.getOp());
					fromMessage.setAfter(
							MapUtils.isNotEmpty(after) ? after : before
					);
					fromMessage.setBefore(
							MapUtils.isNotEmpty(before) ? before : after
					);
				} else {
					throw new RuntimeException(
							String.format(
									"Message before and after is null, cannot be convert to update event, message %s",
									fromMessage.toString()
							)
					);
				}

				break;
			case DELETE:
				if (MapUtils.isNotEmpty(after) || MapUtils.isNotEmpty(before)) {
					fromMessage.setOp(OperationType.DELETE.getOp());
					fromMessage.setAfter(null);
					fromMessage.setBefore(
							MapUtils.isNotEmpty(before) ? before : after
					);
				} else {
					throw new RuntimeException(
							String.format(
									"Message before and after is null, cannot be convert to update event, message %s",
									fromMessage.toString()
							)
					);
				}
				break;
			default:
				throw new RuntimeException(String.format("Unsupported convert message op from %s to %s.", fromMessage.getOp(), toOp));
		}

	}

	private static boolean deletePredicate(boolean preIsDelete, MessageEntity messageEntity, boolean byDelete) {
		return byDelete && ((ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageEntity.getOp()) && !preIsDelete)
				|| (!ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageEntity.getOp()) && preIsDelete));
	}

	private static boolean insertPredicate(boolean preInsert, MessageEntity messageEntity, boolean byInsert) {
		return byInsert && ((ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageEntity.getOp()) && !preInsert)
				|| (!ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageEntity.getOp()) && preInsert));
	}

	private static boolean joinKeyPredicate(
			Set<Map<String, Object>> recordIdSet,
			MessageEntity messageEntity,
			boolean byJoinKey,
			Map<String, Object> recordIdMap
	) {
		// 不需要按照join key拆分
		if (!byJoinKey) {
			return false;
		}

		// 消息没有mapping的情况 不考虑
		Mapping mapping = messageEntity.getMapping();
		if (mapping == null || CollectionUtils.isEmpty(recordIdSet)) {
			return false;
		}

		List<Map<String, String>> joinConditions = mapping.getJoin_condition();
		if (CollectionUtils.isEmpty(joinConditions)) {
			return false;
		}
		Map<String, Object> after = messageEntity.getAfter();
		if (MapUtils.isEmpty(after)) {
			return false;
		}

		boolean recordExists = recordIdSet.contains(recordIdMap);
		if (!recordExists) {
			recordIdSet.add(recordIdMap);
		}

		return recordExists;
	}

	private static Map<String, Object> getRecordIdMap(
			String replacement,
			Map<String, Object> after,
			Mapping mapping
	) {

		if (mapping == null || MapUtils.isEmpty(after)) {
			return null;
		}

		String toTable = mapping.getTo_table();
		List<Map<String, String>> joinConditions = mapping.getJoin_condition();

		if (CollectionUtils.isEmpty(joinConditions) || StringUtils.isBlank(toTable)) {
			return null;
		}

		Map<String, Object> joinKeyMap = new HashMap<>();
		for (Map<String, String> joinCondition : joinConditions) {
			for (String fieldName : joinCondition.values()) {
				Object value = MapUtil.getValueByKey(after, fieldName, replacement);
				joinKeyMap.put(fieldName, value);
			}
		}
		Map<String, Object> recordIdMap = new HashMap<String, Object>() {{
			put("toTable", toTable);
			put("joinKeyMap", joinKeyMap);
		}};


		return recordIdMap;
	}

	public static <T> void splitGroupByDmlOp(
			List<MessageEntity> messageEntities,
			Consumer<List<MessageEntity>> insertConsumer,
			Consumer<List<MessageEntity>> updateConsumer,
			Consumer<List<MessageEntity>> deleteConsumer,
			Consumer<List<MessageEntity>> notDmlConsumer,
			T t, Predicate<T> stop
	) {
		if (CollectionUtils.isEmpty(messageEntities)) {
			return;
		}
		String preOp = messageEntities.get(0).getOp();
		List<MessageEntity> cacheMsgs = new ArrayList<>();
		for (MessageEntity messageEntity : messageEntities) {

			if (t != null && stop != null && stop.test(t)) {
				break;
			}

			String op = messageEntity.getOp();

			if (!op.equals(preOp)) {
				consumerMsgs(insertConsumer, updateConsumer, deleteConsumer, notDmlConsumer, preOp, cacheMsgs);
			}

			cacheMsgs.add(messageEntity);
			preOp = op;
		}

		if (CollectionUtils.isNotEmpty(cacheMsgs)) {
			consumerMsgs(insertConsumer, updateConsumer, deleteConsumer, notDmlConsumer, preOp, cacheMsgs);
		}
	}

	private static void consumerMsgs(
			Consumer<List<MessageEntity>> insertConsumer,
			Consumer<List<MessageEntity>> updateConsumer,
			Consumer<List<MessageEntity>> deleteConsumer,
			Consumer<List<MessageEntity>> notDmlConsumer,
			String preOp, List<MessageEntity> cacheMsgs) {
		OperationType operationType = OperationType.fromOp(preOp);
		switch (operationType) {
			case INSERT:
			case ABSOLUTE_INSERT:
				insertConsumer.accept(cacheMsgs);
				break;
			case UPDATE:
				updateConsumer.accept(cacheMsgs);
				break;
			case DELETE:
				deleteConsumer.accept(cacheMsgs);
				break;
			default:
				notDmlConsumer.accept(cacheMsgs);
				break;
		}
		cacheMsgs.clear();
	}

	public static void main(String[] args) {
		List<MessageEntity> msgs = new ArrayList<>();
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T1", "T1"
		));
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T1", "T1"
		));
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T2", "T2"
		));
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T1", "T1"
		));
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T3", "T3"
		));
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T3", "T3"
		));
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T3", "T3"
		));
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T2", "T2"
		));
		msgs.add(new MessageEntity(
				"i", new HashMap<>(), "T2", "T2"
		));

		dispatcherMessage(new ArrayList<>(msgs), false, pMsgs -> {
			System.out.println(pMsgs.get(0).getTargetStageId() + ": " + pMsgs.size());
		}, msg -> {
		});
		dispatcherMessage(new ArrayList<>(msgs), true, pMsgs -> {
			System.out.println(pMsgs.get(0).getTargetStageId() + ": " + pMsgs.size());
		}, msg -> {
		});
	}
}
