package io.tapdata.flow.engine.V2.node.hazelcast.processor.join;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.InMemoryKVStore;
import io.tapdata.construct.KVStoreFactory;
import io.tapdata.construct.constructImpl.BytesIMap;
import io.tapdata.construct.constructImpl.KVStore;
import io.tapdata.entity.codec.filter.EntryFilter;
import io.tapdata.entity.codec.filter.MapIteratorEx;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.error.TaskProcessorExCode_11;
import io.tapdata.exception.NodeException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.processor.HazelcastProcessorBaseNode;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor.JoinOperation.Delete;
import static io.tapdata.flow.engine.V2.node.hazelcast.processor.join.HazelcastJoinProcessor.JoinOperation.Upsert;

/**
 * @author jackin
 * @date 2021/12/15 12:08 PM
 **/
public class HazelcastJoinProcessor extends HazelcastProcessorBaseNode {

	private final Logger logger = LogManager.getLogger(HazelcastJoinProcessor.class);

	private JoinType joinType;


	private String referenceId;
	private KVStore<Map<String, Object>> leftRowCache;
	private KVStore<String> leftPKCache;
	private KVStore<Map<String, Object>> rightRowCache;
	private KVStore<String> rightPKCache;

	private List<String> leftJoinKeyFields;
	private List<String> rightJoinKeyFields;
	private String embeddedPath;
	private String leftNodeId;
	private String rightNodeId;
	private Context context;

	private List<String> leftPrimaryKeys;
	private List<String> rightPrimaryKeys;
	private Map<String, TapTable> tapTables;
	private TapTable leftTapTable = null;
	private TapTable rightTapTable = null;
	private MapIteratorEx mapIterator;

	public HazelcastJoinProcessor(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
		tapTables = new HashMap<>();
		this.referenceId = referenceId(processorBaseContext.getNode());
		tapTables = processorBaseContext.getTapTableMap();
	}

	private void pkChecker() {
		if (CollectionUtils.isEmpty(leftPrimaryKeys)) {
			throw new NodeException("Join processor missing left table primary key");
		}
		if (CollectionUtils.isEmpty(rightPrimaryKeys)) {
			throw new NodeException("Join processor missing right table primary key");
		}
	}

	@Override
	public void doInit(@Nonnull Context context) throws TapCodeException {
		super.doInit(context);
		this.context = context;
		this.mapIterator = new AllLayerMapIterator();
		initNode();
	}

	private static String referenceId(Node<?> node) {
		return String.format("%s-%s-%s", HazelcastJoinProcessor.class.getSimpleName(), node.getTaskId(), node.getId());
	}

	public static void clearCache(Node<?> node) {
		if (!(node instanceof JoinProcessorNode)) return;
		String leftNodeId = ((JoinProcessorNode) node).getLeftNodeId();
		String rightNodeId = ((JoinProcessorNode) node).getRightNodeId();
		HazelcastInstance hazelcastInstance = HazelcastUtil.getInstance();
		String leftJoinCacheMapName = joinCacheMapName(leftNodeId, "leftJoinCache");
		ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(node);

		CommonUtils.handleAnyErrors(null, () -> {
			BytesIMap<Map<String, Map<String, Object>>> leftJoinCache = new BytesIMap<>(
				hazelcastInstance,
				referenceId(node),
				leftJoinCacheMapName,
				externalStorage
			);
			try {
				leftJoinCache.clear();
				leftJoinCache.destroy();
			} catch (Exception e) {
				throw new RuntimeException(String.format("Clear left join cache map occur an error: %s\n map name: %s", e.getMessage(), leftJoinCacheMapName), e);
			}
		}, () -> {
			String rightJoinCacheMapName = joinCacheMapName(rightNodeId, "rightCache");
			BytesIMap<Map<String, Map<String, Object>>> rightJoinCache = new BytesIMap<>(
				hazelcastInstance,
				referenceId(node),
				rightJoinCacheMapName,
				externalStorage
			);
			try {
				rightJoinCache.clear();
				rightJoinCache.destroy();
			} catch (Exception e) {
				throw new RuntimeException(String.format("Clear right join cache map occur an error: %s\n map name: %s", e.getMessage(), rightJoinCacheMapName), e);
			}
		});
	}

	public void initNode() throws TapCodeException {
		Node<?> node = processorBaseContext.getNode();
		if (verifyJoinNode(node)) {
			vatidate(node);
		}
		JoinProcessorNode joinNode = (JoinProcessorNode) node;
		this.joinType = JoinType.get(joinNode.getJoinType());

		final Boolean embeddedMode = joinNode.getEmbeddedMode();
		if (embeddedMode) {
			final JoinProcessorNode.EmbeddedSetting embeddedSetting = joinNode.getEmbeddedSetting();
			this.embeddedPath = embeddedSetting.getPath();
		}
		final List<JoinProcessorNode.JoinExpression> joinExpressions = joinNode.getJoinExpressions();
		this.leftJoinKeyFields = new ArrayList<>(joinExpressions.size());
		this.rightJoinKeyFields = new ArrayList<>(joinExpressions.size());
		for (JoinProcessorNode.JoinExpression joinExpression : joinExpressions) {
			final String left = joinExpression.getLeft();
			final String right = joinExpression.getRight();
			leftJoinKeyFields.add(left);
			rightJoinKeyFields.add(right);
		}

		this.leftNodeId = joinNode.getLeftNodeId();
		this.rightNodeId = joinNode.getRightNodeId();

		for (String key : tapTables.keySet()) {
			if (leftTapTable == null) {
				leftTapTable = tapTables.get(key);
				continue;
			}
			rightTapTable = tapTables.get(key);
			break;
		}

		this.leftPrimaryKeys = joinNode.getLeftPrimaryKeys();
		this.rightPrimaryKeys = joinNode.getRightPrimaryKeys();
		pkChecker();
		ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(node);
		this.leftRowCache = KVStoreFactory.createKVStore(externalStorage, joinCacheMapName(leftNodeId, "leftJoinCache"));
		this.leftPKCache = KVStoreFactory.createKVStore(externalStorage, joinCacheMapName(leftNodeId, "leftPKCache"));
		this.rightRowCache = KVStoreFactory.createKVStore(externalStorage, joinCacheMapName(rightNodeId, "rightCache"));
		this.rightPKCache = KVStoreFactory.createKVStore(externalStorage, joinCacheMapName(rightNodeId, "rightPKCache"));
		if (!taskHasBeenRun()) {
			try {
				leftRowCache.clear();
				leftPKCache.clear();
				rightRowCache.clear();
				rightPKCache.clear();
			} catch (Exception e) {
				throw new TapCodeException(TaskProcessorExCode_11.UNKNOWN_ERROR, "Clear join cache failed", e);
			}
		}
	}

	@Override
	protected void updateNodeConfig(TapdataEvent tapdataEvent) {
		try {
			initNode();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private boolean verifyJoinNode(Node<?> node) {
		if (node instanceof JoinProcessorNode && (node.disabledNode()
				|| verifyPreNodeHasDisabled(node, ((JoinProcessorNode)node).getLeftNodeId())
				|| verifyPreNodeHasDisabled(node, ((JoinProcessorNode)node).getRightNodeId()))) {
			return false;
		}
		return true;
	}
	private boolean verifyPreNodeHasDisabled(Node<?> currentNode, String verifyNodeId) {
		try {
			if (currentNode instanceof JoinProcessorNode) {
				JoinProcessorNode joinNode = (JoinProcessorNode) currentNode;
				DAG dag = joinNode.getDag();
				if (null != dag) {
					Node<?> preNode = dag.getNode(verifyNodeId);
					if (null != preNode && preNode.disabledNode()) {
						return true;
					}
				}
			}
		} catch (Exception e) {
			return true;
		}
		return false;
	}

	@Override
	protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
		Node<?> node = processorBaseContext.getNode();
		if (logger.isDebugEnabled()) {
			logger.debug(
					"join node [id: {}, name: {}] receive event {}",
					node.getId(),
					node.getName(),
					tapdataEvent
			);
		}
		if (!verifyJoinNode(node)) {
			consumer.accept(tapdataEvent, null);
			return;
		}

		Map<String, Object> before;
		Map<String, Object> after;
		String opType;
		if (!(tapdataEvent.isDML())) {
			consumer.accept(tapdataEvent, null);
			return;
		}
		TapRecordEvent dataEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
		before = TapEventUtil.getBefore(dataEvent);
		after = TapEventUtil.getAfter(dataEvent);
		opType = TapEventUtil.getOp(dataEvent);

		if (opType == null) {
			consumer.accept(tapdataEvent, null);
			return;
		}
		transformDateTime(before, after);
		List<JoinResult> joinResults;
		if (tapdataEvent.getNodeIds().contains(leftNodeId)) {
			joinResults = leftJoinLeftProcess(before, after, opType);
		} else {
			joinResults = leftJoinRightProcess(before, after, opType);
		}

		if (CollectionUtils.isNotEmpty(joinResults)) {
			List<TapdataEvent> tapdataEvents = new ArrayList<>();
			for (JoinResult joinResult : joinResults) {
				TapRecordEvent joinEvent = joinResult2DataEvent(dataEvent, joinResult);
				TapdataEvent joinTapdataEvent = new TapdataEvent();
				joinTapdataEvent.setTapEvent(joinEvent);
				joinTapdataEvent.setSyncStage(tapdataEvent.getSyncStage());
				tapdataEvents.add(joinTapdataEvent);
			}
			if (CollectionUtils.isNotEmpty(tapdataEvents)) {
				if (logger.isDebugEnabled()) {
					logger.debug("join node [id: {}, name: {}] join results {}", node.getId(), node.getName(), tapdataEvents);
				}
				tapdataEvents.forEach(event -> consumer.accept(event, null));
			}
		}
	}

	@Override
	public void doClose() throws TapCodeException {
		super.doClose();
	}

	private void vatidate(Node<?> node) {
	}

	private static String joinCacheMapName(String leftNodeId, String name) {
		return leftNodeId + "-" + name;
	}

	private TapRecordEvent joinResult2DataEvent(TapRecordEvent originEvent, JoinResult joinResult) {
		TapRecordEvent tapRecordEvent = null;
		String opType = joinResult.getOpType();
		OperationType operationType = OperationType.fromOp(opType);
		switch (operationType) {
			case INSERT:
				tapRecordEvent = new TapInsertRecordEvent();
				TapEventUtil.setAfter(tapRecordEvent, joinResult.getAfter());
				break;
			case UPDATE:
				tapRecordEvent = new TapUpdateRecordEvent();
				TapEventUtil.setBefore(tapRecordEvent, joinResult.getBefore());
				TapEventUtil.setAfter(tapRecordEvent, joinResult.getAfter());
				break;
			case DELETE:
				tapRecordEvent = new TapDeleteRecordEvent();
				TapEventUtil.setBefore(tapRecordEvent, joinResult.getBefore());
				break;
			default:
				throw new IllegalArgumentException("operationType " + operationType + " is unexpected while joinResult2DataEvent");
		}
		tapRecordEvent.setTableId(originEvent.getTableId());
		tapRecordEvent.setTime(System.currentTimeMillis());
		tapRecordEvent.setReferenceTime(originEvent.getReferenceTime());

		return tapRecordEvent;
	}

	private void updateCache(
			String table,
			String opType,
			Map<String, Object> before,
			Map<String, Object> after) throws IllegalAccessException, InstantiationException {
		Map<String, Object> before2 = new HashMap<>();
		MapUtil.deepCloneMap(before, before2);
		Map<String, Object> after2 = new HashMap<>();
		MapUtil.deepCloneMap(after, after2);

		KVStore<Map<String, Object>> rowCache;
		KVStore<String> pkCache;
		List<String> primaryKeys;
		List<String> joinKeyFields;

		if ("left".equals(table)) {
			rowCache = leftRowCache;
			pkCache = leftPKCache;
			primaryKeys = leftPrimaryKeys;
			joinKeyFields = leftJoinKeyFields;
		} else {
			rowCache = rightRowCache;
			pkCache = rightPKCache;
			primaryKeys = rightPrimaryKeys;
			joinKeyFields = rightJoinKeyFields;
		}

		if ("insert".equals(opType)) {
			String pk = project(after2, primaryKeys);
			String joinKey = project(after2, joinKeyFields);
			String rowCacheKey = joinKey + "-" + pk;
			rowCache.insert(rowCacheKey, after2);
			pkCache.insert(pk, joinKey);
			return;
		}

		// 不需要为更新设计缓存调整, 更新会按照 插入, 或者 删除+插入的方式进行
		if ("delete".equals(opType)) {
			String pk = project(before2, primaryKeys);
			String joinKey = project(before2, joinKeyFields);
			if (StringUtils.isBlank(joinKey)) {
				joinKey = pkCache.find(pk);
			}
			String rowCacheKey = joinKey + "-" + pk;
			rowCache.delete(rowCacheKey);
			pkCache.delete(pk);
		}
	}

	private List<JoinResult> leftJoinLeftInsertProcess(
			Map<String, Object> after
	) throws Exception {
		List<JoinResult> joinResults = new ArrayList<>();

		// 构建主记录缓存, 以及 PK -> 关联条件的缓存
		updateCache("left", "insert", null, after);

		String joinKey = project(after, leftJoinKeyFields);

		Map<String, Map<String, Object>> rightRows = rightRowCache.findByPrefix(joinKey+"-");
		if (rightRows.isEmpty()) {
			// 如果是 leftJoin, 且没有右关联数据, 则遍历右表属性, 补充 NULL
			if (joinType == JoinType.LEFT) {
				for (String rightTableField : rightTapTable.getNameFieldMap().keySet()) {
					after.put(rightTableField, null);
				}
				joinResults.add(new JoinResult(null, after, OperationType.INSERT.getOp()));
				return joinResults;
			}
			// 如果是 innerJoin, 且没有右关联数据, 则返回空
			if (joinType == JoinType.INNER) {
				return joinResults;
			}
		}

		// 将右表属性添加到左表中, 形成完整记录
		for (Map<String, Object> rightRow : rightRows.values()) {
			after.putAll(rightRow);
			joinResults.add(getJoinResult(null, after, OperationType.INSERT.getOp()));
		}
		return joinResults;
	}

	private List<JoinResult> leftJoinRightInsertProcess(
			Map<String, Object> after
	) throws Exception {
		List<JoinResult> joinResults = new ArrayList<>();

		// 构建主记录缓存, 以及 PK -> 关联条件的缓存
		updateCache("right", "insert", null, after);

		String joinKey = project(after, rightJoinKeyFields);

		Map<String, Map<String, Object>> leftRows = leftRowCache.findByPrefix(joinKey+"-");
		if (leftRows.isEmpty()) {
			// 如果主表为空, 则 LEFT Join 与 Inner Join 都返回空
			return joinResults;
		}

		// 将左表属性添加到右表中, 形成完整记录
		// 先将之前可能为 NULL 的 JOIN 数据删掉
		for (Map<String, Object> leftRow : leftRows.values()) {
			Map<String, Object> before3 = new HashMap<>();
			for(String leftPrimaryKey: leftPrimaryKeys) {
				before3.put(leftPrimaryKey, leftRow.get(leftPrimaryKey));
			}
			for (String rightPrimaryKey : rightPrimaryKeys) {
				before3.put(rightPrimaryKey, null);
			}
			joinResults.add(getJoinResult(before3, null, OperationType.DELETE.getOp()));
		}
		// 再写入新的非 NULL 的数据, 这样做可以保持幂等
		for (Map<String, Object> leftRow : leftRows.values()) {
			Map<String, Object> after2 = new HashMap<>();
			MapUtil.deepCloneMap(after, after2);
			after2.putAll(leftRow);
			joinResults.add(getJoinResult(null, after2, OperationType.INSERT.getOp()));
		}
		return joinResults;
	}

	private List<JoinResult> leftJoinLeftDeleteProcess(
			Map<String, Object> before
	) throws Exception {
		List<JoinResult> joinResults = new ArrayList<>();
		// 删除主记录缓存, 以及 PK -> 关联条件的缓存
		updateCache("left", "delete", before, null);

		// 主表删除, 直接将主表关联的所有主记录直接删除
		joinResults.add(getJoinResult(before, null, OperationType.DELETE.getOp()));
		return joinResults;
	}

	private List<JoinResult> leftJoinRightDeleteProcess(
			Map<String, Object> before
	) throws Exception {
		List<JoinResult> joinResults = new ArrayList<>();

		// 如果 before 数据不全, 先查出 before 的值
		String pk = project(before, rightPrimaryKeys);
		String joinKey = project(before, rightJoinKeyFields);
		if (StringUtils.isBlank(joinKey)) {
			joinKey = rightPKCache.find(pk);
		}

		// 再删除主记录缓存, 以及 PK -> 关联条件的缓存
		updateCache("right", "delete", before, null);

		// 查询所有关联的左表数据
		Map<String, Map<String, Object>> leftRows = leftRowCache.findByPrefix(joinKey+"-");
		if (leftRows.isEmpty()) {
			// 如果没有关联的数据, 直接返回空
			return joinResults;
		}

		for (Map<String, Object> leftRow : leftRows.values()) {
			// 这里注意, 对于 LEFT JOIN, 所以即使没有右表数据, 也要返回一条记录, 且右表数据为 NULL
			// 需要从右表缓存中, 匹配是否还有其他的右表数据
			Map<String, Map<String, Object>> otherRightRows = new HashMap<>();
			if (joinType == JoinType.LEFT) {
				otherRightRows = rightRowCache.findByPrefix(joinKey + "-");
			}
			if (joinType == JoinType.LEFT && otherRightRows.isEmpty()) {
				// 如果没有其他可以匹配的右值, 就设置右值为 NULL, 做一条更新
				Map<String, Object> before2 = new HashMap<>();
				MapUtil.deepCloneMap(before, before2);
				Map<String, Object> after = new HashMap<>();
				for (String field : rightTapTable.getNameFieldMap().keySet()) {
					after.put(field, null);
				}
				for (String leftPrimaryKey : leftPrimaryKeys) {
					before2.put(leftPrimaryKey, leftRow.get(leftPrimaryKey));
				}
				joinResults.add(getJoinResult(before2, after, OperationType.UPDATE.getOp()));
			} else {
				// 如果还有其他的可以匹配的右值, 就直接删除当前匹配到的这一条
				Map<String, Object> before3 = new HashMap<>();
				for (String field : rightPrimaryKeys) {
					before3.put(field, before.get(field));
				}
				for (String field : leftPrimaryKeys) {
					before3.put(field, leftRow.get(field));
				}
				joinResults.add(getJoinResult(before, null, OperationType.DELETE.getOp()));
			}
		}
		return joinResults;
	}

	private List<JoinResult> leftJoinLeftUpdateProcess(
			Map<String, Object> before,
			Map<String, Object> after
	) throws Exception {
		List<JoinResult> joinResults = new ArrayList<>();

		String beforeJoinKey = project(before, leftJoinKeyFields);
		String afterJoinKey = project(after, leftJoinKeyFields);

		// 考虑 join key 发生变化的情况, 发生此情况时, 将视为 删除+插入 两个事情来做
		if (!Objects.equals(beforeJoinKey, afterJoinKey)) {
			joinResults.addAll(leftJoinLeftDeleteProcess(before));
		}
		joinResults.addAll(leftJoinLeftInsertProcess(after));
		return joinResults;
	}

	private List<JoinResult> leftJoinRightUpdateProcess(
			Map<String, Object> before,
			Map<String, Object> after
	) throws Exception {
		List<JoinResult> joinResults = new ArrayList<>();

		String beforeJoinKey = project(before, rightJoinKeyFields);
		String afterJoinKey = project(after, rightJoinKeyFields);

		// 考虑 join key 发生变化的情况, 发生此情况时, 将视为 删除+插入 两个事情来做
		if (!Objects.equals(beforeJoinKey, afterJoinKey)) {
			joinResults.addAll(leftJoinRightDeleteProcess(before));
		}
		joinResults.addAll(leftJoinRightInsertProcess(after));
		return joinResults;
	}

	@SneakyThrows
	private List<JoinResult> leftJoinLeftProcess(
			Map<String, Object> before,
			Map<String, Object> after,
			String opType
	) {
		OperationType operationType = OperationType.fromOp(opType);
		switch (operationType) {
			case INSERT:
				return leftJoinLeftInsertProcess(after);
			case UPDATE:
				return leftJoinLeftUpdateProcess(before, after);
			case DELETE:
				return leftJoinLeftDeleteProcess(before);
		}
		return null;
	}


	@SneakyThrows
	private List<JoinResult> leftJoinRightProcess(
			Map<String, Object> before,
			Map<String, Object> after,
			String opType
	) {
		OperationType operationType = OperationType.fromOp(opType);
		switch (operationType) {
			case INSERT:
				return leftJoinRightInsertProcess(after);
			case UPDATE:
				return leftJoinRightUpdateProcess(before, after);
			case DELETE:
				return leftJoinRightDeleteProcess(before);
		}
		return null;
	}


	@NotNull
	private JoinResult getJoinResult(Map<String, Object> beforeRow, Map<String, Object> afterRow, String opType) throws IllegalAccessException, InstantiationException {
		Map<String, Object> beforeJoinResult = null;
		if (MapUtils.isNotEmpty(beforeRow)) {
			beforeJoinResult = new HashMap<>();
			MapUtil.deepCloneMap(beforeRow, beforeJoinResult);
		}
		Map<String, Object> afterJoinResult = null;
		if (MapUtils.isNotEmpty(afterRow)) {
			afterJoinResult = new HashMap<>();
			MapUtil.deepCloneMap(afterRow, afterJoinResult);
		}
		return new JoinResult(beforeJoinResult, afterJoinResult, opType);
	}

	public static String project(Map<String, Object> record, List<String> fields) {
		Object[] key = new Object[fields.size()];
		for (int i = 0; i < fields.size(); i++) {
			key[i] = record.get(fields.get(i));
		}
		return Arrays.deepToString(key);
	}

	public void transformDateTime(Map<String, Object> before,Map<String, Object> after) {
		EntryFilter entryFilter = (key, value, recursive) -> {
			if (value instanceof DateTime) {
				return ((DateTime) value).toDate();
			}
			return value;
		};
		mapIterator.iterate(before,entryFilter);
		mapIterator.iterate(after, entryFilter);
	}

	public class JoinResult {
		private Map<String, Object> before;
		private Map<String, Object> after;
		private String opType;

		public JoinResult(Map<String, Object> before, Map<String, Object> after, String opType) {
			this.before = before;
			this.after = after;
			this.opType = opType;
		}

		public Map<String, Object> getBefore() {
			return before;
		}

		public Map<String, Object> getAfter() {
			return after;
		}

		public String getOpType() {
			return opType;
		}
	}

	public enum JoinOperation {
		Insert,
		Update,
		Upsert,
		Delete
	}

	@Override
	protected void handleTransformToTapValueResult(TapdataEvent tapdataEvent) {
		tapdataEvent.setTransformToTapValueResult(null);
	}

	@Override
	public boolean needCopyBatchEventWrapper() {
		return true;
	}
}
