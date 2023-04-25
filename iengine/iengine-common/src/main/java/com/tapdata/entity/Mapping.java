package com.tapdata.entity;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.constant.FileProperty;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.dataflow.CloneFieldProcess;
import com.tapdata.entity.dataflow.LogCollectorSetting;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.SyncPoint;
import io.tapdata.exception.DataFlowException;
import io.tapdata.schema.SchemaList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tapdata.entity.FieldProcess.FieldOp.OP_REMOVE;
import static com.tapdata.entity.FieldProcess.FieldOp.OP_RENAME;

/**
 * @author huangjq
 * @ClassName: Mapping
 * @Description: TODO
 * @date 17-10-20
 * @since 1.0
 */
public class Mapping implements Serializable {

	public static final String FIELD_FILTER_TYPE_RETAINED_FIELD = "retainedField";
	public static final String FIELD_FILTER_TYPE_KEEP_ALL_FIELD = "keepAllFields";
	public static final String FIELD_FILTER_TYPE_DELETE_FIELD = "deleteField";

	private static final long serialVersionUID = 1264990958788314542L;

	private String from_table;

	private String to_table;

	private String relationship;

	private List<FieldProcess> fields_process;

	private String target_path;

	private List<Map<String, String>> join_condition;

	private List<Map<String, String>> match_condition;

	private String custom_sql;

	private String dataFliter;

	private String offset;

	private String script;

	private List<FieldRule> rules;

	private Map<String, Object> table_meta_data;

	private String tableId;

	private Ttl ttl;

	private Boolean array = false;

	private List<Stage> stages;

	private List<String> sourcePKs;

	private boolean manyOneUpsert;

	private String fieldFilter;

	private String fieldFilterType;

	/**
	 * 初始化执行顺序
	 * 等于0：未开启顺序功能
	 * 大于0：开启顺序执行功能
	 */
	private int initialSyncOrder;

	/**
	 * log collect
	 */
	private List<LogCollectorSetting> logCollectorSettings;
	private long logTtl;
	private SyncPoint syncPoint;

	/**
	 * redis hmset key and prefix
	 **/
	private String redisKey;
	private String redisKeyPrefix;

	/**
	 * collection aggregate
	 */
	private String collectionAggrPipeline;

	private boolean dropTarget;

	private FileProperty fileProperty;
	/**
	 * es的index，前端传递过来
	 */
	private String index;

	private String tableType;

	/**
	 * 无主键的同步
	 */
	private boolean noPrimaryKey;

	/**
	 * kafka作为源节点使用的分区id, 可为空
	 */
	private Integer partitionId;

	// 是否后续新增
	private boolean addMapping = false;

	private String syncType;

	private boolean addInitialMappingFinish = false;

	private boolean tranModelVersionControl = false;

	/**
	 * 全部字段名大小写转换, optional: {@link com.tapdata.entity.dataflow.Capitalized}
	 */
	private String fieldsNameTransform;

	public Mapping() {
	}

	public Mapping(String from_table, String to_table, List<Map<String, String>> join_condition) {
		this.from_table = from_table;
		this.to_table = to_table;
		this.join_condition = join_condition;
	}

	public Mapping(RelateDataBaseTable table, boolean noPrimaryKey) {
		this.from_table = table.getTable_name();
		this.to_table = table.getTable_name();
		this.relationship = ConnectorConstant.RELATIONSHIP_ONE_ONE;

		List<Map<String, String>> join_condition = new ArrayList<>();
		List<RelateDatabaseField> fields = table.getFields();
		//  默认取主键作为关联条件
		for (RelateDatabaseField field : fields) {
			if (field.getPrimary_key_position() > 0) {
				Map<String, String> condition = new HashMap<>();
				condition.put("source", field.getField_name());
				condition.put("target", field.getField_name());
				join_condition.add(condition);
			}
		}

		// 无主键则使用唯一索引所为关联条件
		if (CollectionUtils.isEmpty(join_condition)) {
			final List<TableIndex> indices = table.getIndices();
			if (CollectionUtils.isNotEmpty(indices)) {
				for (TableIndex index : indices) {
					if (index.isUnique()) {
						final List<TableIndexColumn> columns = index.getColumns();
						for (TableIndexColumn column : columns) {
							Map<String, String> condition = new HashMap<>();
							condition.put("source", column.getColumnName());
							condition.put("target", column.getColumnName());
							join_condition.add(condition);
						}

						break;
					}
				}
			}
		}

		// 无主键和唯一索引且开启了无主键同步支持则使用去哪字段匹配
		if (noPrimaryKey && CollectionUtils.isEmpty(join_condition)) {
			allFieldsForJoinCondition(join_condition, fields);
			this.noPrimaryKey = noPrimaryKey;
		}

		if (CollectionUtils.isNotEmpty(join_condition)) {
			this.join_condition = join_condition;
		}

	}

	public void noPrimaryJoinIfNeed(boolean noPrimaryKey, List<RelateDataBaseTable> tables) {
		if (noPrimaryKey && CollectionUtils.isEmpty(join_condition) && StringUtils.isNotBlank(this.from_table)) {
			RelateDataBaseTable table = ((SchemaList<String, RelateDataBaseTable>) tables).get(this.from_table);
			List<RelateDatabaseField> fields = table.getFields();
			allFieldsForJoinCondition(join_condition, fields);
			this.noPrimaryKey = noPrimaryKey;
		}
	}

	public void allFieldsForJoinCondition(List<Map<String, String>> joinCondition, List<RelateDatabaseField> fields) {
		for (RelateDatabaseField field : fields) {
			Map<String, String> condition = new HashMap<>();
			condition.put("source", field.getField_name());
			condition.put("target", field.getField_name());
			joinCondition.add(condition);
		}
		this.join_condition = joinCondition;
	}

	public static boolean need2CreateTporigMapping(Job job, Mapping mapping) {

		// 不是one many方式，不需要创建
		if (!ConnectorConstant.RELATIONSHIP_ONE_MANY.equals(mapping.getRelationship())) {
			return false;
		}
		// 目标不是mongo不生成中间表
		Stage targetStage = DataFlowStageUtil.findTargetStageFromStages(mapping.getStages());
		if (null != targetStage && !DatabaseTypeEnum.MONGODB.getType().equals(targetStage.getDatabaseType())) {
			return false;
		}

		String source = job.getConnections().getSource();
		String target = job.getConnections().getTarget();
		if (!source.equals(target)) {
			return true;
		}

		List<Stage> stages = mapping.getStages();
		if (CollectionUtils.isNotEmpty(stages)) {
			// 超过2个节点表示中间包含处理节点，需要创建中间表
			if (stages.size() <= 2) {
				return false;
			}
		}
		return true;
	}

	public Mapping(
			String from_table,
			String to_table,
			String relationship,
			List<Map<String, String>> join_condition,
			List<FieldProcess> fieldsProcess,
			String script,
			int initialSyncOrder,
			String fieldFilter,
			String fieldFilterType
	) {
		this.from_table = from_table;
		this.to_table = to_table;
		this.relationship = relationship;
		this.join_condition = join_condition;
		this.fields_process = fieldsProcess;
		this.script = script;
		this.initialSyncOrder = initialSyncOrder;
		this.fieldFilter = fieldFilter;
		this.fieldFilterType = fieldFilterType;
	}

	public static void sortMapping(List<Mapping> mappings) {
		Collections.sort(mappings, (mapping1, mapping2) -> {

			int order1 = mapping1.getInitialSyncOrder();
			int order2 = mapping2.getInitialSyncOrder();
			if (order1 > 0 && order2 > 0) {
				return Integer.compare(order1, order2);
			} else if (order1 > 0) {
				return -1;
			} else if (order2 > 0) {
				return 1;
			} else {
				String toTable1 = mapping1.getTo_table();
				String toTable2 = mapping2.getTo_table();

				if (toTable1.endsWith(ConnectorConstant.LOOKUP_TABLE_SUFFIX) && toTable2.endsWith(ConnectorConstant.LOOKUP_TABLE_SUFFIX)) {
					return 0;
				}
				if (toTable1.endsWith(ConnectorConstant.LOOKUP_TABLE_SUFFIX)) {
					return 1;
				}
				if (toTable2.endsWith(ConnectorConstant.LOOKUP_TABLE_SUFFIX)) {
					return -1;
				}

				String relationship1 = mapping1.getRelationship();
				String relationship2 = mapping2.getRelationship();
				if (ConnectorConstant.RELATIONSHIP_ONE_ONE.equals(relationship1) &&
						ConnectorConstant.RELATIONSHIP_ONE_ONE.equals(relationship2)) {
					return 0;
				}

				if (ConnectorConstant.RELATIONSHIP_ONE_ONE.equals(relationship1)) {
					return -1;
				}

				if (ConnectorConstant.RELATIONSHIP_ONE_ONE.equals(relationship2)) {
					return 1;
				}
			}


			return 0;
		});
	}

	public static Mapping joinTableToMapping(
			List<Stage> mappingStages,
			String script,
			List<FieldProcess> fieldProcesses,
			Stage sourceStage,
			Stage stage,
			JoinTable joinTable
	) {
		Mapping mapping = new Mapping();

		mapping.setIndex(stage.getIndex());
		mapping.setTableType(stage.getTable_type());
		if (StringUtils.isNotEmpty(sourceStage.getPartitionId())) {
			mapping.setPartitionId(Integer.valueOf(sourceStage.getPartitionId()));
		}
		String sourceTableName = sourceStage.getTableName();
		String sourcePrimaryKeys = sourceStage.getPrimaryKeys();

		if (StringUtils.isBlank(sourcePrimaryKeys)) {
			StringBuilder sb = new StringBuilder();
			for (Map<String, String> joinKey : joinTable.getJoinKeys()) {
				if (StringUtils.isNotBlank(joinKey.get("source"))) {
					sb.append(",").append(joinKey.get("source"));
				}
			}
			if (sb.toString().length() > 0) {
				sourcePrimaryKeys = sb.substring(1);
			}
		}

		Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(sourceStage.getType());
		Stage.StageTypeEnum targetStageTypeEnum = Stage.StageTypeEnum.fromString(stage.getType());
		switch (stageTypeEnum) {
			case LOG_COLLECT:
				sourceTableName = "v$logmnr_contents";
				sourcePrimaryKeys = "dataFlowId";
				Ttl ttl = new Ttl("insertTs", (int) (sourceStage.getLogTtl() * 24 * 60));
				mapping.setTtl(ttl);
				break;
			case EXCEL:
			case CSV:
			case JSON:
			case XML:
				mapping.setFileProperty(sourceStage.getFileProperty());
				break;
			default:
				break;
		}
		if (StringUtils.isBlank(sourceTableName)) {
			throw new DataFlowException(String.format("Source stage %s's table name cannot be empty.", sourceStage.getName()));
		}

		// kafka only support append mode
		String relationship = targetStageTypeEnum.onlyAppend ? ConnectorConstant.RELATIONSHIP_APPEND : mapping.convertToRelationShip(joinTable.getJoinType());

//		if (StringUtils.isBlank(sourcePrimaryKeys) && !ConnectorConstant.RELATIONSHIP_APPEND.equals(relationship) && !Stage.StageTypeEnum.REDIS.equals(targetStageTypeEnum)) {
//			throw new DataFlowException(String.format("Source stage %s's primary keys cannot be empty.", sourceStage.getName()));
//		}

		mapping.setFieldFilter(sourceStage.getFieldFilter());
		mapping.setFieldFilterType(sourceStage.getFieldFilterType());

		String joinPath = joinTable.getJoinPath();
		mapping.setFrom_table(sourceTableName);
		mapping.setRelationship(relationship);
		mapping.setScript(script);
		mapping.setFileds_process(fieldProcesses);
		mapping.setTarget_path(joinPath);
		mapping.setInitialSyncOrder(
				sourceStage.getEnableInitialOrder() ? sourceStage.getInitialSyncOrder() : 0
		);

		Stage.StageTypeEnum sourceStageTypeEnum = Stage.StageTypeEnum.fromString(sourceStage.getType());
		if (sourceStage.getIsFilter()) {
			switch (sourceStageTypeEnum) {
				case COLLECTION_TYPE:
					mapping.setDataFliter(sourceStage.getFilter());
					break;
				default:
					mapping.setCustom_sql(sourceStage.getSql());
					break;
			}
		}
		if (Stage.StageTypeEnum.COLLECTION_TYPE.equals(sourceStageTypeEnum)
				&& sourceStage.isCollectionAggregate()) {
			mapping.setCollectionAggrPipeline(sourceStage.getCollectionAggrPipeline());
		}
		mapping.setOffset(sourceStage.getInitialOffset());
		mapping.setStages(mappingStages);
		if (StringUtils.isNotBlank(sourcePrimaryKeys)) {
			mapping.setSourcePKs(Arrays.asList(sourcePrimaryKeys.split(",")));
		}
		mapping.setArray(joinTable.getIsArray());

		// log collect
		mapping.setLogCollectorSettings(sourceStage.getLogCollectorSettings());
		mapping.setLogTtl(sourceStage.getLogTtl());
		mapping.setSyncPoint(sourceStage.getSyncPoint());

		mapping.setRedisKey(stage.getRedisKey());
		mapping.setRedisKeyPrefix(stage.getRedisKeyPrefix());

		mapping.setDropTarget(stage.getDropTable());
		mapping.setFieldsNameTransform(stage.getFieldsNameTransform());

		List<Map<String, String>> joinKeys = joinTable.getJoinKeys();
		List<Map<String, String>> joinConditions = getJoinConditionByJoinKey(stage, relationship, joinKeys);
		mapping.setJoin_condition(joinConditions);

		mapping.setTarget_path(joinPath);
		if (ConnectorConstant.RELATIONSHIP_MANY_ONE.equals(relationship)) {

			if (joinPath.split("\\.").length <= 1) {
				mapping.setArray(false);
			}
			List<Map<String, String>> matchConditions = new ArrayList<>();

			String arrayUniqueKey = joinTable.getArrayUniqueKey();
			if (StringUtils.isNotBlank(arrayUniqueKey)) {
				for (String primaryKey : arrayUniqueKey.split(",")) {

					String targetField = joinPath + "." + primaryKey;
					Map<String, String> matchCondition = new HashMap<>();
					matchCondition.put("source", primaryKey);
					matchCondition.put("target", targetField);
					matchConditions.add(matchCondition);
				}
			} else {
				for (String primaryKey : sourcePrimaryKeys.split(",")) {

					String targetField = joinPath + "." + primaryKey;
					Map<String, String> matchCondition = new HashMap<>();
					matchCondition.put("source", primaryKey);
					matchCondition.put("target", targetField);
					matchConditions.add(matchCondition);
				}
			}

			mapping.setManyOneUpsert(joinTable.getManyOneUpsert());
			mapping.setMatch_condition(matchConditions);
		}
		return mapping;
	}

	public static Mapping stagesToMapping(
			List<Stage> mappingStages,
			String script,
			List<FieldProcess> fieldProcesses,
			Stage sourceStage,
			Stage stage,
			Stage.StageTypeEnum stageTypeEnum
	) {
		Mapping mapping = new Mapping();

		List<String> sourcePks = null;
		String toTable = null;
		String sourceTableName = sourceStage.getTableName();
		String sourcePrimaryKeys = sourceStage.getPrimaryKeys();
		Stage.StageTypeEnum sourceStageTypeEnum = Stage.StageTypeEnum.fromString(sourceStage.getType());

		switch (stageTypeEnum) {
			case MEM_CACHE:
				String cacheKeys = stage.getCacheKeys();
				toTable = stage.getCacheName();
				sourcePks = Arrays.asList(cacheKeys.split(","));
				break;
			case REDIS:
				if (StringUtils.isNotBlank(stage.getRedisKey())) {
					sourcePks = Arrays.asList(stage.getRedisKey().split(","));
				}
				toTable = StringUtils.isNotBlank(stage.getTableName()) ? stage.getTableName() : sourceTableName;
				break;
			case DUMMY:
				sourcePrimaryKeys = "_id";
				break;
			case EXCEL:
			case CSV:
			case JSON:
			case XML:
				mapping.setFileProperty(sourceStage.getFileProperty());
				break;
			default:
				switch (sourceStageTypeEnum) {
					case LOG_COLLECT:
						sourceTableName = "v$logmnr_contents";
						sourcePrimaryKeys = "dataFlowId";
						break;
					default:
						break;
				}
				if (StringUtils.isNotBlank(sourcePrimaryKeys)) {
					sourcePks = Arrays.asList(sourcePrimaryKeys.split(","));
				}
				toTable = StringUtils.isNotBlank(stage.getTableName()) ? stage.getTableName() : sourceTableName;
				break;
		}

//    if (StringUtils.isBlank(sourceTableName)) {
//      throw new DataFlowException(String.format("Source stage %s's table name cannot be empty.", sourceStage.getName()));
//    }

//		if (StringUtils.isBlank(sourcePrimaryKeys)
//			&& !Stage.StageTypeEnum.MEM_CACHE.equals(stageTypeEnum)
//			&& !Stage.StageTypeEnum.REDIS.equals(stageTypeEnum)
//			&& !Stage.StageTypeEnum.CUSTOM.equals(stageTypeEnum)
//			&& !Stage.StageTypeEnum.KAFKA.equals(stageTypeEnum)) {
//			throw new DataFlowException(String.format("Source stage %s's primary keys cannot be empty.", sourceStage.getName()));
//		}

		mapping.setFieldFilter(sourceStage.getFieldFilter());
		mapping.setFieldFilterType(sourceStage.getFieldFilterType());

		// kafka only support append mode
		String relationship = stageTypeEnum.onlyAppend ? ConnectorConstant.RELATIONSHIP_APPEND : ConnectorConstant.RELATIONSHIP_ONE_ONE;
		mapping.setFrom_table(sourceTableName);
		mapping.setRelationship(relationship);
		mapping.setScript(script);
		mapping.setFileds_process(fieldProcesses);
		mapping.setTo_table(toTable);
		if (Stage.StageTypeEnum.COLLECTION_TYPE == sourceStageTypeEnum) {
			mapping.setDataFliter(sourceStage.getFilter());
		} else if (sourceStage.getIsFilter()) {
			mapping.setCustom_sql(sourceStage.getSql());
		}

		mapping.setOffset(sourceStage.getInitialOffset());
		mapping.setStages(mappingStages);
		mapping.setSourcePKs(sourcePks);

		mapping.setInitialSyncOrder(
				sourceStage.getEnableInitialOrder() ? sourceStage.getInitialSyncOrder() : 0
		);

		// log collect
		mapping.setLogCollectorSettings(sourceStage.getLogCollectorSettings());
		mapping.setLogTtl(sourceStage.getLogTtl());
		mapping.setSyncPoint(sourceStage.getSyncPoint());

		List<Map<String, String>> joinConditions = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(mapping.getSourcePKs())) {
			for (String pk : mapping.getSourcePKs()) {
				Map<String, String> joinCondition = new HashMap<>();
				joinCondition.put("source", pk);
				joinCondition.put("target", pk);
				joinConditions.add(joinCondition);
			}
		}

		mapping.setJoin_condition(joinConditions);
		mapping.setRedisKey(stage.getRedisKey());
		mapping.setRedisKeyPrefix(stage.getRedisKeyPrefix());

		mapping.setDropTarget(stage.getDropTable());
		mapping.setFieldsNameTransform(stage.getFieldsNameTransform());

		//设置es的index
		mapping.setIndex(stage.getIndex());
		mapping.setTableType(stage.getTable_type());

		if (StringUtils.isNotEmpty(sourceStage.getPartitionId())) {
			mapping.setPartitionId(Integer.valueOf(sourceStage.getPartitionId()));
		}

		return mapping;
	}

	private static List<Map<String, String>> getJoinConditionByJoinKey(Stage stage, String joinType, List<Map<String, String>> joinKeys) {
		List<Map<String, String>> joinConditions = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(joinKeys)) {
			for (Map<String, String> joinKey : joinKeys) {
				if (MapUtils.isNotEmpty(joinKey) && !StringUtils.isAllBlank(joinKey.get("target"), joinKey.get("source"))) {
					Map<String, String> joinCondition = new HashMap<>();
					joinCondition.putAll(joinKey);
					joinConditions.add(joinCondition);
				}
			}

		} else if (ConnectorConstant.RELATIONSHIP_APPEND.equals(joinType)) {
			String primaryKeys = stage.getPrimaryKeys();
			if (StringUtils.isNotBlank(primaryKeys)) {
				String[] split = primaryKeys.split(",");
				for (String pk : split) {
					Map<String, String> joinCondition = new HashMap<>();
					joinCondition.put("source", pk);
					joinCondition.put("target", pk);
					joinConditions.add(joinCondition);
				}
			}
		}
		return joinConditions;
	}

	public int getInitialSyncOrder() {
		return initialSyncOrder;
	}

	public void setInitialSyncOrder(int initialSyncOrder) {
		this.initialSyncOrder = initialSyncOrder;
	}

	public String getFrom_table() {
		return from_table;
	}

	public void setFrom_table(String from_table) {
		this.from_table = from_table;
	}

	public String getTo_table() {
		return to_table;
	}

	public void setTo_table(String to_table) {
		this.to_table = to_table;
	}

	public String getRelationship() {
		return relationship;
	}

	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}

	public String getTarget_path() {
		return target_path;
	}

	public void setTarget_path(String target_path) {
		this.target_path = target_path;
	}

	public List<Map<String, String>> getJoin_condition() {
		return join_condition;
	}

	public void setJoin_condition(List<Map<String, String>> join_condition) {
		this.join_condition = join_condition;
	}

	public List<Map<String, String>> getMatch_condition() {
		return match_condition;
	}

	public void setMatch_condition(List<Map<String, String>> match_condition) {
		this.match_condition = match_condition;
	}

	public String getCustom_sql() {
		return custom_sql;
	}

	public void setCustom_sql(String custom_sql) {
		this.custom_sql = custom_sql;
	}

	public String getOffset() {
		return offset;
	}

	public void setOffset(String offset) {
		this.offset = offset;
	}

	public List<FieldProcess> getFields_process() {
		return fields_process;
	}

	public void setFileds_process(List<FieldProcess> fileds_process) {
		this.fields_process = fileds_process;
	}


	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public List<FieldRule> getRules() {
		return rules;
	}

	public void setRules(List<FieldRule> rules) {
		this.rules = rules;
	}

	public Map<String, Object> getTable_meta_data() {
		return table_meta_data;
	}

	public void setTable_meta_data(Map<String, Object> table_meta_data) {
		this.table_meta_data = table_meta_data;
	}

	public String getTableId() {
		return tableId;
	}

	public void setTableId(String tableId) {
		this.tableId = tableId;
	}


	public Ttl getTtl() {
		return ttl;
	}

	public void setTtl(Ttl ttl) {
		this.ttl = ttl;
	}

	public Boolean getArray() {
		return array;
	}

	public void setArray(Boolean array) {
		this.array = array;
	}

	public List<Stage> getStages() {
		return stages;
	}

	public void setStages(List<Stage> stages) {
		this.stages = stages;
	}

	public String getDataFliter() {
		return dataFliter;
	}

	public void setDataFliter(String dataFliter) {
		this.dataFliter = dataFliter;
	}

	public boolean getManyOneUpsert() {
		return manyOneUpsert;
	}

	public void setManyOneUpsert(boolean manyOneUpsert) {
		this.manyOneUpsert = manyOneUpsert;
	}

	public String convertToRelationShip(String joinType) {
		switch (joinType) {
			case "upsert":
				this.relationship = ConnectorConstant.RELATIONSHIP_ONE_ONE;
				break;
			case "update":
				this.relationship = ConnectorConstant.RELATIONSHIP_ONE_MANY;
				break;
			case "merge_embed":
				this.relationship = ConnectorConstant.RELATIONSHIP_MANY_ONE;
				break;
			case "append":
				this.relationship = ConnectorConstant.RELATIONSHIP_APPEND;
				break;
		}

		return this.relationship;
	}

	public List<String> getSourcePKs() {
		return sourcePKs;
	}

	public void setSourcePKs(List<String> sourcePKs) {
		this.sourcePKs = sourcePKs;
	}

	public String getFieldFilter() {
		return fieldFilter;
	}

	public void setFieldFilter(String fieldFilter) {
		this.fieldFilter = fieldFilter;
	}

	public String getFieldFilterType() {
		return fieldFilterType;
	}

	public void setFieldFilterType(String fieldFilterType) {
		this.fieldFilterType = fieldFilterType;
	}

	public List<LogCollectorSetting> getLogCollectorSettings() {
		return logCollectorSettings;
	}

	public void setLogCollectorSettings(List<LogCollectorSetting> logCollectorSettings) {
		this.logCollectorSettings = logCollectorSettings;
	}

	public long getLogTtl() {
		return logTtl;
	}

	public void setLogTtl(long logTtl) {
		this.logTtl = logTtl;
	}

	public SyncPoint getSyncPoint() {
		return syncPoint;
	}

	public void setSyncPoint(SyncPoint syncPoint) {
		this.syncPoint = syncPoint;
	}

	public String getRedisKey() {
		return redisKey;
	}

	public String getRedisKeyPrefix() {
		return redisKeyPrefix;
	}

	public void setRedisKey(String redisKey) {
		this.redisKey = redisKey;
	}

	public void setRedisKeyPrefix(String redisKeyPrefix) {
		this.redisKeyPrefix = redisKeyPrefix;
	}

	public String getCollectionAggrPipeline() {
		return collectionAggrPipeline;
	}

	public void setCollectionAggrPipeline(String collectionAggrPipeline) {
		this.collectionAggrPipeline = collectionAggrPipeline;
	}

	public boolean getDropTarget() {
		return dropTarget;
	}

	public void setDropTarget(boolean dropTarget) {
		this.dropTarget = dropTarget;
	}

	public FileProperty getFileProperty() {
		return fileProperty;
	}

	public void setFileProperty(FileProperty fileProperty) {
		this.fileProperty = fileProperty;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getTableType() {
		return StringUtils.isEmpty(tableType) ? "" : tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public boolean getNoPrimaryKey() {
		return noPrimaryKey;
	}

	public void setNoPrimaryKey(boolean noPrimaryKey) {
		this.noPrimaryKey = noPrimaryKey;
	}

	public Integer getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(Integer partitionId) {
		this.partitionId = partitionId;
	}

	public boolean isAddMapping() {
		return addMapping;
	}

	public void setAddMapping(boolean addMapping) {
		this.addMapping = addMapping;
	}

	public String getSyncType() {
		return syncType;
	}

	public void setSyncType(String syncType) {
		this.syncType = syncType;
	}

	public boolean isAddInitialMappingFinish() {
		return addInitialMappingFinish;
	}

	public void setAddInitialMappingFinish(boolean addInitialMappingFinish) {
		this.addInitialMappingFinish = addInitialMappingFinish;
	}

	public boolean checkAddInitialMapping() {
		return isAddMapping() && syncType.equals(ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC) && !addInitialMappingFinish;
	}

	public boolean isTranModelVersionControl() {
		return tranModelVersionControl;
	}

	public void setTranModelVersionControl(boolean tranModelVersionControl) {
		this.tranModelVersionControl = tranModelVersionControl;
	}

	public String getFieldsNameTransform() {
		return fieldsNameTransform;
	}

	public void setFieldsNameTransform(String fieldsNameTransform) {
		this.fieldsNameTransform = fieldsNameTransform;
	}

	@Override
	public String toString() {
		return "Mapping{" +
				"from_table='" + from_table + '\'' +
				", to_table='" + to_table + '\'' +
				", relationship='" + relationship + '\'' +
				", target_path='" + target_path + '\'' +
				", join_condition=" + join_condition +
				", match_condition=" + match_condition +
				", custom_sql='" + custom_sql + '\'' +
				", offset='" + offset + '\'' +
				", script='" + script + '\'' +
				", rules=" + rules +
				", table_meta_data=" + table_meta_data +
				", tableId='" + tableId + '\'' +
				", ttl=" + ttl +
				", array=" + array +
				'}';
	}

	public static List<Map<String, String>> reverseConditionMapKeyValue(List<Map<String, String>> conditions) {
		List<Map<String, String>> processedConditions = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(conditions)) {
			for (Map<String, String> condition : conditions) {
				Map<String, String> processedCondition = new HashMap<>();
				if (!StringUtils.isAllBlank(condition.get("target"), condition.get("source"))) {
					processedCondition.put(condition.get("target"), condition.get("source"));
					processedConditions.add(processedCondition);
				}
			}
		}
		return processedConditions;
	}

	public static void initMappingForFieldProcess(List<Mapping> mappings, String mapping_template) {
		for (Mapping mapping : mappings) {
			initMappingForFieldProcess(mapping, mapping_template);
		}
	}

	public static void initMappingForFieldProcess(Mapping mapping, String mapping_template) {
		List<Map<String, String>> conditions = CollectionUtils.isNotEmpty(mapping.getMatch_condition()) ? mapping.getMatch_condition() : mapping.getJoin_condition();
		if (ConnectorConstant.MAPPING_TEMPLATE_CUSTOM.equals(mapping_template)) {
			// 表同步任务，字段处理器改主键名
			customRenamePK(mapping.getFields_process(), conditions);
		} else if (ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE.equals(mapping_template)) {

			final String fieldsNameTransform = mapping.getFieldsNameTransform();
			// 处理迁移字段转大小写
			final Capitalized capitalized = Capitalized.fromValue(fieldsNameTransform);
			if (capitalized != null && (capitalized == Capitalized.UPPER || capitalized == Capitalized.LOWER)) {
				for (Map<String, String> condition : conditions) {
					for (Map.Entry<String, String> entry : condition.entrySet()) {
						final String sourcePK = entry.getValue();
						condition.remove(entry.getKey());
						String newField = Capitalized.convert(entry.getKey(), fieldsNameTransform);
						condition.put(newField, sourcePK);
					}
				}
			}

			// 迁移任务，字段映射改主键名
			cloneRenamePK(mapping.getStages(), mapping.getFrom_table(), conditions);
		}
	}

	private static void cloneRenamePK(List<Stage> stages, String fromTable, List<Map<String, String>> conditions) {

		if (CollectionUtils.isEmpty(stages)) {
			return;
		}
		// 找到字段处理节点
		Stage fieldProcessStage = stages.stream()
				.filter(stage -> stage.getType().equals(Stage.StageTypeEnum.FIELD_PROCESSOR.getType()) && CollectionUtils.isNotEmpty(stage.getField_process()))
				.findFirst().orElse(null);
		if (fieldProcessStage == null) {
			return;
		}
		// 找到对应的表的字段处理
		List<CloneFieldProcess> fieldProcess = fieldProcessStage.getField_process();
		CloneFieldProcess cloneFieldProcess = fieldProcess.stream().filter(fp -> fp.getTable_name().equals(fromTable)).findFirst().orElse(null);
		if (cloneFieldProcess == null) {
			return;
		}

		List<FieldProcess> operations = cloneFieldProcess.getOperations();
		customRenamePK(operations, conditions);
	}

	private static void customRenamePK(List<FieldProcess> fieldProcesses, List<Map<String, String>> conditions) {
		if (CollectionUtils.isEmpty(fieldProcesses)) {
			return;
		}
		for (FieldProcess process : fieldProcesses) {
			String op = process.getOp();

			if (FieldProcess.FieldOp.fromOperation(op) == OP_RENAME) {
				String field = process.getField();
				if (StringUtils.isBlank(field)) {
					continue;
				}
				String newFiled = process.getOperand();
				for (Map<String, String> condition : conditions) {
					for (Map.Entry<String, String> entry : condition.entrySet()) {
						String sourcePK = entry.getValue();
						if (field.equals(sourcePK)) {
							condition.remove(entry.getKey());
							condition.put(newFiled, sourcePK);
						}
					}
				}
			} else if (FieldProcess.FieldOp.fromOperation(op) == OP_REMOVE) {
				String field = process.getField();
				for (Map<String, String> condition : conditions) {
					for (Map.Entry<String, String> entry : condition.entrySet()) {
						String sourcePK = entry.getValue();
						if (field.equals(sourcePK)) {
							condition.remove(entry.getKey());
						}
					}
				}
			}
		}
		// delete the empty condition
		conditions.removeIf(item -> item.size() == 0);
	}
}
