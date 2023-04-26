package com.tapdata.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by tapdata on 17/10/2017.
 */
public class Stats implements Serializable, Cloneable {

	private static final Logger logger = LogManager.getLogger(Stats.class);

	public static final String SOURCE_RECEIVED_FIELD_NAME = "source_received";
	public static final String PROCESSED_FIELD_NAME = "processed";
	public static final String TARGET_INSERTED_FIELD_NAME = "target_inserted";
	public static final String TOTAL_UPDATED_FIELD_NAME = "total_updated";
	public static final String TOTAL_DELETED_FIELD_NAME = "total_deleted";
	public static final String TOTAL_FILE_LENGTH_FIELD_NAME = "total_data_size";
	public static final String TOTAL_DATA_QUAILTY_FIELD_NAME = "total_data_quality";


	public static final String VALIDATE_STATS_TOTAL_FIELD_NAME = "total";
	public static final String VALIDATE_STATS_ERROR_FIELD_NAME = "error";
	public static final String VALIDATE_STATS_SUCCESS_FIELD_NAME = "success";

	private static final long serialVersionUID = -2121148986767975882L;
	/**
	 * 统计总处理记录
	 */
	private Map<String, Long> total = new HashMap<>();

	/**
	 * 统计每秒记录
	 */
	private Map<String, LinkedList<Long>> per_second = new HashMap<>(4);

	/**
	 * 统计每分钟记录
	 */
	private Map<String, Long> per_minute = new HashMap<>(4);

	/**
	 * validate stats
	 */
	private Map<String, Long> validate_stats = new HashMap<>(3);

	/**
	 * key: stageId
	 * value: stats info
	 */
	private List<StageRuntimeStats> stageRuntimeStats;

	/**
	 * key: stageId
	 * value: dataCount
	 **/
	private List<Map<String, Object>> totalCount;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private List<InitialStat> initialStats;
	private boolean initialStatsEnable = true;

	public Stats() {
	}

	public void setTotal(Map<String, Long> total) {
		this.total = total;
	}

	public Map<String, Long> getTotal() {
		return total;
	}

	public Map<String, LinkedList<Long>> getPer_second() {
		return per_second;
	}

	public void setPer_second(Map<String, LinkedList<Long>> per_second) {
		this.per_second = per_second;
	}

	public Map<String, Long> getPer_minute() {
		return per_minute;
	}

	public void setPer_minute(Map<String, Long> per_minute) {
		this.per_minute = per_minute;
	}

	public Map<String, Long> getValidate_stats() {
		return validate_stats;
	}

	public void setValidate_stats(Map<String, Long> validate_stats) {
		this.validate_stats = validate_stats;
	}

	public void initStats() {
		total.put(SOURCE_RECEIVED_FIELD_NAME, 0L);
		total.put(PROCESSED_FIELD_NAME, 0L);
		total.put(TARGET_INSERTED_FIELD_NAME, 0L);
		total.put(TOTAL_DELETED_FIELD_NAME, 0L);
		total.put(TOTAL_UPDATED_FIELD_NAME, 0L);

		per_minute.put(SOURCE_RECEIVED_FIELD_NAME, 0L);
		per_minute.put(PROCESSED_FIELD_NAME, 0L);
		per_minute.put(TARGET_INSERTED_FIELD_NAME, 0L);
		per_minute.put(TOTAL_DELETED_FIELD_NAME, 0L);
		per_minute.put(TOTAL_UPDATED_FIELD_NAME, 0L);

		LinkedList<Long> linkedList = new LinkedList<>();
		for (int i = 0; i < 20; i++) {
			linkedList.add(0L);
		}
		per_second.put(SOURCE_RECEIVED_FIELD_NAME, linkedList);
		per_second.put(PROCESSED_FIELD_NAME, linkedList);
		per_second.put(TARGET_INSERTED_FIELD_NAME, linkedList);
		per_second.put(TOTAL_DELETED_FIELD_NAME, linkedList);
		per_second.put(TOTAL_UPDATED_FIELD_NAME, linkedList);

		validate_stats.put(VALIDATE_STATS_ERROR_FIELD_NAME, 0L);
		validate_stats.put(VALIDATE_STATS_TOTAL_FIELD_NAME, 0L);
		validate_stats.put(VALIDATE_STATS_SUCCESS_FIELD_NAME, 0L);
	}

	public void initStats(List<Stage> stages) {
		initStats();

		this.stageRuntimeStats = initStageStats(stages);
	}

	public static List<StageRuntimeStats> initStageStats(List<Stage> stages) {
		List<StageRuntimeStats> stageRuntimeStats = null;
		if (CollectionUtils.isNotEmpty(stages)) {
			stageRuntimeStats = new ArrayList<>();
			for (Stage stage : stages) {
				StageRuntimeStats stageRuntimeStat = new StageRuntimeStats();
				String stageId = stage.getId();
				stageRuntimeStat.setStageId(stageId);
				stageRuntimeStats.add(stageRuntimeStat);
			}
		}

		return stageRuntimeStats;
	}

	public void initInitialCount(Job job, Connections sourceConn, Connections targetConn) {
		if (job == null || sourceConn == null || targetConn == null) {
			return;
		}

		String sourceDatabaseType = sourceConn.getDatabase_type();

		if (StringUtils.isNotBlank(DatabaseTypeEnum.fromString(sourceDatabaseType).getSqlSelectCount())
				|| StringUtils.equalsAny(sourceDatabaseType, DatabaseTypeEnum.MONGODB.getType())) {
			List<Mapping> mappings = job.getMappings();
			if (job.needInitial()) {
				// 需要全量同步，则初始化进度统计
				if (this.initialStats == null) {
					this.initialStats = new ArrayList<>();
				}
				boolean syncTypeAndOffsetNeedInitial = job.checkSyncTypeAndOffsetNeedInitial();
				boolean hasInitialOffset = job.hasInitialOffset();
				AtomicInteger loopCounter = new AtomicInteger(0);
				mappings.forEach(mapping -> {

					InitialStat findInitialStat = this.initialStats.stream().filter(initialStat ->
							StringUtils.equals(initialStat.getSourceConnectionId(), sourceConn.getId())
									&& StringUtils.equals(initialStat.getTargetConnectionId(), targetConn.getId())
									&& StringUtils.equals(initialStat.getSourceTableName(), mapping.getFrom_table())
									&& StringUtils.equals(initialStat.getTargetTableName(), mapping.getTo_table())
					).findFirst().orElse(null);

					if (findInitialStat == null) {
						// InitialStat not exists, add new one
						this.initialStats.add(new InitialStat(
								sourceConn.getId(), sourceConn.getName(),
								targetConn.getId(), targetConn.getName(),
								mapping.getFrom_table(), mapping.getTo_table(),
								StringUtils.isBlank(sourceConn.getDatabase_name()) ? sourceConn.getName() : sourceConn.getDatabase_name(),
								StringUtils.isBlank(targetConn.getDatabase_name()) ? targetConn.getName() : targetConn.getDatabase_name(),
								sourceConn.getDatabase_type(), targetConn.getDatabase_type()
						));
					} else {
						if (syncTypeAndOffsetNeedInitial && !hasInitialOffset) {
							// InitialStat exists, but need re initial sync, set target row num to zero
							findInitialStat.setTargetRowNum(0L);
						}
					}
					if (loopCounter.incrementAndGet() % ConnectorConstant.LOOP_BATCH_SIZE == 0) {
						logger.info("Init progress stats: " + loopCounter.get() + "/" + mappings.size());
					}
				});
			}

			if (this.initialStats != null) {
				// 处理被删除的链路
				this.initialStats = initialStats.stream().filter(initialStat -> {
					if (mappings.stream().anyMatch(mapping ->
							StringUtils.equals(initialStat.getSourceConnectionId(), sourceConn.getId())
									&& StringUtils.equals(initialStat.getTargetConnectionId(), targetConn.getId())
									&& StringUtils.equals(initialStat.getSourceTableName(), mapping.getFrom_table())
									&& StringUtils.equals(initialStat.getTargetTableName(), mapping.getTo_table())
					)) {
						return true;
					}
					return false;
				}).collect(Collectors.toList());
			}
		} else {
			this.initialStatsEnable = false;
		}
	}

	public void mergeStageStats(List<StageRuntimeStats> mergeStageRuntimeStats) {
		if (this.stageRuntimeStats == null) {
			this.stageRuntimeStats = mergeStageRuntimeStats;
			return;
		}

		for (StageRuntimeStats stageRuntimeStat : stageRuntimeStats) {
			for (StageRuntimeStats mergeStageRuntimeStat : mergeStageRuntimeStats) {
				if (stageRuntimeStat.getStageId().equals(mergeStageRuntimeStat.getStageId())) {
					stageRuntimeStat.mergeStats(mergeStageRuntimeStat);
					break;
				}
			}
		}
	}

	public void statsStageTransTime(long startProcessTS, long endProcessTS, String stageId) {
		long transTime = endProcessTS - startProcessTS;

		if (stageRuntimeStats == null) return;

		for (StageRuntimeStats stageRuntimeStat : stageRuntimeStats) {
			if (stageId.equals(stageRuntimeStat.getStageId())) {
				stageRuntimeStat.incrementTransTime(transTime);
			}
		}
	}

	@Override
	public Stats clone() throws CloneNotSupportedException {
		Stats cloneObj = (Stats) super.clone();
		cloneObj.setPer_minute(new HashMap<>(this.getPer_minute()));
		cloneObj.setPer_second(new HashMap<>(this.getPer_second()));
		cloneObj.setTotal(new HashMap<>(this.getTotal()));
		cloneObj.setValidate_stats(new HashMap<>(this.getValidate_stats()));

		return cloneObj;
	}

	public List<StageRuntimeStats> getStageRuntimeStats() {
		return stageRuntimeStats;
	}

	public void setStageRuntimeStats(List<StageRuntimeStats> stageRuntimeStats) {
		this.stageRuntimeStats = stageRuntimeStats;
	}

	public List<Map<String, Object>> getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(List<Map<String, Object>> totalCount) {
		this.totalCount = totalCount;
	}

	public List<InitialStat> getInitialStats() {
		return initialStats;
	}

	public void setInitialStats(List<InitialStat> initialStats) {
		this.initialStats = initialStats;
	}

	public Optional<InitialStat> findInitialStat(Connections sourceConn, Connections targetConn, String fromTable, String toTable) {
		return findInitialStat(initialStats, sourceConn.getId(), targetConn.getId(), fromTable, toTable);
	}

	public static Optional<InitialStat> findInitialStat(List<InitialStat> initialStats, Connections sourceConn, Connections targetConn, String fromTable, String toTable) {
		return findInitialStat(initialStats, sourceConn.getId(), targetConn.getId(), fromTable, toTable);
	}

	public static Optional<InitialStat> findInitialStat(List<InitialStat> initialStats, String sourceConnId, String targetConnId, String fromTable, String toTable) {
		if (initialStats == null) {
			return Optional.empty();
		}
		String key = sourceConnId + "_" + fromTable + "_" + targetConnId + "_" + toTable;
		return initialStats.stream()
				.filter(initialStat -> (initialStat.getSourceConnectionId() + "_" + initialStat.getSourceTableName()
						+ "_" + initialStat.getTargetConnectionId() + "_" + initialStat.getTargetTableName()).equals(key))
				.findFirst();
	}

	public boolean isInitialStatsEnable() {
		return initialStatsEnable;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("Stats{");
		sb.append("total=").append(total);
		sb.append(", per_second=").append(per_second);
		sb.append(", per_minute=").append(per_minute);
		sb.append(", validate_stats=").append(validate_stats);
		sb.append('}');
		return sb.toString();
	}
}
