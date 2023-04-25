package com.tapdata.constant;


import com.tapdata.entity.Connections;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.Stage.StageTypeEnum;
import io.tapdata.exception.DataFlowException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jackin
 */
public class DataFlowStageUtil {

	public static boolean isDataStage(String stageType) {
		StageTypeEnum stageTypeEnum = StageTypeEnum.fromString(stageType);
		if (stageTypeEnum == null) {
			return false;
		}

		return "data".equals(stageTypeEnum.parentType);
	}

	public static boolean isProcessorStage(String stageType) {
		StageTypeEnum stageTypeEnum = StageTypeEnum.fromString(stageType);
		if (stageTypeEnum == null) {
			return false;
		}

		return "processor".equals(stageTypeEnum.parentType);
	}

	public static void fillStageConfig(Stage stage, Connections connection) {
		Map<String, List<RelateDataBaseTable>> schema = connection.getSchema();
		if (MapUtils.isEmpty(schema)) {
			throw new DataFlowException(String.format("Source stage %s schema cannot empty.", stage.getName()));
		}
		List<RelateDataBaseTable> tables = schema.get("tables");
		if (CollectionUtils.isEmpty(tables)) {
			throw new DataFlowException(String.format("Source stage %s schema cannot empty.", stage.getName()));
		}

		RelateDataBaseTable relateDataBaseTable = tables.get(0);
		String tableName = relateDataBaseTable.getTable_name();
		StringBuilder pksSB = new StringBuilder();
		for (RelateDatabaseField relateDatabaseField : relateDataBaseTable.getFields()) {
			if (relateDatabaseField.getPrimary_key_position() > 0) {
				pksSB.append(relateDatabaseField.getField_name()).append(",");
			}
		}

		String pks = StringUtils.removeEnd(pksSB.toString(), ",");
		if (StringUtils.isNotBlank(tableName)) {
			stage.setTableName(tableName);
		}
		if (StringUtils.isNotBlank(pks)) {
			stage.setPrimaryKeys(pks);
		}
	}

	public static Stage findSourceStageFromStages(List<Stage> stages) {
		if (CollectionUtils.isEmpty(stages)) return null;

		for (Stage stage : stages) {
			if (CollectionUtils.isEmpty(stage.getInputLanes())) return stage;
			String input = stage.getInputLanes().get(0);
			if (StringUtils.isBlank(input)) return stage;
		}

		Stage sourceStage = recursiveFindSource(stages, stages.get(0).getInputLanes().get(0));

		if (sourceStage == null) return stages.get(0);

		return sourceStage;
	}

	public static Stage findTargetStageFromStages(List<Stage> stages) {
		if (CollectionUtils.isEmpty(stages)) return null;

		for (Stage stage : stages) {
			if (CollectionUtils.isEmpty(stage.getOutputLanes())) return stage;
			String output = stage.getOutputLanes().get(0);
			if (StringUtils.isBlank(output)) return stage;
		}

		Stage targetStage = recursiveFindSource(stages, stages.get(0).getOutputLanes().get(0));

		if (targetStage == null) return stages.get(0);

		return targetStage;
	}

	public static Stage findTargetStageByStageId(String sourceStageId, List<Stage> stages) {
		if (CollectionUtils.isEmpty(stages)) {
			return null;
		}

		Stage sourceStage = null;
		for (Stage stage : stages) {
			if (stage.getId().equals(sourceStageId)) {
				sourceStage = stage;
			}
		}

		if (sourceStage == null) {
			return null;
		}

		List<String> outputLanes = sourceStage.getOutputLanes();
		if (CollectionUtils.isEmpty(outputLanes)) {
			return null;
		}

		for (Stage stage : stages) {
			if (outputLanes.contains(stage.getId())) {
				return stage;
			}
		}

		return null;
	}

	public static List<Stage> findTargetDataStageByStageId(String sourceStageId, List<Stage> stages) {
		List<Stage> results = new ArrayList<>();
		if (CollectionUtils.isEmpty(stages)) {
			return results;
		}

		Stage sourceStage = null;
		for (Stage stage : stages) {
			if (stage.getId().equals(sourceStageId)) {
				sourceStage = stage;
			}
		}

		if (sourceStage == null) {
			return results;
		}

		List<String> outputLanes = sourceStage.getOutputLanes();
		if (CollectionUtils.isEmpty(outputLanes)) {
			return null;
		}

		for (Stage stage : stages) {
			if (outputLanes.contains(stage.getId())) {
				if (DataFlowStageUtil.isDataStage(stage.getType())) {
					results.add(stage);
				} else {
					final List<Stage> targetDataStages = findTargetDataStageByStageId(stage.getId(), stages);
					if (CollectionUtils.isNotEmpty(targetDataStages)) {
						results.addAll(targetDataStages);
					}
				}
			}
		}

		return results;
	}

	public static List<Stage> findReachableStageEndByDataStage(String sourceStageId, List<Stage> stages) {
		List<Stage> results = new ArrayList<>();
		if (CollectionUtils.isEmpty(stages)) {
			return results;
		}

		Stage sourceStage = null;
		for (Stage stage : stages) {
			if (stage.getId().equals(sourceStageId)) {
				sourceStage = stage;
				break;
			}
		}

		if (sourceStage == null) {
			return results;
		}

		List<String> outputLanes = sourceStage.getOutputLanes();
		if (CollectionUtils.isEmpty(outputLanes)) {
			return null;
		}

		Set<Stage> reachableStages = new HashSet<>();
		for (Stage stage : stages) {
			if (outputLanes.contains(stage.getId())) {
				reachableStages.add(stage);
				if (DataFlowStageUtil.isDataStage(stage.getType())) {
					continue;
				} else {
					final List<Stage> ret = findReachableStageEndByDataStage(stage.getId(), stages);
					reachableStages.addAll(ret);
				}
			}
		}

		return new ArrayList<>(reachableStages);
	}

	private static Stage findFirstInputStage(Stage stage, List<Stage> stages, Set<String> exists) {
		for (String inputLane : stage.getInputLanes()) {
			if (StringUtils.isBlank(inputLane)) continue;
			if (exists.contains(inputLane)) {
				throw new RuntimeException("Infinite loop: " + inputLane);
			} else {
				exists.add(inputLane);
			}
			for (Stage s : stages) {
				if (!inputLane.equals(s.getId())) continue;
				if (DataFlowStageUtil.isDataStage(s.getType())) {
					return s;
				}
				return findFirstInputStage(s, stages);
			}
		}
		return null;
	}

	public static Stage findFirstInputStage(Stage stage, List<Stage> stages) {
		return findFirstInputStage(stage, stages, new HashSet<>());
	}

	public static Map<String, Integer> stageToFieldProjection(Stage stage) {
		Map<String, Integer> projection = new HashMap<>();
		if (stage == null) {
			return projection;
		}
		String fieldFilterType = stage.getFieldFilterType();
		String fieldFilter = stage.getFieldFilter();
		if (StringUtils.isNotBlank(fieldFilterType)) {
			if (Mapping.FIELD_FILTER_TYPE_RETAINED_FIELD.equals(fieldFilterType) && StringUtils.isNotBlank(fieldFilter)) {

				List<String> fields = Arrays.asList(fieldFilter.split(","));
				if (!fields.contains("_id")) {
					projection.put("_id", 0);
				}
				fields.forEach(field -> projection.put(field, 1));

			} else if (Mapping.FIELD_FILTER_TYPE_DELETE_FIELD.equals(fieldFilterType) && StringUtils.isNotBlank(fieldFilter)) {

				List<String> fields = Arrays.asList(fieldFilter.split(","));

				fields.forEach(field -> projection.put(field, 0));

			}
		}

		return projection;
	}

	public static void fieldProjection(Map<String, Object> record, Map<String, Integer> projection) {

		if (MapUtils.isEmpty(record) || MapUtils.isEmpty(projection)) {
			return;
		}

		// 执行删除字段操作
		for (Map.Entry<String, Integer> entry : projection.entrySet()) {
			String fieldName = entry.getKey();
			Integer projectValue = entry.getValue();
			if (projectValue <= 0) {
//				MapUtil.removeValueByKey(record, fieldName);
				MapUtil.removeKey(record, fieldName);
			}
		}

		// 执行保留字段操作
		Map<String, Object> newRecord = new HashMap<>();
		for (Map.Entry<String, Integer> entry : projection.entrySet()) {
			String fieldName = entry.getKey();
			Integer projectValue = entry.getValue();
//			if (projectValue > 0 && MapUtil.containKey(record, fieldName)) {
//
//				Object value = MapUtil.getValueByKey(record, fieldName);
//				MapUtil.putValueInMap(newRecord, fieldName, value);
//			}
			if (projectValue > 0) {
				MapUtil.retainKey(newRecord, record, fieldName);
			}
		}

		if (MapUtils.isNotEmpty(newRecord)) {
			record.clear();
			record.putAll(newRecord);
		}
	}


	public static Stage recursiveFindSource(List<Stage> stages, String inputStageId) {
		if (CollectionUtils.isEmpty(stages)) return null;

		for (Stage stage : stages) {
			if (StringUtils.isBlank(stage.getId())) continue;

			if (stage.getId().equals(inputStageId)) {
				if (CollectionUtils.isEmpty(stage.getInputLanes())) return stage;

				String input = stage.getInputLanes().get(0);

				if (StringUtils.isBlank(input)) return stage;

				Stage SourceStage = recursiveFindSource(stages, input);

				if (SourceStage == null) return stage;
			}
		}

		return null;
	}

	public static List<Stage> findRecentSourceStage(Stage startStage, List<Stage> stages) {
		List<Stage> srcStages = new ArrayList<>();
		if (CollectionUtils.isEmpty(stages)) {
			return srcStages;
		}

		final List<String> inputLanes = startStage.getInputLanes();
		for (String inputLane : inputLanes) {
			for (Stage stage : stages) {
				if (inputLane.equals(stage.getId())) {
					srcStages.add(stage);
					if (!DataFlowStageUtil.isDataStage(stage.getType())) {
						final List<Stage> recentSourceStage = findRecentSourceStage(stage, stages);
						if (CollectionUtils.isNotEmpty(recentSourceStage)) {
							srcStages.addAll(recentSourceStage);
						}
					}
					break;
				}
			}
		}

		return srcStages;
	}

	/**
	 * Add a stage after the specified stage
	 *
	 * @param stages   Stage List
	 * @param stage    Specified previous stage
	 * @param addStage Add stage
	 */
	public static void addStageAfterStage(List<Stage> stages, Stage stage, Stage addStage) {
		if (CollectionUtils.isEmpty(stages) || stage == null || addStage == null
				|| StringUtils.isAnyBlank(stage.getId(), addStage.getId())
				|| CollectionUtils.isEmpty(stage.getOutputLanes())) {
			return;
		}
		// Record specified stage's outputLanes
		List<String> outputLanes = new ArrayList<>(stage.getOutputLanes());
		// Link specified stage's to add stage
		stage.setOutputLanes(new ArrayList<>());
		stage.getOutputLanes().add(addStage.getId());
		// Handle add stage's outputLanes
		if (addStage.getOutputLanes() == null) {
			addStage.setOutputLanes(outputLanes);
		} else {
			addStage.getOutputLanes().addAll(outputLanes);
		}
		addStage.setOutputLanes(addStage.getOutputLanes().stream().distinct().collect(Collectors.toList()));
		// Handle add stage's inputLanes
		if (addStage.getInputLanes() == null) {
			addStage.setInputLanes(new ArrayList<>());
		}
		addStage.getInputLanes().add(stage.getId());
		addStage.setInputLanes(addStage.getInputLanes().stream().distinct().collect(Collectors.toList()));
		// Unlink stage(s) which linked by specified stage
		// Linked by add stage
		stages.forEach(s -> {
			if (s.getId() != null && outputLanes.contains(s.getId()) && s.getInputLanes() != null) {
				s.getInputLanes().remove(stage.getId());
				s.getInputLanes().add(addStage.getId());
			}
		});
	}

	/**
	 * Add a stage before the specified stage
	 *
	 * @param stages   Stage list
	 * @param stage    Specified next stage
	 * @param addStage Add stage
	 */
	public static void addStageBeforeStage(List<Stage> stages, Stage stage, Stage addStage) {
		if (CollectionUtils.isEmpty(stages) || stage == null || addStage == null
				|| StringUtils.isAnyBlank(stage.getId(), addStage.getId())
				|| CollectionUtils.isEmpty(stage.getInputLanes())) {
			return;
		}
		// Record specified stage's inputLanes
		List<String> inputLanes = new ArrayList<>(stage.getInputLanes());
		// Link specified stage's to add stage
		stage.setInputLanes(new ArrayList<>());
		stage.getInputLanes().add(addStage.getId());
		// Handle add stage's inputLanes
		if (addStage.getInputLanes() == null) {
			addStage.setInputLanes(inputLanes);
		} else {
			addStage.getInputLanes().addAll(inputLanes);
		}
		addStage.setInputLanes(addStage.getInputLanes().stream().distinct().collect(Collectors.toList()));
		// Handle add stage's outputLanes
		if (addStage.getOutputLanes() == null) {
			addStage.setOutputLanes(new ArrayList<>());
		}
		addStage.getOutputLanes().add(stage.getId());
		addStage.setOutputLanes(addStage.getOutputLanes().stream().distinct().collect(Collectors.toList()));
		// Unlink stage(s) which link to specified stage
		// Link to add stage
		stages.forEach(s -> {
			if (s.getId() != null && inputLanes.contains(s.getId()) && s.getOutputLanes() != null) {
				s.getOutputLanes().remove(stage.getId());
				s.getOutputLanes().add(addStage.getId());
			}
		});
	}
}
