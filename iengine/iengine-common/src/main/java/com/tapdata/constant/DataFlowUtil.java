package com.tapdata.constant;

import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.dataflow.CloneFieldProcess;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jackin
 */
public class DataFlowUtil {

	private static Logger logger = LogManager.getLogger(DataFlowUtil.class);

	public static Graph<Stage> buildDataFlowGrah(List<Stage> stages) {

		int size = stages.size();

		Map<String, Stage> stageById = new HashMap<>();
		for (int i = 0; i < size; i++) {
			Stage stage = stages.get(i);
			stageById.put(stage.getId(), stage);
		}

		Graph<Stage> stageGraph = new Graph<>();
		for (Stage stage : stages) {
			List<String> outputLanes = stage.getOutputLanes();
			for (String outputLane : outputLanes) {
				if (stageById.containsKey(outputLane)) {
					stageGraph.addEdge(stage, stageById.get(outputLane));
				}
			}
		}

		return stageGraph;
	}

	public static Set<String> findSourceDataStagesByInputLane(List<Stage> stages, String inputLane) {
		Set<String> result = new HashSet<>();
		if (CollectionUtils.isEmpty(stages) || StringUtils.isBlank(inputLane)) {
			return result;
		}

		for (Stage stage : stages) {
			if (inputLane.equals(stage.getId())) {
				if (DataFlowStageUtil.isDataStage(stage.getType())) {
					result.add(stage.getId());
					break;
				} else {
					List<String> inputLanes = stage.getInputLanes();
					if (CollectionUtils.isNotEmpty(inputLanes)) {
						for (String subInputLane : inputLanes) {
							result.addAll(
									findSourceDataStagesByInputLane(stages, subInputLane)
							);
						}
					}
				}

			}
		}

		return result;
	}

	public static List<Job> convertDataFlowToJobs(DataFlow dataFlow, ClientMongoOperator clientMongoOperator) {
		List<Job> jobs = new ArrayList<>();

		List<Stage> stages = dataFlow.getStages();
		if (CollectionUtils.isNotEmpty(stages)) {

			stages = handleStage(dataFlow);

			// 找出所有的有效路径
			List<List<Stage>> splittedDataFlows = splitDataFlow(dataFlow, stages);

			// 将同一个job下的节点合并到一起
			Map<String, List<List<Stage>>> sameJobOfDataFlowsMap = mergeSameJobFlows(splittedDataFlows);

			boolean hasCacheStage = hasCacheStage(dataFlow.getStages());

			String dataFlowName = dataFlow.getName();
			long index = 0L;
			if (MapUtils.isNotEmpty(sameJobOfDataFlowsMap)) {
				Set<String> initializedStageId = new HashSet<>();
				if (dataFlow.getStats() != null && CollectionUtils.isNotEmpty(dataFlow.getStats().getStagesMetrics())) {
					for (StageRuntimeStats dataFlowStagesMetric : dataFlow.getStats().getStagesMetrics()) {
						if (ConnectorConstant.STATS_STATUS_INITIALIZED.equals(dataFlowStagesMetric.getStatus())) {
							initializedStageId.add(dataFlowStagesMetric.getStageId());
						}
					}
				}
				for (List<List<Stage>> samJobDataFlow : sameJobOfDataFlowsMap.values()) {

					/**
					 * 是否开启了高性能模式
					 */
//          Boolean performanceMode = samJobDataFlow.stream().map(l -> l.get(0).getPerformanceMode())
//            .findFirst().orElse(Boolean.FALSE);
					/**
					 * 有多条线路的情况，源和目标肯定是一样的，所以用findFirst匹配一个源节点就可以了
					 */
					Set<Integer> partitionIdSet = samJobDataFlow.stream()
							.filter(l -> Stage.StageTypeEnum.fromString(l.get(0).getType()) == Stage.StageTypeEnum.KAFKA
									&& CollectionUtils.isNotEmpty(l.get(0).getPartitionIdSet()))
							.map(l -> l.get(0).getPartitionIdSet()).findFirst().orElseGet(HashSet::new);

					if (partitionIdSet.size() == 0) {
						//未开启高性能模式、非kafka
						partitionIdSet.add(-1);
					} else if (partitionIdSet.size() == 1) {
						//未开启高性能模式或者开启了只选择一个分区的情况
						samJobDataFlow.forEach(l -> l.get(0)
								.setPartitionId(String.valueOf(partitionIdSet.stream().findFirst().orElse(0))));
					}

					for (Integer partitionId : partitionIdSet) {
						if (partitionIdSet.size() > 1) {
							//<=1的情况可能为：非kafka源、未开启高性能模式、开启了但是只设置了一个分区id，这几种情况都不需要设置分区id
							samJobDataFlow.forEach(l -> l.get(0).setPartitionId(String.valueOf(partitionId)));
						}
						String mappingTemplate = dataFlow.getMappingTemplate();
						Job job = new Job(samJobDataFlow, mappingTemplate, clientMongoOperator, dataFlow.getSetting());
						job.setDataFlowId(dataFlow.getId());
						job.setName(dataFlowName + "_" + ++index);
						job.setUser_id(dataFlow.getUser_id());
						job.setExecuteMode(dataFlow.getExecuteMode());
						job.setPartitionId(String.valueOf(partitionId));
						if (hasCacheStage) {
							job.setProcess_id(dataFlow.getAgentId());
							job.setIsDistribute(false);
						}

						List<Mapping> mappings = job.getMappings();
						if (CollectionUtils.isEmpty(mappings)) {
							Optional<Stage> optionalStage = job.getStages().stream().filter(Stage::getDisabled).findFirst();
							if (optionalStage.isPresent()) {
								logger.info("Stage disable,stage name : {}", optionalStage.get().getName());
								continue;
							}
						}
						List<Mapping> newMappings = new ArrayList<>();
						for (Mapping mapping : mappings) {

							if (initializedMapping(initializedStageId, job, mapping)
									&& StringUtils.isBlank(dataFlow.getSetting().getCronExpression())
									&& !dataFlow.getSetting().getIncrement()) {
								continue;
							}

							newMappings.add(mapping);

							List<Stage> mappingStages = mapping.getStages();
							// 生成one many隐藏节点
							if (Mapping.need2CreateTporigMapping(job, mapping)) {

								List<Map<String, String>> joinConditions = mapping.getJoin_condition();
								List<Map<String, String>> joinConditionNew = new ArrayList<>();
								for (Map<String, String> joinCondition : joinConditions) {
									Map<String, String> joinKey = new HashMap<>();
									joinKey.put("source", joinCondition.get("source"));
									joinKey.put("target", joinCondition.get("source"));
									joinConditionNew.add(joinKey);
								}

								List<Stage> oneManyMappingStages = new ArrayList<>(mappingStages);
								// 上一个节点
								Stage previousStage = oneManyMappingStages.get(oneManyMappingStages.size() - 2);
								// 目标节点
								Stage targetStage = oneManyMappingStages.get(oneManyMappingStages.size() - 1);
								// 源节点
								Stage srcStage = oneManyMappingStages.get(0);
								Stage oneManyInvisibleStage = Stage.oneManyInvisibleStage(
										srcStage,
										previousStage,
										targetStage,
										dataFlow.getId()
								);

								previousStage.getOutputLanes().add(oneManyInvisibleStage.getId());
								job.getStages().add(oneManyInvisibleStage);
								oneManyMappingStages.set(oneManyMappingStages.size() - 1, oneManyInvisibleStage);

								String toTable = getOneManyTporigTableName(mapping.getFrom_table(), dataFlow.getId(), srcStage.getId());

								Mapping mappingNew = new Mapping(
										mapping.getFrom_table(),
										toTable,
										ConnectorConstant.RELATIONSHIP_ONE_ONE,
										joinConditionNew,
										mapping.getFields_process(),
										mapping.getScript(),
										0,           // 中间表优先级最低
										mapping.getFieldFilter(),
										mapping.getFieldFilterType()
								);
								mappingNew.setStages(oneManyMappingStages);
								mappingNew.setDropTarget(targetStage.getDropTable());

								newMappings.add(mappingNew);
							}
						}

						if (!CollectionUtils.isEmpty(newMappings)) {
							// 排序mapping，优先执行one one的场景
							Mapping.sortMapping(newMappings);
							job.setMappings(newMappings);
						}
						jobs.add(job);
					}
				}
			}
		}

		return jobs;
	}

	/**
	 * 对stage进行处理
	 * 1. 禁用节点处理
	 * 2. 根据某些任务配置，生成为处理节点，以适配后续的分析和同步，生成的处理节点不会保存进中间库，只有任务运行时存在
	 *
	 * @param dataFlow
	 * @return
	 */
	private static List<Stage> handleStage(DataFlow dataFlow) {
		if (CollectionUtils.isEmpty(dataFlow.getStages())) {
			return dataFlow.getStages();
		}
		List<Stage> stages = dataFlow.getStages();
		stages = stages.stream()
				.filter(s -> !s.getDisabled())
				.collect(Collectors.toList());

		stages = generateSpecialStage(dataFlow, stages);

		return stages;
	}

	/**
	 * 根据某些任务配置，生成为处理节点，以适配后续的分析和同步
	 * 生成的处理节点不会保存进中间库，只有任务运行时存在
	 *
	 * @param dataFlow
	 * @param stages
	 * @return
	 */
	private static List<Stage> generateSpecialStage(DataFlow dataFlow, List<Stage> stages) {
		stages = generateCloneFieldProcessStage(dataFlow, stages);
		stages = generateFieldNameTransformStage(dataFlow, stages);
		return stages;
	}

	/**
	 * 数据迁移任务，扫描stage，如果源数据节点中，存在字段处理属性
	 * 拆出生成新的字段处理节点
	 *
	 * @param dataFlow
	 * @param stages
	 */
	private static List<Stage> generateCloneFieldProcessStage(DataFlow dataFlow, List<Stage> stages) {
		if (dataFlow == null || !dataFlow.getMappingTemplate().equals(ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE)) {
			return stages;
		}

		List<Stage> newStages = new ArrayList<>();
		for (Stage stage : stages) {
			newStages.add(stage);
			List<String> outputLanes = stage.getOutputLanes();
			if (CollectionUtils.isNotEmpty(outputLanes) && CollectionUtils.isNotEmpty(stage.getField_process())) {
				List<CloneFieldProcess> fieldProcess = stage.getField_process();
				fieldProcess = fieldProcess.stream().filter(fp -> CollectionUtils.isNotEmpty(fp.getOperations()) && StringUtils.isNoneBlank(fp.getTable_name())).collect(Collectors.toList());
				if (CollectionUtils.isEmpty(fieldProcess)) {
					continue;
				}
				Stage cloneFieldProcessStage = new Stage();
				cloneFieldProcessStage.setId(UUIDGenerator.uuid());
				cloneFieldProcessStage.setName(stage.getName() + "_" + Stage.StageTypeEnum.FIELD_PROCESSOR.getType());
				cloneFieldProcessStage.setType(Stage.StageTypeEnum.FIELD_PROCESSOR.getType());
				cloneFieldProcessStage.setField_process(fieldProcess);

				DataFlowStageUtil.addStageAfterStage(stages, stage, cloneFieldProcessStage);

				newStages.add(cloneFieldProcessStage);
			}
		}

		return newStages;
	}

	/**
	 * 数据迁移任务，扫描stages，如果目标数据节点中，存在字段名大小写转换的配置
	 * 生成字段大小写处理节点
	 *
	 * @param dataFlow
	 * @param stages
	 * @return
	 */
	private static List<Stage> generateFieldNameTransformStage(DataFlow dataFlow, List<Stage> stages) {
		if (dataFlow == null || !dataFlow.getMappingTemplate().equals(ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE)) {
			return stages;
		}

		List<Stage> newStages = new ArrayList<>();

		for (Stage stage : stages) {
			newStages.add(stage);
			if (!DataFlowStageUtil.isDataStage(stage.getType()) || CollectionUtils.isEmpty(stage.getInputLanes())) {
				continue;
			}
			String fieldsNameTransform = stage.getFieldsNameTransform();
			if (fieldsNameTransform == null) {
				continue;
			}
			Capitalized capitalized;
			try {
				capitalized = Capitalized.fromValue(fieldsNameTransform);
			} catch (Exception e) {
				continue;
			}
			if (capitalized == null) {
				continue;
			}
			Stage fieldNameTransformStage = null;
			switch (capitalized) {
				case UPPER:
				case LOWER:
					fieldNameTransformStage = new Stage();
					fieldNameTransformStage.setId(UUIDGenerator.uuid());
					fieldNameTransformStage.setName(stage.getName() + "_" + Stage.StageTypeEnum.FIELD_NAME_TRANSFORM_PROCESSOR.getType());
					fieldNameTransformStage.setType(Stage.StageTypeEnum.FIELD_NAME_TRANSFORM_PROCESSOR.getType());
					fieldNameTransformStage.setFieldsNameTransform(fieldsNameTransform);
					break;
				default:
					break;
			}
			if (fieldNameTransformStage == null) {
				continue;
			}
			DataFlowStageUtil.addStageBeforeStage(stages, stage, fieldNameTransformStage);

			newStages.add(fieldNameTransformStage);
		}
		return newStages;
	}

	public static boolean initializedMapping(Set<String> initializedStageId, Job job, Mapping mapping) {
		if (CollectionUtils.isEmpty(mapping.getStages())) {
			return false;
		}
		Optional<Stage> optionalStage = mapping.getStages().stream().filter(Stage::getDisabled).findFirst();
		if (optionalStage.isPresent()) {
			logger.info("Mapping disable,stage name : {}", optionalStage.get().getName());
			return true;
		}
		boolean initializedFlag = false;
		if (ConnectorConstant.SYNC_TYPE_INITIAL_SYNC.equals(job.getSync_type()) && CollectionUtils.isNotEmpty(initializedStageId)) {
			for (Stage stage : mapping.getStages()) {
				if (CollectionUtils.isNotEmpty(stage.getOutputLanes()) && initializedStageId.contains(stage.getId())) {
					initializedFlag = true;
					break;
				}
			}
		}

		return initializedFlag;
	}

	/**
	 * 将同一个Job的节点合并到一起
	 * 合并规则：
	 * 1. source 连接id 相同 且 target连接id 相同 合并为一个Job
	 * 2. source 连接id 相同 且 目标不是数据节点 合并为一个Job
	 *
	 * @param splittedDataFlows
	 * @return
	 */
	private static Map<String, List<List<Stage>>> mergeSameJobFlows(List<List<Stage>> splittedDataFlows) {

		Map<String, List<List<Stage>>> sameJobOfDataFlowsMap = new HashMap<>();
		if (CollectionUtils.isNotEmpty(splittedDataFlows)) {

			for (List<Stage> splittedDataFlow : splittedDataFlows) {
				int size = splittedDataFlow.size();
				// 只有一个节点的场景，debug的时候
				if (size == 1) {
					Stage sourceStage = splittedDataFlow.get(0);
					String connectionId = sourceStage.getConnectionId();
					if (!sameJobOfDataFlowsMap.containsKey(connectionId)) {
						sameJobOfDataFlowsMap.put(connectionId, new ArrayList<>());
					}

					sameJobOfDataFlowsMap.get(connectionId).add(splittedDataFlow);
				} else {
					Stage sourceStage = splittedDataFlow.get(0);
					Stage targetStage = splittedDataFlow.get(size - 1);
					String sourceConnectionId = sourceStage.getConnectionId();
					// 目标节点是数据节点
					if (DataFlowStageUtil.isDataStage(targetStage.getType())) {

						String targetStageConnectionId = targetStage.getConnectionId();

						if (StringUtils.isBlank(sourceConnectionId)) {
							sourceConnectionId = sourceStage.getId();
						}
						if (StringUtils.isBlank(targetStageConnectionId)) {
							targetStageConnectionId = targetStage.getId();
						}
						StringBuilder sb = new StringBuilder(sourceConnectionId).append(targetStageConnectionId);

						if (!sameJobOfDataFlowsMap.containsKey(sb.toString())) {
							sameJobOfDataFlowsMap.put(sb.toString(), new ArrayList<>());
						}

						sameJobOfDataFlowsMap.get(sb.toString()).add(splittedDataFlow);
					}
					// 目标不是数据节点
					else {

						if (!sameJobOfDataFlowsMap.containsKey(sourceConnectionId)) {
							sameJobOfDataFlowsMap.put(sourceConnectionId, new ArrayList<>());
						}

						sameJobOfDataFlowsMap.get(sourceConnectionId).add(splittedDataFlow);
					}
				}

			}
		}

		return sameJobOfDataFlowsMap;
	}

	/**
	 * 将stages初始化成DAG，找出两两数据节点中的所有路径
	 *
	 * @param dataFlow
	 * @param stages
	 * @return
	 */
	private static List<List<Stage>> splitDataFlow(DataFlow dataFlow, List<Stage> stages) {
		List<List<Stage>> splittedDataFlows = new ArrayList<>();

		List<Stage> dataStage = new ArrayList<>();
		int size = stages.size();
		for (int i = 0; i < size; i++) {
			Stage stage = stages.get(i);

			if (DataFlowUtil.isTargetStage(stage, dataFlow.getExecuteMode())) {
				dataStage.add(stage);
			}
		}

		Graph<Stage> dag = buildDataFlowGrah(stages);

		for (Stage source : dataStage) {
			for (Stage target : dataStage) {
				if (source.equals(target)) {
					continue;
				}

				List<List<Stage>> list = dag.printAllPaths(source, target);
				if (CollectionUtils.isNotEmpty(list)) {
					splittedDataFlows.addAll(list);
				}
			}
		}

		// 移除包含两个以上数据源的路径
		for (int i = 0; i < splittedDataFlows.size(); i++) {
			List<Stage> splittedDataFlow = splittedDataFlows.get(i);
			int dataStageCount = 0;
			for (Stage stage : splittedDataFlow) {
				if (DataFlowStageUtil.isDataStage(stage.getType())) {
					dataStageCount++;
				}
			}

			if (dataStageCount > 2) {
				splittedDataFlows.remove(i);
				i--;
			}
		}

		return splittedDataFlows;
	}

	/**
	 * 检查是否属于同步的编排任务
	 *
	 * @param dataFlow
	 * @return
	 */
	public static boolean isReplicateFlow(DataFlow dataFlow) {
		List<Stage> stages = dataFlow.getStages();
		if (CollectionUtils.isNotEmpty(stages)) {
			for (Stage stage : stages) {
				String type = stage.getType();
				List<String> inputLanes = stage.getInputLanes();
				if (DataFlowStageUtil.isDataStage(type) && CollectionUtils.isNotEmpty(inputLanes)) {
					return true;
				}
			}
		}

		return false;
	}

	public static boolean hasCacheStage(List<Stage> stages) {
		if (CollectionUtils.isNotEmpty(stages)) {
			for (Stage stage : stages) {
				if (Stage.StageTypeEnum.fromString(stage.getType()) == Stage.StageTypeEnum.MEM_CACHE) {
					return true;
				}
			}
		}

		return false;
	}

	public static boolean isTargetStage(Stage stage, String executeMode) {

		String type = stage.getType();

		List<String> outputLanes = stage.getOutputLanes();
		if (DataFlow.EXECUTEMODE_EDIT_DEBUG.equals(executeMode) &&
				CollectionUtils.isEmpty(outputLanes)) {
			return true;
		}

		if (DataFlowStageUtil.isDataStage(type)) {
			return true;
		}

		return false;
	}

	public static String getOneManyTporigTableName(String srcTableName, String dataFlowId, String stageId) {
		if (StringUtils.isNotBlank(dataFlowId) && StringUtils.isNotBlank(stageId)) {
			if (StringUtils.endsWith(stageId, ConnectorConstant.LOOKUP_TABLE_SUFFIX)) {
				stageId = StringUtils.removeEnd(stageId, ConnectorConstant.LOOKUP_TABLE_SUFFIX);
			}
			srcTableName = StringUtils.substring(srcTableName, 0, 30);
			return dataFlowId + "_" + stageId + "_" + srcTableName + ConnectorConstant.LOOKUP_TABLE_SUFFIX;
		} else {
			return srcTableName + ConnectorConstant.LOOKUP_TABLE_SUFFIX;
		}
	}
}
