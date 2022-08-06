package io.tapdata.milestone;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.Milestone;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.SyncObjects;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2020-12-25 14:51
 **/
public class MilestoneJobService extends MilestoneService {

	private final static Logger logger = LogManager.getLogger(MilestoneJobService.class);

	private List<Milestone> milestones;

	public MilestoneJobService(MilestoneContext milestoneContext) {
		super(milestoneContext);
	}

	private List<Milestone> generateMilestonesByJob() {
		MilestoneStage[] allMilestoneStages = MilestoneStage.values();
		String syncType = milestoneContext.getJob().getSync_type();
		String mappingTemplate = milestoneContext.getJob().getMapping_template();
		DatabaseTypeEnum sourceType = DatabaseTypeEnum.fromString(milestoneContext.getSourceConn().getDatabase_type());
		if (CollectionUtils.isNotEmpty(milestoneContext.getJob().getStages()) &&
				milestoneContext.getJob().getStages().stream().anyMatch(stage -> Stage.StageTypeEnum.LOG_COLLECT.getType().equals(stage.getType()))) {
			sourceType = DatabaseTypeEnum.LOG_COLLECT;
		}
		DatabaseTypeEnum targetType = DatabaseTypeEnum.fromString(milestoneContext.getTargetConn().getDatabase_type());

		List<Milestone> milestones = new ArrayList<>();

		for (MilestoneStage milestoneStage : allMilestoneStages) {

			if (milestoneStage.isNeedSameSourceAndTarget() && !sourceType.getType().equals(targetType.getType())) {
				continue;
			}

			DatabaseTypeEnum[] sourceDatabases = milestoneStage.getSourceDatabases();
			if (CollectionUtils.isNotEmpty(Arrays.asList(sourceDatabases))) {
				if (milestoneStage.isSourceTypeInclude() && arrayNotInclude(sourceDatabases, sourceType)) {
					continue;
				} else if (!milestoneStage.isSourceTypeInclude() && arrayInclude(sourceDatabases, sourceType)) {
					continue;
				}
			}

			DatabaseTypeEnum[] targetDatabases = milestoneStage.getTargetDatabases();
			if (CollectionUtils.isNotEmpty(Arrays.asList(targetDatabases)) && arrayNotInclude(targetDatabases, targetType)) {
				continue;
			}

			String[] syncTypes = milestoneStage.getSyncTypes();
			if (CollectionUtils.isNotEmpty(Arrays.asList(syncTypes)) && arrayNotInclude(syncTypes, syncType)) {
				continue;
			}

			String[] mappingTemplates = milestoneStage.getMappingTemplates();
			if (CollectionUtils.isNotEmpty(Arrays.asList(mappingTemplates)) && arrayNotInclude(mappingTemplates, mappingTemplate)) {
				continue;
			}

			boolean needOffsetEmpty = milestoneStage.isNeedOffsetEmpty();
			if (needOffsetEmpty && this.milestoneContext.getJob().getOffset() != null) {
				continue;
			}

			// 特殊适配，使用custom sql方式进行增量
			if (!customSqlCDCPredicate(milestoneStage, syncType)) {
				continue;
			}

			// 特殊适配，是否需要drop schema
			if (keepSchemaPredicate(milestoneStage)) {
				continue;
			}

			// 特殊适配，是否需要清空目标数据
			if (!clearDataPredicate(milestoneStage)) {
				continue;
			}

			// 特殊适配，是否需要有创建表、视图、函数、存储过程的里程碑
			if (!checkDDLMilestone(milestoneStage)) {
				continue;
			}

			if ((milestoneStage.equals(MilestoneStage.CLEAR_TARGET_DATA) || milestoneStage.equals(MilestoneStage.READ_SOURCE_DDL))
					&& ConnectorConstant.SYNC_TYPE_CDC.equals(syncType)) {
				continue;
			}

			if (milestoneStage.equals(MilestoneStage.CREATE_TARGET_INDEX) && !this.milestoneContext.getJob().getNeedToCreateIndex()) {
				continue;
			}

			milestones.add(new Milestone(milestoneStage.name(), MilestoneStatus.WAITING.getStatus(), milestoneStage.getGroup().getName()));
		}

		return milestones;
	}

	private boolean checkDDLMilestone(MilestoneStage milestoneStage) {
		List<SyncObjects> syncObjects = this.milestoneContext.getJob().getSyncObjects();
		boolean result = true;
		if (CollectionUtils.isNotEmpty(syncObjects)) {
			switch (milestoneStage) {
				case CREATE_TARGET_TABLE:
					if (findObjectInList(syncObjects, obj -> ((SyncObjects) obj).getType().equals(SyncObjects.TABLE_TYPE)) == null) {
						result = false;
					}

					break;
				case CREATE_TARGET_VIEW:
					if (findObjectInList(syncObjects, obj -> ((SyncObjects) obj).getType().equals(SyncObjects.VIEW_TYPE)) == null) {
						result = false;
					}

					break;
				case CREATE_TARGET_FUNCTION:
					if (findObjectInList(syncObjects, obj -> ((SyncObjects) obj).getType().equals(SyncObjects.FUNCTION_TYPE)) == null) {
						result = false;
					}

					break;
				case CREATE_TARGET_PROCEDURE:
					if (findObjectInList(syncObjects, obj -> ((SyncObjects) obj).getType().equals(SyncObjects.PROCEDURE_TYPE)) == null) {
						result = false;
					}

					break;
				default:
					break;
			}
		}

		return result;
	}

	/**
	 * 检查是否是custom sql的方式进行增量，如果是，需要添加CDC的里程碑节点
	 *
	 * @param milestoneStage
	 * @return
	 */
	private boolean customSqlCDCPredicate(MilestoneStage milestoneStage, String syncType) {
		if ((milestoneStage.equals(MilestoneStage.READ_CDC_EVENT) || milestoneStage.equals(MilestoneStage.WRITE_CDC_EVENT))
				&& syncType.equals(ConnectorConstant.SYNC_TYPE_INITIAL_SYNC)) {

			boolean increment = this.milestoneContext.getJob().isIncrement();
			if (!increment) {
				return false;
			}

			if (!checkCustomSqlNeedCdc()) {
				return false;
			}
		}

		return true;
	}

	private boolean keepSchemaPredicate(MilestoneStage milestoneStage) {
		if (milestoneStage.equals(MilestoneStage.DROP_TARGET_SCHEMA)) {
			return this.milestoneContext.getJob().getKeepSchema();
		}

		return false;
	}

	private boolean clearDataPredicate(MilestoneStage milestoneStage) {
		if (milestoneStage.equals(MilestoneStage.CLEAR_TARGET_DATA)) {
			return this.milestoneContext.getJob().getDrop_target();
		}

		return true;
	}

	private boolean checkIsInitial(MilestoneStage milestoneStage) {
		Job job = this.milestoneContext.getJob();

		if (!milestoneStage.isNeedOffsetEmpty()) {
			return true;
		}

		if (job.needInitial()) {
			return true;
		}

		return false;
	}

	/**
	 * 遍历mappings，检查是否有至少一个custom sql并且包含"${OFFSET1}"
	 *
	 * @return
	 */
	private boolean checkCustomSqlNeedCdc() {
		List<Mapping> mappings = this.milestoneContext.getJob().getMappings();

		Mapping mapping = mappings.stream().filter(m -> StringUtils.isNotBlank(m.getCustom_sql()) && m.getCustom_sql().contains("${OFFSET1}")).findFirst().orElse(null);

		return mapping != null;
	}

	@Override
	public List<Milestone> initMilestones() {
		if (null == this.milestoneContext.getJob() || null == this.milestoneContext.getSourceConn() || null == this.milestoneContext.getTargetConn()) {
			throw new IllegalArgumentException("Milestone context missing job, source connection, target connection");
		}

		Job job = this.milestoneContext.getJob();
		if (CollectionUtils.isEmpty(job.getMilestones())) {
			// 如果任务不存在里程碑列表(第一次运行，旧任务)，则根据任务配置，生成里程碑列表
			this.milestones = generateMilestonesByJob();
		} else {
			// 如果任务已经存在里程碑，修改状态
			this.milestones = job.getMilestones();

			for (Milestone milestone : this.milestones) {

				if (MilestoneStage.valueOf(milestone.getCode()).isNeedOffsetEmpty() && !job.needInitial()) {
					// 如果已经不是增量阶段，则保持一部分里程碑的状态不变，这些里程碑只有在初始化的时候运行
					continue;
				}

				milestone.setStatus(MilestoneStatus.WAITING.getStatus());
				milestone.setStart(0L);
				milestone.setEnd(0L);
			}
		}

		return this.milestones;
	}
}
