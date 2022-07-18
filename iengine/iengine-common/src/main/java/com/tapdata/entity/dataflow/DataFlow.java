package com.tapdata.entity.dataflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.entity.Milestone;
import com.tapdata.entity.dataflow.validator.ValidatorSetting;
import io.tapdata.milestone.EdgeMilestone;
import io.tapdata.milestone.MilestoneService;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 */
public class DataFlow implements Serializable {

	public static final String STATUS_FIELD = "status";
	public static final String PING_TIME_FIELD = "pingTime";
	public static final String STATS_FIELD = "stats";
	public static final String STAGES_FIELD = "stages";
	public static final String ERROR_MSG = "errorMsg";

	public final static String EXECUTEMODE_NORMAL = "normal";
	public final static String EXECUTEMODE_EDIT_DEBUG = "editing_debug";
	public final static String EXECUTEMODE_RUNNING_DEBUG = "running_debug";

	public final static String STATUS_SCHEDULED = "scheduled";
	public final static String STATUS_RUNNING = "running";
	public final static String STATUS_STOPPING = "stopping";
	public final static String STATUS_FORCE_STOPPING = "force stopping";
	public final static String STATUS_PAUSED = "paused";
	public final static String STATUS_ERROR = "error";
	private static final long serialVersionUID = -8796115632630099164L;

	public final static String NOT_ENOUGH_RESOURCE = "NotEnoughResource";

	private String id;

	private String name;

	private String description;

	private String status;

	private String executeMode;

	private String mappingTemplate;

	private DataFlowSetting setting;

	private String user_id;

	private String category;

	private boolean stopOnError;

	private Debug debug;

	private Map<String, Boolean> emailWaring;

	private List<Stage> stages;

	private DataFlowStats stats;

	private long pingTime;

	private long nextSyncTime;

	private String agentId;

	private String validateStatus;

	private String validateFailedMSG;

	private List<ValidatorSetting> validationSettings;

	private String validateBatchId;

	private String lastValidateBatchId;

	private List<String> cdcShareDataFlowIds;

	private List<CdcLastTime> cdcLastTimes;

	@JsonIgnore
	private MilestoneService milestoneService;

	private List<Milestone> flowMilestones;
	/**
	 * key: sourceVertexName+":"+destVertexName
	 */
	private Map<String, EdgeMilestone> edgeMilestones;

	private SyncProgress syncProgress;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private List<ModPipeline> modPipeline;

	/**
	 * 新旧模型推演控制
	 * key: sink stage id
	 * value: true-old version, false-new version
	 * example: {'stage-id-1': true, 'stage-id-2': false}
	 */
	private Map<String, Boolean> tranModelVersionControl;

	private String subTaskId;
	private String taskId;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getExecuteMode() {
		return executeMode;
	}

	public void setExecuteMode(String executeMode) {
		this.executeMode = executeMode;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public boolean isStopOnError() {
		return stopOnError;
	}

	public void setStopOnError(boolean stopOnError) {
		this.stopOnError = stopOnError;
	}

	public Map<String, Boolean> getEmailWaring() {
		return emailWaring;
	}

	public void setEmailWaring(Map<String, Boolean> emailWaring) {
		this.emailWaring = emailWaring;
	}

	public List<Stage> getStages() {
		return stages;
	}

	public void setStages(List<Stage> stages) {
		this.stages = stages;
	}

	public Debug getDebug() {
		return debug;
	}

	public void setDebug(Debug debug) {
		this.debug = debug;
	}

	public long getPingTime() {
		return pingTime;
	}

	public void setPingTime(long pingTime) {
		this.pingTime = pingTime;
	}

	public String getMappingTemplate() {
		return mappingTemplate;
	}

	public void setMappingTemplate(String mappingTemplate) {
		this.mappingTemplate = mappingTemplate;
	}

	public DataFlowStats getStats() {
		return stats;
	}

	public void setStats(DataFlowStats stats) {
		this.stats = stats;
	}

	public DataFlowSetting getSetting() {
		return setting;
	}

	public void setSetting(DataFlowSetting setting) {
		this.setting = setting;
	}

	public long getNextSyncTime() {
		return nextSyncTime;
	}

	public void setNextSyncTime(long nextSyncTime) {
		this.nextSyncTime = nextSyncTime;
	}

	public String getAgentId() {
		return agentId;
	}

	public void setAgentId(String agentId) {
		this.agentId = agentId;
	}


	public String getValidateStatus() {
		return validateStatus;
	}

	public String getValidateFailedMSG() {
		return validateFailedMSG;
	}

	public void setValidateStatus(String validateStatus) {
		this.validateStatus = validateStatus;
	}

	public void setValidateFailedMSG(String validateFailedMSG) {
		this.validateFailedMSG = validateFailedMSG;
	}

	public List<ValidatorSetting> getValidationSettings() {
		return validationSettings;
	}

	public void setValidationSettings(List<ValidatorSetting> validationSettings) {
		this.validationSettings = validationSettings;
	}

	public String getValidateBatchId() {
		return validateBatchId;
	}

	public void setValidateBatchId(String validateBatchId) {
		this.validateBatchId = validateBatchId;
	}

	public String getLastValidateBatchId() {
		return lastValidateBatchId;
	}

	public void setLastValidateBatchId(String lastValidateBatchId) {
		this.lastValidateBatchId = lastValidateBatchId;
	}

	public List<String> getCdcShareDataFlowIds() {
		return cdcShareDataFlowIds;
	}

	public List<CdcLastTime> getCdcLastTimes() {
		return cdcLastTimes;
	}

	public void setCdcLastTimes(List<CdcLastTime> cdcLastTimes) {
		this.cdcLastTimes = cdcLastTimes;
	}

	public MilestoneService getMilestoneService() {
		return milestoneService;
	}

	public void setMilestoneService(MilestoneService milestoneService) {
		this.milestoneService = milestoneService;
	}

	public Map<String, EdgeMilestone> getEdgeMilestones() {
		return edgeMilestones;
	}

	public void setEdgeMilestones(Map<String, EdgeMilestone> edgeMilestones) {
		this.edgeMilestones = edgeMilestones;
	}

	public List<Milestone> getFlowMilestones() {
		return flowMilestones;
	}

	public void setFlowMilestones(List<Milestone> flowMilestones) {
		this.flowMilestones = flowMilestones;
	}

	public SyncProgress getSyncProgress() {
		return syncProgress;
	}

	public void setSyncProgress(SyncProgress syncProgress) {
		this.syncProgress = syncProgress;
	}

	public List<ModPipeline> getModPipeline() {
		return modPipeline;
	}

	public Map<String, Boolean> getTranModelVersionControl() {
		return tranModelVersionControl;
	}

	public void setTranModelVersionControl(Map<String, Boolean> tranModelVersionControl) {
		this.tranModelVersionControl = tranModelVersionControl;
	}

	public String getSubTaskId() {
		return subTaskId;
	}

	public void setSubTaskId(String subTaskId) {
		this.subTaskId = subTaskId;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public class Debug {

		@JsonProperty("debug_row")
		private Integer debugRow;

		public Debug(Integer debugRow) {
			this.debugRow = debugRow;
		}

		public Integer getDebugRow() {
			return debugRow;
		}

		public void setDebugRow(Integer debugRow) {
			this.debugRow = debugRow;
		}
	}
}
