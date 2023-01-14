/**
 * @title: DataFlows
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.dag.vo.CustomTypeMapping;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.worker.dto.TcmInfo;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
public class DataFlowDto extends SchedulableDto {

	private String name;

	private String description;

	private String status;

	private String executeMode;

	private String mappingTemplate;

	private Map<String, Object> setting;

	private String category;

	private Boolean stopOnError;

	private Map<String, Object> debug;

	private Map<String, Object> emailWaring;

	private List<Map<String, Object>> stages;

	private Map<String, Object> stats;

	private String dataFlowType;

	private Long pingTime;

	private Long nextSyncTime;

	private Long nextScheduledTime;

	//private String agentId;

	private String validateStatus;

	private String validateFailedMSG;

	private List<Map<String, Object>> validationSettings;

	private String validateBatchId;

	private String lastValidateBatchId;

	private List<String> cdcShareDataFlowIds;

	private List<Map<String, Object>> cdcLastTimes;

	private List<Map<String, Object>> flowMilestones;

	private Map<String, Object> edgeMilestones;

	private Map<String, Object> syncProgress;

	private Object startTime;
	private Object scheduledTime;
	private Object stoppingTime;
	private Object forceStoppingTime;
	private Object runningTime;
	private Object errorTime;
	private Object pausedTime;
	private Object finishTime;
	private Object operationTime;
	private List<Map<String, Object>> milestones;
	//private List<String> agentTags;

	private TcmInfo tcm;

	private Object dataSourceModel;

	private PlatformInfo platformInfo;

	private String agentType;

	private String flowEngineVersion;

	/**
	 * 新旧模型推演控制
	 * key: sink stage id
	 * value: true-old version, false-new version
	 * example: {'stage-id-1': true, 'stage-id-2': false}
	 */
	private Map<String, Boolean> tranModelVersionControl;

	private String creator;

	public String getCreator() {
		return creator == null ? getCreateUser() : creator;
	}

	private String rollback; //: "table"/"all"
	private String rollbackTable; //: "Leon_CAR_CUSTOMER";

	/**
	 * 模型推演结果
	 */
	private List<SchemaTransformerResult> metadataMappings;

	private String startType;  // 任务启动方式, manual-手动运行， auto-自动运行

	private String dataFlowRecordId; // 任务记录id
	private List<CustomTypeMapping> customTypeMappings;

	private Map<String, List<String>> deletedFields;
}
