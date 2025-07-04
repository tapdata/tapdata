package com.tapdata.tm.inspect.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.base.IDataPermissionDto;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleVO;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.inspect.bean.Limit;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.bean.Timing;
import com.tapdata.tm.inspect.vo.InspectTaskVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;


/**
 * 校验任务
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class InspectDto extends SchedulableDto implements IDataPermissionDto {
    /**
     *
     */
    private String name;
    /**
     *
     */
    private String flowId;
    /**
     * 查询详情的时候需要返回关联的task详情
     * */
    private InspectTaskVo taskDto;
    /**
     *
     */
    private String mode;
    /**
     *
     */
    private String inspectMethod;
    private String inspectDifferenceMode;

    /**
     *
     */
    private PlatformInfo platformInfo;
    /**
     *
     */
    private Timing timing;
    /**
     *
     */
    private Limit limit;
    /**
     *
     */
    private Boolean enabled = true;
    /**
     *
     */
    private List<Task> tasks;
    /**
     *
     */
    private String dataFlowName;
    /**
     *
     */
    private String status;
    /**
     *
     */
    private Long ping_time;


    /**
     *
     */
    private Long lastStartTime;
    /**
     *
     */
    private String errorMsg;

    /**
     * 调用该方法，result  总是返回空字符串，导致inspect表更新总是不正确，因此需要另作调整
     */
    private String result;

    @JsonProperty("difference_number")
    private Integer differenceNumber = 0;

    @JsonProperty("InspectResult")
    private InspectResultDto inspectResult;
    private  Boolean is_deleted;

    private String taskId;

    private Boolean ignoreTimePrecision;
    private String roundingMode;

    /**
     * 标志是否是二次校验，如果是，值就是父校验的id,
     * 如果不是，就 空
     */
    private String byFirstCheckId;
    private String version; // 校验任务编辑时生成版本号，用于表示结果属于哪个版本
    private int browserTimezoneOffset; // 浏览器时区偏移量，+8 = -480(分钟)
    private String cdcBeginDate; // 事件开始时间，不能为空。指定从哪个源事件操作时间开始校验，样式：'yyyy-MM-dd HH:mm'
    private int cdcDuration; // 每次处理时长，不能小于5；单位：分钟

    private String inspectResultId;     // 重新校验功能
    private List<String> taskIds;       // 重新校验要执行的任务id
    private Boolean enableRecovery; // 启用数据恢复逻辑（不存库，传递给引擎启动）
    private Boolean canRecovery; // 是否可恢复数据（不存库）
    private Boolean isExportSql;

		private List<AlarmSettingVO> alarmSettings;
		private List<AlarmRuleVO> alarmRules;
}
