package com.tapdata.tm.inspect.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 校验修复启动前校验
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/11 10:54 Create
 */
@Data
public class InspectRecoveryStartVerifyVo implements Serializable {
    /** 校验任务编号 */
    private String inspectId;
    /** 第一次校验结果编号 */
    private String firstCheckId;
    /** 引擎编号（跟随同步任务） */
    private String agentId;
    /** 任务编号 */
    private String flowId;
    /** 任务名称 */
    private String flowName;
    /** 任务类型 */
    private String flowType;
    /** 任务状态 */
    private String flowStatus;
    /** 任务里程碑阶段 */
    private String flowMilestoneStage;
    /** 任务延迟（毫秒） */
    private Long flowDelay;
    /** 差异阈值 */
    private Integer diffLimit;
    /** 差异总数 */
    private Long diffTotals;
    /** 源多数据 */
    private Long sourceOnly;
    /** 目标多数据 */
    private Long targetOnly;
    /** 恢复数据量 */
    private Long recoveryDataTotals;
    /** 恢复表数量 */
    private Integer recoveryTableTotals;

    /** 是否符合恢复条件 */
    private Boolean canRecovery;
    /** 校验项错误码 */
    private List<String> errorCodes;
}
