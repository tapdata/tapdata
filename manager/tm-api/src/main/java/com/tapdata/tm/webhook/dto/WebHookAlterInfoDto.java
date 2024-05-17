package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.message.constant.Level;
import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
public class WebHookAlterInfoDto {
    private String id;
    private AlarmStatusEnum status;
    private Level level;
    private AlarmComponentEnum component;
    private AlarmTypeEnum type;
    private String agentId;
    private String taskId;
    private String name;
    private String nodeId;
    private String node;
    private AlarmKeyEnum metric;
    private Integer currentValue;
    private Integer threshold;
    private Date firstOccurrenceTime;
    private Date lastOccurrenceTime;
    private Date lastNotifyTime;
    private Integer tally;
    private String summary;
    private Date recoveryTime;
    private Date closeTime;
    private String closeBy;
    private String inspectId;

    private String title;
    private String content;
    private String smsEvent;

    /**
     * @see AlarmStatusEnum
     * */
    private String statusTxt;
    /**
     * @see AlarmComponentEnum
     * */
    private String componentTxt;
    /**
     * @see AlarmTypeEnum
     * */
    private String typeTxt;
    /**
     * @see AlarmKeyEnum
     * */
    private String metricTxt;

    private transient Map<String, Object> param;

    public void copyTxt() {
        withStatusTxt(this.status);
        withComponentTxt(this.component);
        withMetricTxt(this.metric);
        withTypeTxt(this.type);
    }

    public void withStatusTxt(AlarmStatusEnum status) {
        switch (status) {
            case ING: this.statusTxt = "正在进行";break;
            case RECOVER: this.statusTxt = "已恢复";break;
            case CLOESE: this.statusTxt = "已关闭";break;
            default: this.statusTxt = "-";
        }
    }
    public void withComponentTxt(AlarmComponentEnum component) {
        if (AlarmComponentEnum.FE.equals(component)) {
            this.componentTxt = "引擎";
            return;
        }
        this.componentTxt = "-";
    }
    public void withTypeTxt(AlarmTypeEnum type) {
        this.typeTxt = type.getDesc();
    }
    public void withMetricTxt(AlarmKeyEnum key) {
        switch (key) {
            case DATANODE_AVERAGE_HANDLE_CONSUME:
                this.metricTxt = "数据源节点的平均处理耗时超过阀值";break;
            case TASK_INCREMENT_DELAY:
                this.metricTxt = "任务的增量延迟超过阀值";break;
            case SYSTEM_FLOW_EGINGE_DOWN:
                this.metricTxt = "引擎离线";break;
            case PROCESSNODE_AVERAGE_HANDLE_CONSUME:
                this.metricTxt = "节点的平均处理耗时超过阀值";break;
            case TASK_STATUS_STOP:
                this.metricTxt = "任务运行停止";break;
            case SYSTEM_FLOW_EGINGE_UP:
                this.metricTxt = "引擎上线";break;
            case TASK_STATUS_ERROR:
                this.metricTxt = "任务运行错误";break;
            case INSPECT_COUNT_ERROR:
                this.metricTxt = "Count校验结果的差异行数大于阈值";break;
            case TASK_INCREMENT_START:
                this.metricTxt = "任务增量开始";break;
            case TASK_FULL_COMPLETE:
                this.metricTxt = "任务全量完成";break;
            case INSPECT_VALUE_ERROR:
                this.metricTxt = "值校验结果的表数据差大于阈值";break;
            case TASK_INSPECT_ERROR:
                this.metricTxt = "任务校验出错";break;
            case DATANODE_CANNOT_CONNECT:
                this.metricTxt = "数据源无法连接网络";break;
            case DATANODE_TCP_CONNECT_CONSUME:
                this.metricTxt = "数据源TCP连接完成";break;
            case DATANODE_HTTP_CONNECT_CONSUME:
                this.metricTxt = "数据源连接网络完成";break;
            case INSPECT_TASK_ERROR:
                this.metricTxt = "校验任务遇到错误";break;
            default:
                this.metricTxt = "-";
        }
    }

}
