package com.tapdata.tm.task.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;


/**
 * 任务详情视图类
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class TaskDetailVo extends BaseVo {
    private String name;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Date startTime;
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Date stopTime;
    /** 描述*/
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String desc;

    // 增量所处时间点
    private Date eventTime;

    //全量开始时间
    private Date initStartTime;

    //增量开始时间
    private Date cdcStartTime;

    //任务完成时间
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Date taskFinishTime;

    //任务总时长
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Long taskLastHour;

    //缺少增量最大滞后时间
    private Long cdcDelayTime;

    //        任务失败次数
    //即使空，也返回字段给前端
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private Long failCount;

    /** 状态*/
    private String status;


    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String createUser;

    private String type;

}
