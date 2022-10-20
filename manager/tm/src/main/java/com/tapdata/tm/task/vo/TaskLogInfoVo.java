package com.tapdata.tm.task.vo;

import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import com.tapdata.tm.message.constant.Level;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "日志VO")
public class TaskLogInfoVo {

    public TaskLogInfoVo(String id, Level grade, String log) {
        this.id = id;
        this.grade = grade;
        this.log = log;
    }

    @Schema(description = "id")
    private String id;
    @Schema(description = "等级 INFO WARN ERROR")
    private Level grade;
    @Schema(description = "日志文本")
    private String log;

    private String nodeId;
    private String nodeName;
    /**
     * 清理项描述
     */
    private String describe;
    /** start finished， taskStart, taskEnd */
    private TaskResetEventDto.ResetStatusEnum status;

    /** 日志级别 */
    private com.tapdata.tm.commons.task.constant.Level level;

    private String errorMsg;
    private String errorStack;

    private Integer totalEvent;
    private Integer succeedEvent;
    private Integer failedEvent;
    private Long elapsedTime;
    private Date time;

    private int resetTimes;
    private int resetInterval;
    private int resetAllTimes;
}
