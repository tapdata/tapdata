package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;

import java.util.Date;

/**
 * @author samuel
 * @Description  引擎往tm推送状态，start 跟 finished， tm需要根据状态填写开始结束时间。
 * 前端根据这个开始，结束时间，打印显示在控制台上。
 * @create 2022-10-12 12:15
 **/
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskResetEventDto extends BaseDto {
	/** 任务id*/
	private ObjectId taskId;
	/** 清理项描述*/
	private String describe;
	/** start finished*/
	private String status;

	private Date startTime;
	private Date finishedTime;
}
