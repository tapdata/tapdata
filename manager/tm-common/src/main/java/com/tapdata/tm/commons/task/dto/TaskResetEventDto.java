package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.constant.Level;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;


/**
 * @author samuel
 * @Description  引擎往tm推送状态，start 跟 finished， tm需要根据状态填写开始结束时间。
 * 前端根据这个开始，结束时间，打印显示在控制台上。
 * taskStart 为任务开始重置
 * taskEnd 为任务重置完成
 * @create 2022-10-12 12:15
 **/
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskResetEventDto extends BaseDto {
	/** 任务id*/
	private ObjectId taskId;
	/** 清理项描述*/
	private String describe;
	/** start finished， taskStart, taskEnd */
	private ResetStatusEnum status;

	/** 日志级别 */
	private Level level;

	public enum ResetStatusEnum {
		/** start 清理项开始 */
		START,
		/** finished 清理项结束 */
		FINISHED,
		/** task start  重置任务开始 */
		TASK_START,
		/** task finished 重置任务结束 */
		TASK_FINISHED,
		TASK_FAILED,

	}
}
