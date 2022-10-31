package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.constant.Level;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Date;

/**
 * @author samuel
 * @Description 引擎往tm推送状态，start 跟 finished， tm需要根据状态填写开始结束时间。
 * 前端根据这个开始，结束时间，打印显示在控制台上。
 * taskStart 为任务开始重置
 * taskEnd 为任务重置完成
 * @create 2022-10-12 12:15
 **/
@EqualsAndHashCode(callSuper = true)
@Data
@Document("TaskResetLogs")
public class TaskResetEventDto extends BaseDto implements Serializable {
	private static final long serialVersionUID = -3102894699787546947L;
	private String taskId;
	private String nodeId;
	private String nodeName;
	/**
	 * 清理项描述
	 */
	private String describe;
	/** start finished， taskStart, taskEnd */
	private ResetStatusEnum status;

	/** 日志级别 */
	private Level level;

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

	public TaskResetEventDto succeed() {
		this.status = ResetStatusEnum.SUCCEED;
		this.level = Level.INFO;
		return this;
	}

	public TaskResetEventDto failed(Throwable throwable) {
		this.status = ResetStatusEnum.FAILED;
		this.level = Level.ERROR;
		this.errorMsg = throwable.getMessage();
		StringWriter sw = new StringWriter();
		try (
				PrintWriter pw = new PrintWriter(sw)
		) {
			throwable.printStackTrace(pw);
			this.errorStack = sw.toString();
		} catch (Exception e) {
			throw new RuntimeException("Get error stack failed: " + e.getMessage(), e);
		}
		return this;
	}

	public enum ResetStatusEnum {
		/** start 清理项开始 */
		START,
		/** finished 清理项结束 */
		SUCCEED,
		/** failed 清理项失败 **/
		FAILED,
		/** task start  重置任务开始 */
		TASK_START,
		/** task finished 重置任务结束 */
		TASK_SUCCEED,
		TASK_FAILED,

	}
}
