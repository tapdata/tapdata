package com.tapdata.constant;

import com.tapdata.entity.Job;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.debug.DebugConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Log4jUtil {
	public static void setThreadContext(Job job) {
		ThreadContext.clearAll();
		ThreadContext.put("userId", job.getUser_id());
		ThreadContext.put("jobId", job.getId());
		ThreadContext.put("jobName", job.getName());
		ThreadContext.put("dataFlowId", job.getDataFlowId());
		ThreadContext.put("subTaskId", job.getDataFlowId());
		ThreadContext.put("taskId", job.getDataFlowId());
		ThreadContext.put("app", ConnectorConstant.APP_TRANSFORMER);
		if (StringUtils.isNotBlank(job.getDataFlowId())) {
			ThreadContext.put(DebugConstant.SUB_DATAFLOW_ID, job.getDataFlowId());
		}
		ThreadContext.put("threadName", Thread.currentThread().getName());
	}

	public static void setThreadContext(TaskDto taskDto) {
		if (taskDto == null) {
			return;
		}
		ThreadContext.clearAll();
		ThreadContext.put("dataFlowId", taskDto.getId().toHexString());
		ThreadContext.put("subTaskId", taskDto.getId().toHexString());
		ThreadContext.put("taskId", taskDto.getId().toHexString());
	}


	public static void setThreadContext(String taskId) {
		if (StringUtils.isBlank(taskId)) {
			return;
		}
		ThreadContext.clearAll();
		ThreadContext.put("dataFlowId", taskId);
		ThreadContext.put("subTaskId", taskId);
		ThreadContext.put("taskId", taskId);
	}


	public static String getContextTaskId() {
		String taskId = ThreadContext.get("taskId");
		if (StringUtils.isBlank(taskId)) {
			taskId = ThreadContext.get("subTaskId");
		}
		if (StringUtils.isBlank(taskId)) {
			taskId = ThreadContext.get("dataFlowId");
		}
		return taskId;
	}

	public static String getStackString(Throwable throwable) {
		StringWriter sw = new StringWriter();
		try (
				PrintWriter pw = new PrintWriter(sw)
		) {
			throwable.printStackTrace(pw);
			return sw.toString();
		}
	}

	public static String getStackString(Throwable throwable, String classNameFilter) {
		if (throwable == null) {
			return "";
		}

		StringBuilder stringBuilder = new StringBuilder();

		while (throwable != null) {
			StackTraceElement[] stackTrace = throwable.getStackTrace();
			if (stackTrace.length > 0) {
				for (StackTraceElement stackTraceElement : stackTrace) {
					if (StringUtils.isNotBlank(classNameFilter) && !stackTraceElement.getClassName().contains(classNameFilter)) {
						continue;
					}

					stringBuilder.append(stackTraceElement.toString()).append("\n");
				}
			}

			throwable = throwable.getCause();

			if (throwable != null) {
				stringBuilder.append(throwable.getMessage()).append("\n");
			}
		}
		if (stringBuilder.length() == 0) {
			stringBuilder.append(throwable.getClass().getName()).append(" stacks is empty.\n");
		}

		return stringBuilder.toString();
	}

	public static String[] getStackStrings(Throwable throwable) {
		return getStackStrings(throwable, "");
	}

	public static String[] getStackStrings(Throwable throwable, String classNameFilter) {
		if (throwable == null) {
			return new String[]{};
		}

		StackTraceElement[] stackTrace = throwable.getStackTrace();
		String[] stackStrings = new String[stackTrace.length];
		if (stackTrace.length > 0) {
			for (int i = 0; i < stackTrace.length; i++) {
				if (StringUtils.isNotBlank(classNameFilter) && !stackTrace[i].getClassName().contains(classNameFilter)) {
					continue;
				}

				stackStrings[i] = stackTrace[i].toString();
			}
		}

		return stackStrings;
	}
}
