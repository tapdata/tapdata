package io.tapdata.aspect;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.logger.Log;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskStartAspect extends Aspect {
	private TaskDto task;
	private Log log;
	private ThreadGroup threadGroup;
	private Map<String, Object> infoMap;

	public TaskStartAspect info(String key, Object value) {
		if(key == null)
			return this;
		if(infoMap == null)
			infoMap = new ConcurrentHashMap<>();
		infoMap.put(key, value);
		return this;
	}

	public Object info(String key){
		return null == infoMap ? null : infoMap.get(key);
	}

	public TaskStartAspect task(TaskDto task) {
		this.task = task;
		return this;
	}

	public TaskStartAspect log(Log log) {
		this.log = log;
		return this;
	}

	public TaskStartAspect threadGroup(ThreadGroup threadGroup){
		this.threadGroup = threadGroup;
		return this;
	}

	public TaskDto getTask() {
		return task;
	}

	public void setTask(TaskDto task) {
		this.task = task;
	}

	public Log getLog() {
		return log;
	}
}
