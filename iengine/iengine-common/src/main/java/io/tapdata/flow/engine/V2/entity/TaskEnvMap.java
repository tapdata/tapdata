package io.tapdata.flow.engine.V2.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2025-02-21 15:04
 **/
public class TaskEnvMap extends HashMap<String, Object> implements Serializable {
	public static final String NAME_PREFIX = TaskEnvMap.class.getSimpleName();
	private static final long serialVersionUID = 6926166768087564040L;

	public TaskEnvMap() {
	}

	public TaskEnvMap(Map<? extends String, ?> m) {
		super(m);
	}

	public static String name(String taskId) {
		return String.join("_", NAME_PREFIX, taskId);
	}
}
