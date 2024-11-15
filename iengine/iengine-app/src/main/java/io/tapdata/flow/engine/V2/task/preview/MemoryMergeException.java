package io.tapdata.flow.engine.V2.task.preview;

import com.tapdata.tm.commons.task.dto.MergeTableProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-10-08 19:16
 **/
public class MemoryMergeException extends Exception {
	private final Map<String, Object> parentData;
	private final List<Map<String, Object>> childData;
	private final MergeTableProperties mergeTableProperties;

	public MemoryMergeException(Throwable cause, Map<String, Object> parentData, Map<String, Object> childData, MergeTableProperties mergeTableProperties) {
		super(cause);
		this.parentData = parentData;
		this.childData = Collections.singletonList(childData);
		this.mergeTableProperties = mergeTableProperties;
	}

	public MemoryMergeException(Throwable cause, Map<String, Object> parentData, List<Map<String, Object>> childData, MergeTableProperties mergeTableProperties) {
		super(cause);
		this.parentData = parentData;
		this.childData = childData;
		this.mergeTableProperties = mergeTableProperties;
	}

	@Override
	public String getMessage() {
		return String.format("Memory merge child data into parent data error, merge type: %s, join keys: %s, parent data: %s, child data: %s", mergeTableProperties.getMergeType(), mergeTableProperties.getJoinKeys(), parentData, childData);
	}
}
