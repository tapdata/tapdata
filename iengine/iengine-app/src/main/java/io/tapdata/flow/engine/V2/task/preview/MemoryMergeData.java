package io.tapdata.flow.engine.V2.task.preview;

import com.tapdata.tm.commons.task.dto.MergeTableProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-09-27 18:57
 **/
public class MemoryMergeData {
	private MergeTableProperties mergeTableProperties;
	private List<Map<String, Object>> data;

	public static MemoryMergeData create() {
		MemoryMergeData memoryMergeData = new MemoryMergeData();
		memoryMergeData.data = new ArrayList<>();
		return memoryMergeData;
	}

	public MemoryMergeData mergeTableProperties(MergeTableProperties mergeTableProperties) {
		this.mergeTableProperties = mergeTableProperties;
		return this;
	}

	public MemoryMergeData data(List<Map<String, Object>> data) {
		this.data.addAll(data);
		return this;
	}

	public MergeTableProperties getMergeTableProperties() {
		return mergeTableProperties;
	}

	public List<Map<String, Object>> getData() {
		return data;
	}
}
