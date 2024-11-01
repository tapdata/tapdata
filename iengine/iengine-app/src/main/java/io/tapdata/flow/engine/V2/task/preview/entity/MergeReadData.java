package io.tapdata.flow.engine.V2.task.preview.entity;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 15:01
 **/
public class MergeReadData {
	private List<Map<String, Object>> data;
	private Exception error;

	public MergeReadData(List<Map<String, Object>> data) {
		this.data = data;
	}

	public List<Map<String, Object>> getData() {
		return data;
	}

	public void setData(List<Map<String, Object>> data) {
		this.data = data;
	}

	public Exception getError() {
		return error;
	}

	public void setError(Exception error) {
		this.error = error;
	}
}
