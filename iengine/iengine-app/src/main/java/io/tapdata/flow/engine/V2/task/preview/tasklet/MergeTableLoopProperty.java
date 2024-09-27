package io.tapdata.flow.engine.V2.task.preview.tasklet;

import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import io.tapdata.entity.utils.DataMap;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2024-09-25 15:21
 **/
public class MergeTableLoopProperty implements Cloneable {
	private final int level;
	private final MergeTableProperties mergeTableProperties;
	private DataMap data;
	private List<MergeTableLoopProperty> parentProperties = new ArrayList<>();

	public MergeTableLoopProperty(int level, MergeTableProperties mergeTableProperties) {
		this.level = level;
		this.mergeTableProperties = mergeTableProperties;
	}

	public MergeTableLoopProperty(int level, MergeTableProperties mergeTableProperties, DataMap data) {
		this.level = level;
		this.mergeTableProperties = mergeTableProperties;
		this.data = data;
	}

	public void addParent(MergeTableLoopProperty mergeTableLoopProperty) {
		this.parentProperties.add(mergeTableLoopProperty);
	}

	public int getLevel() {
		return level;
	}

	public MergeTableProperties getMergeTableProperties() {
		return mergeTableProperties;
	}

	public DataMap getData() {
		return data;
	}

	public List<MergeTableLoopProperty> getParentProperties() {
		return parentProperties;
	}

	public void setData(DataMap data) {
		this.data = data;
	}

	public void setParentProperties(List<MergeTableLoopProperty> parentProperties) {
		this.parentProperties = parentProperties;
	}

	@Override
	public MergeTableLoopProperty clone() {
		try {
			return (MergeTableLoopProperty) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}
}
