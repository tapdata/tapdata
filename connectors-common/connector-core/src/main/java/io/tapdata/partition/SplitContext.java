package io.tapdata.partition;

import io.tapdata.entity.schema.TapIndexField;

import java.util.List;

/**
 * @author aplomb
 */
public class SplitContext {
	private long total;
	public SplitContext total(long total) {
		this.total = total;
		return this;
	}
	private List<TapIndexField> indexFields;
	public SplitContext indexFields(List<TapIndexField> indexFields) {
		this.indexFields = indexFields;
		return this;
	}
	private int currentFieldPos;
	public SplitContext currentFieldPos(int currentFieldPos) {
		this.currentFieldPos = currentFieldPos;
		return this;
	}
	public static SplitContext create() {
		return new SplitContext();
	}

	public List<TapIndexField> getIndexFields() {
		return indexFields;
	}

	public void setIndexFields(List<TapIndexField> indexFields) {
		this.indexFields = indexFields;
	}

	public int getCurrentFieldPos() {
		return currentFieldPos;
	}

	public void setCurrentFieldPos(int currentFieldPos) {
		this.currentFieldPos = currentFieldPos;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

}
