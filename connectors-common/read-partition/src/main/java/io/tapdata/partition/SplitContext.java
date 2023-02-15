package io.tapdata.partition;

import io.tapdata.entity.schema.TapIndexField;

import java.util.List;

/**
 * @author aplomb
 */
public class SplitContext {
	private Long total;
	public SplitContext total(Long total) {
		this.total = total;
		return this;
	}
	private List<TapIndexField> indexFields;
	public SplitContext indexFields(List<TapIndexField> indexFields) {
		this.indexFields = indexFields;
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

	public Long getTotal() {
		return total;
	}

	public void setTotal(Long total) {
		this.total = total;
	}

}
