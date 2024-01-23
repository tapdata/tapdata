package io.tapdata.flow.engine.V2.cleaner;

import io.tapdata.flow.engine.V2.cleaner.impl.MergeNodeCleaner;

/**
 * @author samuel
 * @Description
 * @create 2024-01-03 12:21
 **/
public enum CleanTypeEnum {
	MERGE_NODE_EXTERNAL_STORAGE(MergeNodeCleaner.class),
	;

	private Class<? extends ICleaner> cleanerClz;

	CleanTypeEnum(Class<? extends ICleaner> cleanerClz) {
		this.cleanerClz = cleanerClz;
	}

	public Class<? extends ICleaner> getCleanerClz() {
		return cleanerClz;
	}
}
