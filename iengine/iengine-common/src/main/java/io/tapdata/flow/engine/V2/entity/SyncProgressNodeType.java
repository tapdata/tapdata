package io.tapdata.flow.engine.V2.entity;

/**
 * @author samuel
 * @Description
 * @create 2024-11-27 12:07
 **/
public enum SyncProgressNodeType {
	SOURCE(0),
	TARGET(1),
	;

	private final int index;

	SyncProgressNodeType(int index) {
		this.index = index;
	}

	public int getIndex() {
		return index;
	}
}
