package io.tapdata.connector.mariadb.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-05-05 14:47
 **/
public class MariadbSnapshotOffset implements Serializable {
	private Map<String, Object> offset;

	public MariadbSnapshotOffset() {
		offset = new HashMap<>();
	}

	public Map<String, Object> getOffset() {
		return offset;
	}

	public void setOffset(Map<String, Object> offset) {
		this.offset = offset;
	}
}
