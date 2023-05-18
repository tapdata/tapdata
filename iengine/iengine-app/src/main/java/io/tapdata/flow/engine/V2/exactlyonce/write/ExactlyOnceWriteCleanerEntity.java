package io.tapdata.flow.engine.V2.exactlyonce.write;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2023-05-18 01:34
 **/
public class ExactlyOnceWriteCleanerEntity implements Serializable {
	private static final long serialVersionUID = 1789569685389554077L;
	public static final int DEFAULT_TIME_WINDOW_DAY = 3;
	private final String tableName;
	private final String nodeId;
	private Integer timeWindowDay;
	private final String connectionId;
	private final Object lock;

	public ExactlyOnceWriteCleanerEntity(String tableName, String nodeId, Integer timeWindowDay, String connectionId) {
		this.tableName = tableName;
		this.nodeId = nodeId;
		this.timeWindowDay = null != timeWindowDay && timeWindowDay > 0 ? timeWindowDay : DEFAULT_TIME_WINDOW_DAY;
		this.connectionId = connectionId;
		this.lock = new int[0];
	}

	public String getTableName() {
		return tableName;
	}

	public String getNodeId() {
		return nodeId;
	}

	public Integer getTimeWindowDay() {
		return timeWindowDay;
	}

	public String getConnectionId() {
		return connectionId;
	}

	public Object getLock() {
		return lock;
	}

	public String identity() {
		return String.join("_", nodeId, tableName);
	}

	public void setTimeWindowDay(Integer timeWindowDay) {
		this.timeWindowDay = timeWindowDay;
	}

	public ExactlyOnceWriteCleanerEntity timeWindowDay(Integer timeWindowDay) {
		this.timeWindowDay = timeWindowDay;
		return this;
	}

	public boolean isEmpty() {
		return StringUtils.isBlank(tableName)
				|| StringUtils.isBlank(nodeId)
				|| StringUtils.isBlank(connectionId);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", ExactlyOnceWriteCleanerEntity.class.getSimpleName() + "[", "]")
				.add("tableName='" + tableName + "'")
				.add("nodeId='" + nodeId + "'")
				.add("timeWindowDay=" + timeWindowDay)
				.add("connectionId='" + connectionId + "'")
				.toString();
	}
}
