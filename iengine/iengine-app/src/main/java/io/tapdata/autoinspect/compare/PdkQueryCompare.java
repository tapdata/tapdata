package io.tapdata.autoinspect.compare;

import com.tapdata.tm.autoinspect.connector.IPdkConnector;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import lombok.NonNull;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/9/6 11:17 Create
 */
public class PdkQueryCompare extends QueryCompare {
	private final @NonNull IPdkConnector sourceConnector;
	private final @NonNull IPdkConnector targetConnector;

	public PdkQueryCompare(@NonNull IPdkConnector sourceConnector, @NonNull IPdkConnector targetConnector) {
		this.sourceConnector = sourceConnector;
		this.targetConnector = targetConnector;
	}

	@Override
	protected CompareRecord querySourceByKey(@NonNull String tableName, @NonNull LinkedHashMap<String, Object> keymap, @NonNull LinkedHashSet<String> keys) {
		return sourceConnector.queryByKey(tableName, keymap, keys);
	}

	@Override
	protected CompareRecord queryTargetByKey(@NonNull String tableName, @NonNull LinkedHashMap<String, Object> keymap, @NonNull LinkedHashSet<String> keys) {
		return targetConnector.queryByKey(tableName, keymap, keys);
	}
}
