package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector;

import com.tapdata.constant.MapUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author jackin
 * @date 2022/7/26 09:28
 **/
public class TapEventPartitionKeySelector implements PartitionKeySelector<TapEvent, Object, Map<String, Object>> {

	private Function<TapEvent, List<String>> keyFunction;

	public TapEventPartitionKeySelector(Function<TapEvent, List<String>> keyFunction) {
		this.keyFunction = keyFunction;
	}

	@Override
	public List<Object> select(TapEvent tapEvent, Map<String, Object> row) {

		List<Object> partitionValue = null;
		final List<String> keys = keyFunction.apply(tapEvent);
		if (CollectionUtils.isNotEmpty(keys) && MapUtils.isNotEmpty(row)) {
			partitionValue = new ArrayList<>(keys.size());
			getPartitionValue(partitionValue, keys, row);
		}

		return partitionValue;
	}

	private void getPartitionValue(List<Object> partitionValue, List<String> keys, Map<String, Object> row) {
		if (MapUtils.isNotEmpty(row)) {
			for (String key : keys) {
				partitionValue.add(MapUtil.getValueByKey(row, key));
			}
		}
	}
}
