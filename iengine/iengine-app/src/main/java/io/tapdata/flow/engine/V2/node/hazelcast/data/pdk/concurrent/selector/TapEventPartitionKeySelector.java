package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector;

import com.tapdata.constant.MapUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.value.TapValue;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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

		if (null != partitionValue && !CollectionUtils.isEmpty(partitionValue)) {
			partitionValue = convert2OriginValue(partitionValue);
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

	/**
	 * retrieve original value from TapValue for hash instead of TapValue object for hash
	 *
	 * @param values
	 * @return
	 */
	public List<Object> convert2OriginValue(final List<Object> values) {
		if (CollectionUtils.isEmpty(values)) {
			return values;
		}
		return values.stream().filter(Objects::nonNull).map(v -> {
			if (v instanceof TapValue) {
				return ((TapValue<?, ?>) v).getValue();
			} else {
				return v;
			}
		}).collect(Collectors.toList());
	}
}
