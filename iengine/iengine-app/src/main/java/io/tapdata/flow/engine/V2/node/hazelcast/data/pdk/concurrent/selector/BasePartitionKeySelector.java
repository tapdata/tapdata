package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector;

import io.tapdata.entity.schema.value.TapValue;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2023-08-09 15:04
 **/
public abstract class BasePartitionKeySelector<T, R, M> implements PartitionKeySelector<T, R, M> {
	protected Function<T, R> customGetPartitionValueString;

	public BasePartitionKeySelector<T, R, M> customGetPartitionValueString(Function<T, R> customGetPartitionValueString) {
		this.customGetPartitionValueString = customGetPartitionValueString;
		return this;
	}

	/**
	 * retrieve original value from TapValue for hash instead of TapValue object for hash
	 *
	 * @param values
	 * @return
	 */
	@Override
	public List<R> convert2OriginValue(final List<R> values) {
		if (CollectionUtils.isEmpty(values)) {
			return values;
		}
		return (List<R>) values.stream().filter(Objects::nonNull).map(v -> {
			if (v instanceof TapValue) {
				return ((TapValue<?, ?>) v).getValue();
			} else {
				return v;
			}
		}).collect(Collectors.toList());
	}
}
