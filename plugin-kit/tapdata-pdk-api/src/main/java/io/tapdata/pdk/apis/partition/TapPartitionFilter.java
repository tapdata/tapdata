package io.tapdata.pdk.apis.partition;


import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author aplomb
 */
public class TapPartitionFilter extends TapFilter {
	private QueryOperator leftBoundary;
	public TapPartitionFilter leftBoundary(QueryOperator operator) {
		leftBoundary = operator;
		return this;
	}
	private QueryOperator rightBoundary;
	public TapPartitionFilter rightBoundary(QueryOperator operator) {
		rightBoundary = operator;
		return this;
	}

	public static TapPartitionFilter create() {
		return new TapPartitionFilter();
	}


	public TapPartitionFilter resetMatch(DataMap match) {
		this.match = match != null ? (DataMap) InstanceFactory.instance(TapUtils.class).cloneMap(match) : null;
		return this;
	}

	public TapPartitionFilter match(String key, Object value) {
		if(key == null)
			return this;
		if(match == null)
			match = DataMap.create();
		match.put(key, value);
		return this;
	}

	public TapPartitionFilter match(Map<String, Object> map) {
		if(map == null)
			return this;
		if(match == null)
			match = DataMap.create();
		match.putAll(map);
		return this;
	}

	public boolean matchAny() {
		return (match == null || match.isEmpty()) && leftBoundary == null && rightBoundary == null;
	}

	public QueryOperator getLeftBoundary() {
		return leftBoundary;
	}

	public void setLeftBoundary(QueryOperator leftBoundary) {
		this.leftBoundary = leftBoundary;
	}

	public QueryOperator getRightBoundary() {
		return rightBoundary;
	}

	public void setRightBoundary(QueryOperator rightBoundary) {
		this.rightBoundary = rightBoundary;
	}

	public TapAdvanceFilter toAdvanceFilter() {
		TapAdvanceFilter advanceFilter = TapAdvanceFilter.create().match(match);
		if(leftBoundary != null)
			advanceFilter.op(leftBoundary);
		if(rightBoundary != null)
			advanceFilter.op(rightBoundary);
		return advanceFilter;
	}

	public TapPartitionFilter fromAdvanceFilter(TapAdvanceFilter advanceFilter) {
		if(advanceFilter != null) {
			match = advanceFilter.getMatch();
			List<QueryOperator> ops = advanceFilter.getOperators();
			if(ops != null) {
				for(QueryOperator operator : ops) {
					if(operator.getOperator() == QueryOperator.LT || operator.getOperator() == QueryOperator.LTE) {
						rightBoundary = operator;
					} else if(operator.getOperator() == QueryOperator.GT || operator.getOperator() == QueryOperator.GTE) {
						leftBoundary = operator;
					}
				}
			}
		}
		return this;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("TapPartitionFilter ");
		if(leftBoundary != null) {
			builder.append("leftBoundary ").append(leftBoundary).append("; ");
		}
		if(rightBoundary != null) {
			builder.append("rightBoundary ").append(rightBoundary).append("; ");
		}

		if(match != null) {
			builder.append("match {");
			for(Map.Entry<String, Object> entry : match.entrySet()) {
				builder.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
			}
			builder.append("}");
		}
		return builder.toString();
	}

	public static List<TapPartitionFilter> filtersWhenMinMaxEquals(TapPartitionFilter boundaryPartitionFilter, FieldMinMaxValue fieldMinMaxValue, Object min) {
		List<TapPartitionFilter> partitionFilters = new ArrayList<>();
		if(boundaryPartitionFilter.getLeftBoundary() == null || min != boundaryPartitionFilter.getLeftBoundary().getValue()) {
			partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
					.leftBoundary(boundaryPartitionFilter.getLeftBoundary())
					.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), min)));
		}

		partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch()).match(fieldMinMaxValue.getFieldName(), min));

		partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
				.leftBoundary(QueryOperator.gt(fieldMinMaxValue.getFieldName(), min))
				.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
		return partitionFilters;
	}
}
