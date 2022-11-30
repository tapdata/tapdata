package io.tapdata.pdk.apis.partition;


import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class TapPartitionFilter extends TapFilter {
	private List<QueryOperator> operators;

	public static TapPartitionFilter create() {
		return new TapPartitionFilter();
	}

	public TapPartitionFilter op(QueryOperator operator) {
		if(operators == null) {
			operators = new ArrayList<>();
		}
		operators.add(operator);
		return this;
	}

	public TapPartitionFilter match(String key, Object value) {
		if(match == null)
			match = DataMap.create();
		match.put(key, value);
		return this;
	}

	public List<QueryOperator> getOperators() {
		return operators;
	}

	public void setOperators(List<QueryOperator> operators) {
		this.operators = operators;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("TapPartitionFilter ");
		if(operators != null) {
			builder.append("operators [");
			for(QueryOperator queryOperator : operators) {
				builder.append(queryOperator).append(", ");
			}
			builder.append("] ");
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
}
