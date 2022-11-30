package io.tapdata.partition.splitter;

import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.util.NumberUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author aplomb
 */
public class NumberSplitter implements TypeSplitter {
	public static NumberSplitter INSTANCE = new NumberSplitter();
	@Override
	public List<TapPartitionFilter> split(FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces) {
		Object min = fieldMinMaxValue.getMin();
		Object max = fieldMinMaxValue.getMax();
		Object value = NumberUtils.subtract(max, min);
		Object pieceSize = NumberUtils.divide(value, maxSplitPieces);
		List<TapPartitionFilter> partitionFilters = new ArrayList<>();
		if(min.equals(max)) {
			partitionFilters.add(TapPartitionFilter.create().match(fieldMinMaxValue.getFieldName(), min));
		} else {
			for(int i = 0; i < maxSplitPieces; i++) {
				if(i == 0) {
					partitionFilters.add(TapPartitionFilter.create().op(QueryOperator.lt(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, pieceSize))));
				} else if(i == maxSplitPieces - 1) {
					partitionFilters.add(TapPartitionFilter.create().op(QueryOperator.gte(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, NumberUtils.multiply(pieceSize, i)))));
				} else {
					partitionFilters.add(TapPartitionFilter.create()
							.op(QueryOperator.gte(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, NumberUtils.multiply(pieceSize, i))))
							.op(QueryOperator.lt(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, NumberUtils.multiply(pieceSize, i + 1))))
					);
				}
			}
		}
		return partitionFilters;
	}
}
