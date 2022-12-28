package io.tapdata.pdk.apis.partition.splitter;

import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author aplomb
 */
public class NumberSplitter implements TypeSplitter<Number> {
	public static NumberSplitter INSTANCE = new NumberSplitter();
	@Override
	public List<TapPartitionFilter> split(TapPartitionFilter boundaryPartitionFilter, FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces) {
		Object min = fieldMinMaxValue.getMin();
		Object max = fieldMinMaxValue.getMax();
		List<TapPartitionFilter> partitionFilters = new ArrayList<>();
		if(min == null || max == null) {
			partitionFilters.add(boundaryPartitionFilter);
		} else if(min.equals(max)) {
			partitionFilters.addAll(TapPartitionFilter.filtersWhenMinMaxEquals(boundaryPartitionFilter, fieldMinMaxValue, min));
		} else {
			Object value = NumberUtils.subtract(max, min);
			Object pieceSize = NumberUtils.divide(value, maxSplitPieces);
			if(Objects.equals(pieceSize, 0)) {
				pieceSize = 1;
				maxSplitPieces = (int) NumberUtils.divide(value, pieceSize);
			}

			for(int i = 0; i < maxSplitPieces; i++) {
				if(i == 0) {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(boundaryPartitionFilter.getLeftBoundary())
							.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, pieceSize))));
				} else if(i == maxSplitPieces - 1) {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, NumberUtils.multiply(pieceSize, i))))
							.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
				} else {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, NumberUtils.multiply(pieceSize, i))))
							.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, NumberUtils.multiply(pieceSize, i + 1))))
					);
				}
			}
			if(maxSplitPieces == 1) {
				partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
						.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), NumberUtils.add(min, pieceSize)))
						.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
			}
		}
		return partitionFilters;
	}

	@Override
	public int compare(Number o1, Number o2) {
		return NumberUtils.compareTo(o1, o2);
	}
}
