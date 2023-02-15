package io.tapdata.pdk.apis.partition.splitter;

import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * @author aplomb
 */
public class DateTimeSplitter implements TypeSplitter<DateTime> {
	public static DateTimeSplitter INSTANCE = new DateTimeSplitter();
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
			DateTime minDateTime = AnyTimeToDateTime.toDateTime(fieldMinMaxValue.getMin());
			DateTime maxDateTime = AnyTimeToDateTime.toDateTime(fieldMinMaxValue.getMax());
			if(minDateTime == null || maxDateTime == null)
				throw new CoreException(TapAPIErrorCodes.MIN_MAX_CANNOT_CONVERT_TO_DATETIME, "Min {} max {} convert to DateTime failed, min {}, max {}", min, max, minDateTime, maxDateTime);

			BigDecimal minDecimal = minDateTime.toNanoSeconds();
			BigDecimal maxDecimal = maxDateTime.toNanoSeconds();

			BigDecimal value = maxDecimal.subtract(minDecimal);
			BigDecimal pieceSize = value.divide(BigDecimal.valueOf(maxSplitPieces), RoundingMode.HALF_UP);
			if(pieceSize.compareTo(BigDecimal.ZERO) == 0) {
				pieceSize = BigDecimal.ONE;
				maxSplitPieces = value.divide(pieceSize, RoundingMode.HALF_UP).intValue(); //int) NumberUtils.divide(value, pieceSize);
			}

			for(int i = 0; i < maxSplitPieces; i++) {
				if(i == 0) {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(boundaryPartitionFilter.getLeftBoundary())
							.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), new DateTime(minDecimal.add(pieceSize))/*toOriginObject(minDateTime.getOriginType())*/)));
				} else if(i == maxSplitPieces - 1) {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), new DateTime(minDecimal.add(pieceSize.multiply(BigDecimal.valueOf(i))))/*toOriginObject(minDateTime.getOriginType())*/))
							.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
				} else {
					partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
							.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), new DateTime(minDecimal.add(pieceSize.multiply(BigDecimal.valueOf(i))))/*toOriginObject(minDateTime.getOriginType())*/))
							.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), new DateTime(minDecimal.add(pieceSize.multiply(BigDecimal.valueOf(i + 1))))/*toOriginObject(minDateTime.getOriginType())*/))
					);
				}
			}
			if(maxSplitPieces == 1) {
				partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
						.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), new DateTime(minDecimal.add(pieceSize))/*toOriginObject(minDateTime.getOriginType())*/))
						.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
			}
		}
		return partitionFilters;
	}

	@Override
	public int compare(DateTime o1, DateTime o2) {
		return o1.compareTo(o2);
	}
}
