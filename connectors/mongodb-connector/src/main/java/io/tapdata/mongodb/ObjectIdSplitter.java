package io.tapdata.mongodb;

import io.tapdata.pdk.apis.partition.splitter.TypeSplitter;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

/**
 * @author aplomb
 */
public class ObjectIdSplitter implements TypeSplitter<ObjectId> {
	@Override
	public List<TapPartitionFilter> split(TapPartitionFilter boundaryPartitionFilter, FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces) {
		ObjectId min = (ObjectId) fieldMinMaxValue.getMin();
		ObjectId max = (ObjectId) fieldMinMaxValue.getMax();

		List<TapPartitionFilter> partitionFilters = new ArrayList<>();
		if(min == null || max == null) {
			partitionFilters.add(boundaryPartitionFilter);
			return partitionFilters;
		} else if(min.equals(max)) {
			partitionFilters.addAll(TapPartitionFilter.filtersWhenMinMaxEquals(boundaryPartitionFilter, fieldMinMaxValue, min));
			return partitionFilters;
		}

		int minSeconds = min.getTimestamp();
		int maxSeconds = max.getTimestamp();
		int value = maxSeconds - minSeconds;
		int pieceSize = value / maxSplitPieces;
		if(pieceSize == 0) {
			pieceSize = 1;
			maxSplitPieces = value / pieceSize;
		}

		for(int i = 0; i < maxSplitPieces; i++) {
			if(i == 0) {
				partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
						.leftBoundary(boundaryPartitionFilter.getLeftBoundary())
						.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), new ObjectId(minSeconds + pieceSize, 0))));
			} else if(i == maxSplitPieces - 1) {
				partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
						.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), new ObjectId(minSeconds + pieceSize * i, 0)))
						.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
			} else {
				partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
						.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), new ObjectId(minSeconds + pieceSize * i, 0)))
						.rightBoundary(QueryOperator.lt(fieldMinMaxValue.getFieldName(), new ObjectId(minSeconds + pieceSize * (i + 1), 0)))
				);
			}
		}
		if(maxSplitPieces == 1) {
			partitionFilters.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch())
					.leftBoundary(QueryOperator.gte(fieldMinMaxValue.getFieldName(), new ObjectId(minSeconds + pieceSize, 0)))
					.rightBoundary(boundaryPartitionFilter.getRightBoundary()));
		}
		return partitionFilters;
	}

	@Override
	public int compare(ObjectId o1, ObjectId o2) {
		return o1.compareTo(o2);
	}
}
