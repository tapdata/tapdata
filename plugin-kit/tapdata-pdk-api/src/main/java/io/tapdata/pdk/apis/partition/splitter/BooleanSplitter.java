package io.tapdata.pdk.apis.partition.splitter;

import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author aplomb
 */
public class BooleanSplitter implements TypeSplitter<Boolean> {
	public static BooleanSplitter INSTANCE = new BooleanSplitter();
	@Override
	public List<TapPartitionFilter> split(TapPartitionFilter boundaryPartitionFilter, FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces) {
		List<TapPartitionFilter> list = list();
		Boolean min = (Boolean) fieldMinMaxValue.getMin();
		if(min != null && !min)
			list.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch()).match(fieldMinMaxValue.getFieldName(), false));
		Boolean max = (Boolean) fieldMinMaxValue.getMax();
		if(max != null && max)
			list.add(TapPartitionFilter.create().resetMatch(boundaryPartitionFilter.getMatch()).match(fieldMinMaxValue.getFieldName(), true));
		return list;
	}

	@Override
	public int compare(Boolean o1, Boolean o2) {
		return o1.compareTo(o2);
	}
}
