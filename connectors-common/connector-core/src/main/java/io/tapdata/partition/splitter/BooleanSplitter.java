package io.tapdata.partition.splitter;

import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author aplomb
 */
public class BooleanSplitter implements TypeSplitter {
	public static BooleanSplitter INSTANCE = new BooleanSplitter();
	@Override
	public List<TapPartitionFilter> split(FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces) {
		return list(
				TapPartitionFilter.create().match(fieldMinMaxValue.getFieldName(), true),
				TapPartitionFilter.create().match(fieldMinMaxValue.getFieldName(), false)
		);
	}
}
