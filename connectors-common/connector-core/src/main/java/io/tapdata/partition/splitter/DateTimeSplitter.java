package io.tapdata.partition.splitter;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.List;

/**
 * @author aplomb
 */
public class DateTimeSplitter implements TypeSplitter {
	public static DateTimeSplitter INSTANCE = new DateTimeSplitter();
	@Override
	public List<TapPartitionFilter> split(FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces) {
		return null;
	}
}
