package io.tapdata.partition.splitter;

import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.List;

/**
 * @author aplomb
 */
public interface TypeSplitter {
	List<TapPartitionFilter> split(FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces);
}
