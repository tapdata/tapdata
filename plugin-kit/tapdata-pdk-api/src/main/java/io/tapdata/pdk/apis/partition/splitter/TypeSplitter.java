package io.tapdata.pdk.apis.partition.splitter;

import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.Comparator;
import java.util.List;

/**
 * @author aplomb
 */
public interface TypeSplitter<T> extends Comparator<T> {
	List<TapPartitionFilter> split(TapPartitionFilter partitionFilter, FieldMinMaxValue fieldMinMaxValue, int maxSplitPieces);
}
