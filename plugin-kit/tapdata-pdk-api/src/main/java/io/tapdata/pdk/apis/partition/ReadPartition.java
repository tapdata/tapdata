package io.tapdata.pdk.apis.partition;

import io.tapdata.async.master.JobContext;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitter;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

/**
 * @author aplomb
 */
public class ReadPartition implements Comparable<ReadPartition> {
	private static final String TAG = ReadPartition.class.getSimpleName();
	private TapIndexEx partitionIndex;
	public ReadPartition partitionIndex(TapIndexEx partitionIndex) {
		this.partitionIndex = partitionIndex;
		return this;
	}
	private String id;
	public ReadPartition id(String id) {
		this.id = id;
		return this;
	}
	private Map<String, Object> partitionValues;
	public ReadPartition partitionValues(Map<String, Object> partitionValues) {
		this.partitionValues = partitionValues;
		return this;
	}
	private TapPartitionFilter partitionFilter;
	public ReadPartition partitionFilter(TapPartitionFilter partitionFilter) {
		this.partitionFilter = partitionFilter;
		return this;
	}

	public static ReadPartition create() {
		return new ReadPartition();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public TapPartitionFilter getPartitionFilter() {
		return partitionFilter;
	}

	public void setPartitionFilter(TapPartitionFilter partitionFilter) {
		this.partitionFilter = partitionFilter;
	}

	@Override
	public String toString() {
		return "ReadPartition " + partitionFilter;
	}

	@Override
	public int compareTo(ReadPartition o) {
		if (partitionIndex == null && o.partitionIndex == null)
			throw new CoreException(TapAPIErrorCodes.ERROR_ILLEGAL_PARAMETERS, "at least exist one partitionIndex for readPartition {} compare {}", this, o);
		List<TapIndexField> indexFields = partitionIndex != null ? partitionIndex.getIndexFields() : o.partitionIndex.getIndexFields();
		for (TapIndexField indexField : indexFields) {
			Object curMatchValue = null;
			Object curRightBValue = null;
			if (partitionFilter.getMatch() != null)
				curMatchValue = partitionFilter.getMatch().getObject(indexField.getName());
			if (curMatchValue == null) {
				QueryOperator curRightBoundary = partitionFilter.getRightBoundary();
				if (curRightBoundary != null) {
					curRightBValue = curRightBoundary.getValue();
				} else {
					if (partitionFilter.getLeftBoundary() == null)
						return -1;
					else
						return 1;
				}
			}
			Object oMatchValue = null;
			Object oRightBValue = null;
			if (o.partitionFilter.getMatch() != null)
				oMatchValue = o.partitionFilter.getMatch().getObject(indexField.getName());
			if (oMatchValue == null) {
				QueryOperator oRightBoundary = o.partitionFilter.getRightBoundary();
				if (oRightBoundary != null) {
					oRightBValue = oRightBoundary.getValue();
				} else {
					if (o.partitionFilter.getLeftBoundary() == null)
						return 1;
					else
						return -1;
				}
			}
			if (curMatchValue != null) {
				if (oMatchValue != null) {
					if (curMatchValue instanceof Comparable && oMatchValue instanceof Comparable) {
						int compareResult = ((Comparable) curMatchValue).compareTo(oMatchValue);
						if (compareResult == 0) {
							continue;
						} else {
							return compareResult;
						}
					} else {
						System.out.println("can not compare rightBoundary, rightBoundary.value must be Comparable");
						return 0;
					}
				} else {
					if (curMatchValue instanceof Comparable && oRightBValue instanceof Comparable) {
						int compareResult = ((Comparable) curMatchValue).compareTo(oRightBValue);
						if (compareResult == 0) {
							return 1;
						} else {
							return compareResult;
						}
					} else {
						TapLogger.warn(TAG, "can not compare rightBoundary, rightBoundary.value must be Comparable, curMatchValue {}, oRightBValue {}", curMatchValue, oRightBValue);
						return 0;
					}
				}
			} else {
				if (oMatchValue != null) {
					if (curRightBValue instanceof Comparable && oMatchValue instanceof Comparable) {
						int compareResult = ((Comparable) curRightBValue).compareTo(oMatchValue);
						if (compareResult == 0) {
							return -1;
						} else {
							return compareResult;
						}
					} else {
						TapLogger.warn(TAG, "can not compare rightBoundary, rightBoundary.value must be Comparable, curRightBValue {}, oMatchValue {}", curRightBValue, oMatchValue);
						return 0;
					}
				} else {
					if (curRightBValue instanceof Comparable && oRightBValue instanceof Comparable) {
						int compareResult = ((Comparable) curRightBValue).compareTo(oRightBValue);
						if (compareResult == 0) {
							continue;
						} else {
							return compareResult;
						}
					} else {
						TapLogger.warn(TAG, "can not compare rightBoundary, rightBoundary.value must be Comparable, curRightBValue {}, oRightBValue {}", curRightBValue, oRightBValue);
						return 0;
					}
				}
			}
		}
		return 0;
	}

	public static void main(String[] args) {
		ConcurrentSkipListMap<ReadPartition, Object> map = new ConcurrentSkipListMap<>(ReadPartition::compareTo);
		TapIndexEx indexEx = new TapIndexEx(new TapIndex().indexField(new TapIndexField().name("a")).indexField(new TapIndexField().name("b")));
		ReadPartition r1 = new ReadPartition().partitionFilter(TapPartitionFilter.create().leftBoundary(QueryOperator.lt("a", 1))).partitionIndex(indexEx);
		ReadPartition r2 = new ReadPartition().partitionFilter(TapPartitionFilter.create().rightBoundary(QueryOperator.gte("a", 1))).partitionIndex(indexEx);
		map.put(r2, new Object());
		map.put(r1, new Object());

		NavigableSet<ReadPartition> readPartitions = map.keySet();

		ReadPartition readPartition = map.floorKey(ReadPartition.create().partitionValues(map(entry("a", 3), entry("b", 59))));

	}
}
