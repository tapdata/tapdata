package io.tapdata.pdk.apis.partition;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.QueryOperator;

import java.io.Serializable;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author aplomb
 */
public class ReadPartition implements Comparable<ReadPartition>, Serializable {
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
		if(partitionFilter != null && partitionFilter.matchAny())
			return 1;
		if (partitionIndex == null && o.partitionIndex == null)
			throw new CoreException(TapAPIErrorCodes.ERROR_ILLEGAL_PARAMETERS, "at least exist one partitionIndex for readPartition {} compare {}", this, o);
		List<TapIndexField> indexFields = partitionIndex != null ? partitionIndex.getIndexFields() : o.partitionIndex.getIndexFields();
		for (TapIndexField indexField : indexFields) {
			Object curMatchValue = null;
			Object curRightBValue = null;
			DataMap curMatch = partitionFilter.getMatch();
			DataMap oMatch = o.partitionFilter.getMatch();
			if (curMatch != null)
				curMatchValue = curMatch.getObject(indexField.getName());
			if (curMatchValue == null) {
				QueryOperator curRightBoundary = partitionFilter.getRightBoundary();
				if (curRightBoundary != null) {
					curRightBValue = curRightBoundary.getValue();
				} else {
					// 只有一个分区的情况 ==> return 0;
					return null == partitionFilter.getLeftBoundary() ? 1 : (
							((oMatch == null || oMatch.getObject(indexField.getName()) == null)
									&& o.partitionFilter.getRightBoundary() == null
									&& o.partitionFilter.getLeftBoundary() == null )? 0 : -1 );
				}
			}
			Object oMatchValue = null;
			Object oRightBValue = null;
			if (oMatch != null)
				oMatchValue = oMatch.getObject(indexField.getName());
			if (oMatchValue == null) {
				QueryOperator oRightBoundary = o.partitionFilter.getRightBoundary();
				if (oRightBoundary != null) {
					oRightBValue = oRightBoundary.getValue();
				} else {
					// o永远都是map中的值，如果o都为null，则直接匹配，注：这种情况下map中只有一个值且这个值中的Filter全部为null
					return o.partitionFilter.getLeftBoundary() == null ? 0 : -1;
				}
			}
			if (curMatchValue != null) {
				if (oMatchValue != null) {
					if (curMatchValue instanceof Comparable && oMatchValue instanceof Comparable) {
						int compareResult = ((Comparable) curMatchValue).compareTo(oMatchValue);
						if (compareResult != 0) {
							return compareResult;
						}
					} else {
						//TapLogger.debug(TAG, "can not compare rightBoundary, rightBoundary.value must be Comparable");
						return 0;
					}
				} else {
					if (curMatchValue instanceof Comparable && oRightBValue instanceof Comparable) {
						int compareResult = ((Comparable) curMatchValue).compareTo(oRightBValue);
						return compareResult == 0 ? 1 : compareResult;
					} else {
						//TapLogger.debug(TAG, "can not compare rightBoundary, rightBoundary.value must be Comparable, curMatchValue {}, oRightBValue {}", curMatchValue, oRightBValue);
						return 0;
					}
				}
			} else {
				if (oMatchValue != null) {
					if (curRightBValue instanceof Comparable && oMatchValue instanceof Comparable) {
						int compareResult = ((Comparable) curRightBValue).compareTo(oMatchValue);
						return compareResult == 0 ? -1 : compareResult;
					} else {
						//TapLogger.debug(TAG, "can not compare rightBoundary, rightBoundary.value must be Comparable, curRightBValue {}, oMatchValue {}", curRightBValue, oMatchValue);
						return 0;
					}
				} else {
					if (curRightBValue instanceof Comparable && oRightBValue instanceof Comparable) {
						int compareResult = ((Comparable) curRightBValue).compareTo(oRightBValue);
						if (compareResult != 0) {
							return compareResult;
						}
					} else {
						//TapLogger.debug(TAG, "can not compare rightBoundary, rightBoundary.value must be Comparable, curRightBValue {}, oRightBValue {}", curRightBValue, oRightBValue);
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


	}


}
