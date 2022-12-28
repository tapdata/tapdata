package io.tapdata.pdk.apis.partition;

import io.tapdata.async.master.JobContext;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
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
		TapIndexEx indexEx = partitionIndex;
		//partition keys
		if(partitionFilter == null || o.partitionFilter == null)
			throw new CoreException(TapAPIErrorCodes.ERROR_PARTITION_FILTER_NULL, "partitionFilter can not be null while compare from {} to {}", this, o);
		int compare = 0;
		DataMap match = partitionFilter.getMatch();
		DataMap match1 = o.partitionFilter.getMatch();

		QueryOperator leftBoundary = partitionFilter.getLeftBoundary();
		QueryOperator leftBoundary1 = o.partitionFilter.getLeftBoundary();

		QueryOperator rightBoundary = partitionFilter.getRightBoundary();
		QueryOperator rightBoundary1 = o.partitionFilter.getRightBoundary();

		List<TapIndexField> fieldList = indexEx.getIndexFields();
		for(TapIndexField field : fieldList) {
			Object matchValue = match.get(field.getName());
			Object matchValue1 = match1.get(field.getName());
			if(matchValue instanceof Comparable && matchValue1 instanceof Comparable) {
				compare = ((Comparable) matchValue).compareTo(matchValue1);
				if(compare == 0)
					continue;
				return compare;
			}

			if(leftBoundary != null && leftBoundary.getKey().equals(field.getName())) {
				Object leftValue = leftBoundary.getValue();
				if(matchValue1 != null) {
					if(leftValue instanceof Comparable) {
						int c = ((Comparable) leftValue).compareTo(matchValue1);
						switch (leftBoundary.getOperator()) {
							case QueryOperator.GT:
								return c < 0 ? -1 : 1;
							case QueryOperator.GTE:
								if(c != 0) {
									return c < 0 ? -1 : 1;
								}
								break;
							case QueryOperator.LT:
							case QueryOperator.LTE:
								throw new CoreException(TapAPIErrorCodes.ILLEGAL_OPERATOR_FOR_LEFT_BOUNDARY, "left boundary should only have gt or gte, {}", leftBoundary);
						}
					}
				} else if(rightBoundary1 != null && rightBoundary1.getKey().equals(field.getName())) {
					Object rightValue1 = rightBoundary1.getValue();
					if(leftValue instanceof Comparable) {
						int c = ((Comparable) leftValue).compareTo(rightValue1);

					}
				}
			}
			if(rightBoundary != null && rightBoundary.getKey().equals(field.getName())) {

			}
			if(leftBoundary1.getKey().equals(field.getName())) {

			}
		}

		if(match != null && match1 == null)
			return 1;
		else if(match == null)
			return -1;

		if(match != null && match1 != null) {
			if(match.size() > match1.size())
				return 1;
			else if(match.size() < match1.size())
				return -1;
			else {
				for(Map.Entry<String, Object> entry : match.entrySet()) {
					String key = entry.getKey();
					Object value = entry.getValue();
					Object value1 = match1.get(key);
					if(value1 == null)
						return 1;
					if(value instanceof Comparable && value1 instanceof Comparable) {
						compare = ((Comparable) value).compareTo(value1);
					}
					if(compare != 0)
						return compare;
//					TypeSplitter typeSplitter = typeSplitterMap.get(TypeSplitterMap.detectType(value));
//					compare = typeSplitter.compare(value, value1);
				}
			}
		}
//		QueryOperator leftBoundary = partitionFilter.getLeftBoundary();
//		QueryOperator leftBoundary1 = o.partitionFilter.getLeftBoundary();
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
