package partition;

import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

/**
 * @author aplomb
 */
public class Test {
	public static void main(String[] args) {
		ReadPartition readPartition = ReadPartition.create().id("id").partitionFilter(TapPartitionFilter.create().rightBoundary(QueryOperator.lt("a", 1)).leftBoundary(QueryOperator.gt("a", 123)));
		System.out.println(readPartition);

	}
}
