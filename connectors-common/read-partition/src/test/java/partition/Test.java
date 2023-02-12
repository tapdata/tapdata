package partition;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.partition.splitter.BooleanSplitter;
import io.tapdata.pdk.apis.partition.splitter.DateTimeSplitter;
import io.tapdata.pdk.apis.partition.splitter.NumberSplitter;
import io.tapdata.pdk.apis.partition.splitter.StringSplitter;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.Comparator;
import java.util.List;

import static io.tapdata.base.ConnectorBase.list;

/**
 * @author aplomb
 */
public class Test {
	public static void main(String[] args) {
		ReadPartition readPartition = ReadPartition.create().id("id").partitionFilter(TapPartitionFilter.create().rightBoundary(QueryOperator.lt("a", 1)).leftBoundary(QueryOperator.gt("a", 123)));
		System.out.println(readPartition);

		Comparator<String> comparator = String::compareTo;
		List<String> strList = ConnectorBase.list("a1", "b", "c", "a", "a", "A", "a4");
		System.out.println("strList " + strList);
		strList.sort(comparator);
		System.out.println("sorted strList " + strList);

		int value = NumberSplitter.INSTANCE.compare(23d, 1l);
		int value1 = NumberSplitter.INSTANCE.compare(23d, 23d);
		int value2 = NumberSplitter.INSTANCE.compare(23d, 24d);

		int v1 = StringSplitter.INSTANCE.compare("aaa", "aab");
		int v2 = StringSplitter.INSTANCE.compare("aaa", "aaa");
		int v3 = StringSplitter.INSTANCE.compare("aab", "aaa");

		int a1 = DateTimeSplitter.INSTANCE.compare(new DateTime(1671848089546L, 3), new DateTime(1671848089446L, 3));
		int a2 = DateTimeSplitter.INSTANCE.compare(new DateTime(1671848089446L, 3), new DateTime(1671848089446L, 3));
		int a3 = DateTimeSplitter.INSTANCE.compare(new DateTime(1671848089346L, 3), new DateTime(1671848089446L, 3));


		int aa1 = DateTimeSplitter.INSTANCE.compare(new DateTime(1681848089546L, 3), new DateTime(1671848089446L, 3));
		int aa2 = DateTimeSplitter.INSTANCE.compare(new DateTime(1671848089446L, 3), new DateTime(1671848089446L, 3));
		int aa3 = DateTimeSplitter.INSTANCE.compare(new DateTime(1631848089346L, 3), new DateTime(1671848089446L, 3));

		int b1 = BooleanSplitter.INSTANCE.compare(true, false);
		int b2 = BooleanSplitter.INSTANCE.compare(true, true);
		int b3 = BooleanSplitter.INSTANCE.compare(false, true);
	}
}
