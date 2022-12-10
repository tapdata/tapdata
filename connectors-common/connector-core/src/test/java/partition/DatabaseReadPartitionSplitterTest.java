package partition;

import io.tapdata.async.master.ParallelWorkerStateListener;
import io.tapdata.entity.schema.*;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.utils.test.AsyncTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

/**
 * @author aplomb
 */
public class DatabaseReadPartitionSplitterTest extends AsyncTestBase {
	@Test
	public void test() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null);
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1)));
		records.add(map(entry("a", 2)));
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 4)));
		records.add(map(entry("a", 5)));
		records.add(map(entry("a", 6)));
		records.add(map(entry("a", 7)));


		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 1L, null, readPartition -> {
					readPartitionList.add(readPartition);
		})
				.parallelWorkerStateListener((id, fromState, toState) -> {
					if(toState == ParallelWorkerStateListener.STATE_LONG_IDLE) {
						$(() -> Assertions.assertEquals(7, readPartitionList.size()));
					}
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

		waitCompleted(40000L);
	}

}
