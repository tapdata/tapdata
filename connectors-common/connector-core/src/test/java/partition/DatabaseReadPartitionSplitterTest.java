package partition;

import io.tapdata.async.master.ParallelWorkerStateListener;
import io.tapdata.entity.schema.*;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.utils.test.AsyncTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * @author aplomb
 */
public class DatabaseReadPartitionSplitterTest extends AsyncTestBase {
	@Test
	public void testOnlyOneRecordForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null);
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1)));
//		records.add(map(entry("a", 2)));
//		records.add(map(entry("a", 3)));
//		records.add(map(entry("a", 4)));
//		records.add(map(entry("a", 5)));
//		records.add(map(entry("a", 6)));
//		records.add(map(entry("a", 7)));


		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 1L, null, readPartition -> {
					readPartitionList.add(readPartition);
		})
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(1, readPartitionList.size()));
					ReadPartition readPartition = readPartitionList.get(0);
					Assertions.assertNotNull(readPartition);
					Assertions.assertNull(readPartition.getPartitionFilter().getRightBoundary());
					Assertions.assertNull(readPartition.getPartitionFilter().getLeftBoundary());
					Assertions.assertNull(readPartition.getPartitionFilter().getMatch());
					completed();
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

		waitCompleted(4);
	}
	@Test
	public void testTwoRecordForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null);
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1)));
		records.add(map(entry("a", 2)));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 1L, null, readPartition -> {
					readPartitionList.add(readPartition);
				})
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(2, readPartitionList.size()));
					ReadPartition readPartition = readPartitionList.get(0);
					Assertions.assertNotNull(readPartition);
					Assertions.assertEquals(QueryOperator.LT, readPartition.getPartitionFilter().getRightBoundary().getOperator());
					Assertions.assertEquals("a", readPartition.getPartitionFilter().getRightBoundary().getKey());
					Assertions.assertEquals(2, readPartition.getPartitionFilter().getRightBoundary().getValue());
					ReadPartition readPartition1 = readPartitionList.get(1);
					Assertions.assertEquals(QueryOperator.GTE, readPartition1.getPartitionFilter().getLeftBoundary().getOperator());
					Assertions.assertEquals("a", readPartition1.getPartitionFilter().getLeftBoundary().getKey());
					Assertions.assertEquals(2, readPartition1.getPartitionFilter().getLeftBoundary().getValue());
					completed();
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

		waitCompleted(4);
	}
	@Test
	public void testThreeRecordForInt() throws Throwable {
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

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 1L, null, readPartition -> {
					readPartitionList.add(readPartition);
				})
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(2, readPartitionList.size()));
					ReadPartition readPartition = readPartitionList.get(0);
					Assertions.assertNotNull(readPartition);
					Assertions.assertEquals(QueryOperator.LT, readPartition.getPartitionFilter().getRightBoundary().getOperator());
					Assertions.assertEquals("a", readPartition.getPartitionFilter().getRightBoundary().getKey());
					Assertions.assertEquals(2, readPartition.getPartitionFilter().getRightBoundary().getValue());
					ReadPartition readPartition1 = readPartitionList.get(1);
					Assertions.assertEquals(QueryOperator.GTE, readPartition1.getPartitionFilter().getLeftBoundary().getOperator());
					Assertions.assertEquals("a", readPartition1.getPartitionFilter().getLeftBoundary().getKey());
					Assertions.assertEquals(2, readPartition1.getPartitionFilter().getLeftBoundary().getValue());
					completed();
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

		waitCompleted(4);
	}

	@Test
	public void testManyRecordsTheSameForInt() throws Throwable {
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
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 3)));
		records.add(map(entry("a", 3)));


		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 1L, null, readPartition -> {
					readPartitionList.add(readPartition);
				})
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(4, readPartitionList.size()));
					ReadPartition readPartition = readPartitionList.get(0);
					$(() -> Assertions.assertNotNull(readPartition));
					$(() -> Assertions.assertEquals(QueryOperator.LT, readPartition.getPartitionFilter().getRightBoundary().getOperator()));
					$(() -> Assertions.assertEquals("a", readPartition.getPartitionFilter().getRightBoundary().getKey()));
					$(() -> Assertions.assertEquals(2, readPartition.getPartitionFilter().getRightBoundary().getValue()));

					ReadPartition readPartition1 = readPartitionList.get(1);
					$(() -> Assertions.assertEquals(QueryOperator.GTE, readPartition1.getPartitionFilter().getLeftBoundary().getOperator()));
					$(() -> Assertions.assertEquals("a", readPartition1.getPartitionFilter().getLeftBoundary().getKey()));
					$(() -> Assertions.assertEquals(2, readPartition1.getPartitionFilter().getLeftBoundary().getValue()));

					ReadPartition readPartition2 = readPartitionList.get(2);
					$(() -> Assertions.assertNull(readPartition2.getPartitionFilter().getLeftBoundary()));
					$(() -> Assertions.assertNull(readPartition2.getPartitionFilter().getRightBoundary()));
					$(() -> Assertions.assertEquals(3, readPartition2.getPartitionFilter().getMatch().get("a")));

					ReadPartition readPartition3 = readPartitionList.get(3);
					$(() -> Assertions.assertEquals(QueryOperator.GT, readPartition3.getPartitionFilter().getLeftBoundary().getOperator()));
					$(() -> Assertions.assertEquals("a", readPartition3.getPartitionFilter().getLeftBoundary().getKey()));
					$(() -> Assertions.assertEquals(3, readPartition3.getPartitionFilter().getLeftBoundary().getValue()));
					completed();
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

		waitCompleted(4);
	}

	@Test
	public void testThreeRecordForDouble() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null);
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1.0d)));
		records.add(map(entry("a", 2.0d)));
		records.add(map(entry("a", 3.0d)));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 1L, null, readPartition -> {
					readPartitionList.add(readPartition);
				})
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(3, readPartitionList.size()));
					ReadPartition readPartition = readPartitionList.get(0);
					Assertions.assertNotNull(readPartition);
					Assertions.assertEquals(QueryOperator.LT, readPartition.getPartitionFilter().getRightBoundary().getOperator());
					Assertions.assertEquals("a", readPartition.getPartitionFilter().getRightBoundary().getKey());
					Assertions.assertEquals(1.6666666666666665d, readPartition.getPartitionFilter().getRightBoundary().getValue());
					ReadPartition readPartition1 = readPartitionList.get(2);
					Assertions.assertEquals(QueryOperator.GTE, readPartition1.getPartitionFilter().getLeftBoundary().getOperator());
					Assertions.assertEquals("a", readPartition1.getPartitionFilter().getLeftBoundary().getKey());
					Assertions.assertEquals(2.333333333333333d, readPartition1.getPartitionFilter().getLeftBoundary().getValue());
					completed();
				})
				.queryFieldMinMaxValue(new DoubleFieldMinMaxHandler(records))
				.countByPartitionFilter(new DoubleFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

		waitCompleted(4);
	}

	@Test
	public void testThreeRecordForBoolean() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null);
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", false)));
		records.add(map(entry("a", true)));
		records.add(map(entry("a", true)));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 1L, null, readPartition -> {
					readPartitionList.add(readPartition);
				})
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(2, readPartitionList.size()));
					ReadPartition readPartition = readPartitionList.get(0);
					Assertions.assertNotNull(readPartition);
					Assertions.assertEquals(false, readPartition.getPartitionFilter().getMatch().get("a"));
					ReadPartition readPartition1 = readPartitionList.get(1);
					Assertions.assertEquals(true, readPartition1.getPartitionFilter().getMatch().get("a"));
					completed();
				})
				.queryFieldMinMaxValue(new BooleanFieldMinMaxHandler(records))
				.countByPartitionFilter(new BooleanFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

		waitCompleted(48888888);
	}

	@Test
	public void testMultiPrimaryKeysForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null);
		TapTable table = new TapTable("table")
				.add(field("a", "varchar"))
				.add(field("b", "varchar"))

				.add(index("a1").indexField(indexField("a").fieldAsc(true)).indexField(indexField("b").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1), entry("b", 5)));
		records.add(map(entry("a", 2), entry("b", 51)));
		records.add(map(entry("a", 3), entry("b", 53)));
		records.add(map(entry("a", 3), entry("b", 54)));
		records.add(map(entry("a", 3), entry("b", 55)));
		records.add(map(entry("a", 3), entry("b", 56)));
		records.add(map(entry("a", 3), entry("b", 58)));
		records.add(map(entry("a", 3), entry("b", 59)));
		records.add(map(entry("a", 3), entry("b", 512)));
		records.add(map(entry("a", 3), entry("b", 513)));
		records.add(map(entry("a", 3), entry("b", 513)));
		records.add(map(entry("a", 3), entry("b", 515)));


		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 1L, null, readPartition -> {
					readPartitionList.add(readPartition);
				})
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(11, readPartitionList.size()));
					ReadPartition readPartition = readPartitionList.get(0);
					$(() -> Assertions.assertNotNull(readPartition));
					$(() -> Assertions.assertEquals(QueryOperator.LT, readPartition.getPartitionFilter().getRightBoundary().getOperator()));
					$(() -> Assertions.assertEquals("a", readPartition.getPartitionFilter().getRightBoundary().getKey()));
					$(() -> Assertions.assertEquals(2, readPartition.getPartitionFilter().getRightBoundary().getValue()));
//
//					ReadPartition readPartition1 = readPartitionList.get(1);
//					$(() -> Assertions.assertEquals(QueryOperator.GTE, readPartition1.getPartitionFilter().getLeftBoundary().getOperator()));
//					$(() -> Assertions.assertEquals("a", readPartition1.getPartitionFilter().getLeftBoundary().getKey()));
//					$(() -> Assertions.assertEquals(2, readPartition1.getPartitionFilter().getLeftBoundary().getValue()));
//
					ReadPartition readPartition2 = readPartitionList.get(2);
					$(() -> Assertions.assertEquals(QueryOperator.LT, readPartition2.getPartitionFilter().getRightBoundary().getOperator()));
					$(() -> Assertions.assertEquals("b", readPartition2.getPartitionFilter().getRightBoundary().getKey()));
					$(() -> Assertions.assertEquals(54, readPartition2.getPartitionFilter().getRightBoundary().getValue()));
					$(() -> Assertions.assertEquals(3, readPartition2.getPartitionFilter().getMatch().get("a")));
//
					ReadPartition readPartition3 = readPartitionList.get(7);
					$(() -> Assertions.assertEquals(QueryOperator.GTE, readPartition3.getPartitionFilter().getLeftBoundary().getOperator()));
					$(() -> Assertions.assertEquals("b", readPartition3.getPartitionFilter().getLeftBoundary().getKey()));
					$(() -> Assertions.assertEquals(99, readPartition3.getPartitionFilter().getLeftBoundary().getValue()));
					$(() -> Assertions.assertEquals(QueryOperator.LT, readPartition3.getPartitionFilter().getRightBoundary().getOperator()));
					$(() -> Assertions.assertEquals("b", readPartition3.getPartitionFilter().getRightBoundary().getKey()));
					$(() -> Assertions.assertEquals(513, readPartition3.getPartitionFilter().getRightBoundary().getValue()));
					$(() -> Assertions.assertEquals(3, readPartition3.getPartitionFilter().getMatch().get("a")));

					ReadPartition readPartition9 = readPartitionList.get(9);
					$(() -> Assertions.assertEquals(3, readPartition9.getPartitionFilter().getMatch().get("a")));
					$(() -> Assertions.assertEquals(QueryOperator.GTE, readPartition9.getPartitionFilter().getLeftBoundary().getOperator()));
					$(() -> Assertions.assertEquals("b", readPartition9.getPartitionFilter().getLeftBoundary().getKey()));
					$(() -> Assertions.assertEquals(514, readPartition9.getPartitionFilter().getLeftBoundary().getValue()));

					ReadPartition readPartition10 = readPartitionList.get(10);
					$(() -> Assertions.assertEquals(QueryOperator.GT, readPartition10.getPartitionFilter().getLeftBoundary().getOperator()));
					$(() -> Assertions.assertEquals("a", readPartition10.getPartitionFilter().getLeftBoundary().getKey()));
					$(() -> Assertions.assertEquals(3, readPartition10.getPartitionFilter().getLeftBoundary().getValue()));

					completed();
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

		waitCompleted(41111111);
	}

	@Test
	public void testMultiPrimaryKeysForIntMaxRecord2() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null);
		TapTable table = new TapTable("table")
				.add(field("a", "varchar"))
				.add(field("b", "varchar"))

				.add(index("a1").indexField(indexField("a").fieldAsc(true)).indexField(indexField("b").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1), entry("b", 5)));
		records.add(map(entry("a", 2), entry("b", 51)));
		records.add(map(entry("a", 3), entry("b", 53)));
		records.add(map(entry("a", 3), entry("b", 54)));
		records.add(map(entry("a", 3), entry("b", 55)));
		records.add(map(entry("a", 3), entry("b", 56)));
		records.add(map(entry("a", 3), entry("b", 58)));
		records.add(map(entry("a", 3), entry("b", 59)));
		records.add(map(entry("a", 3), entry("b", 512)));
		records.add(map(entry("a", 3), entry("b", 513)));
		records.add(map(entry("a", 3), entry("b", 513)));
		records.add(map(entry("a", 3), entry("b", 515)));


		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, 2L, null, readPartition -> {
					readPartitionList.add(readPartition);
				})
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(6, readPartitionList.size()));
					ReadPartition readPartition = readPartitionList.get(0);
					$(() -> Assertions.assertNotNull(readPartition));
					$(() -> Assertions.assertEquals(QueryOperator.LT, readPartition.getPartitionFilter().getRightBoundary().getOperator()));
					$(() -> Assertions.assertEquals("a", readPartition.getPartitionFilter().getRightBoundary().getKey()));
					$(() -> Assertions.assertEquals(3, readPartition.getPartitionFilter().getRightBoundary().getValue()));

					ReadPartition readPartition1 = readPartitionList.get(1);
					$(() -> Assertions.assertEquals(QueryOperator.LT, readPartition1.getPartitionFilter().getRightBoundary().getOperator()));
					$(() -> Assertions.assertEquals(3, readPartition1.getPartitionFilter().getMatch().get("a")));
					$(() -> Assertions.assertEquals("b", readPartition1.getPartitionFilter().getRightBoundary().getKey()));
					$(() -> Assertions.assertEquals(55, readPartition1.getPartitionFilter().getRightBoundary().getValue()));
					$(() -> Assertions.assertNull(readPartition1.getPartitionFilter().getLeftBoundary()));

					ReadPartition readPartition2 = readPartitionList.get(2);
					$(() -> Assertions.assertEquals(QueryOperator.LT, readPartition2.getPartitionFilter().getRightBoundary().getOperator()));
					$(() -> Assertions.assertEquals("b", readPartition2.getPartitionFilter().getRightBoundary().getKey()));
					$(() -> Assertions.assertEquals(57, readPartition2.getPartitionFilter().getRightBoundary().getValue()));
					$(() -> Assertions.assertEquals(QueryOperator.GTE, readPartition2.getPartitionFilter().getLeftBoundary().getOperator()));
					$(() -> Assertions.assertEquals("b", readPartition2.getPartitionFilter().getLeftBoundary().getKey()));
					$(() -> Assertions.assertEquals(55, readPartition2.getPartitionFilter().getLeftBoundary().getValue()));
					$(() -> Assertions.assertEquals(3, readPartition2.getPartitionFilter().getMatch().get("a")));
//
					ReadPartition readPartition3 = readPartitionList.get(4);
					$(() -> Assertions.assertEquals(QueryOperator.GTE, readPartition3.getPartitionFilter().getLeftBoundary().getOperator()));
					$(() -> Assertions.assertEquals("b", readPartition3.getPartitionFilter().getLeftBoundary().getKey()));
					$(() -> Assertions.assertEquals(145, readPartition3.getPartitionFilter().getLeftBoundary().getValue()));
					$(() -> Assertions.assertEquals(3, readPartition3.getPartitionFilter().getMatch().get("a")));

					ReadPartition readPartition9 = readPartitionList.get(5);
					$(() -> Assertions.assertEquals(QueryOperator.GT, readPartition9.getPartitionFilter().getLeftBoundary().getOperator()));
					$(() -> Assertions.assertEquals("a", readPartition9.getPartitionFilter().getLeftBoundary().getKey()));
					$(() -> Assertions.assertEquals(3, readPartition9.getPartitionFilter().getLeftBoundary().getValue()));

					completed();
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
//				.countIsSlow(true)
				.startSplitting();

		waitCompleted(4);
	}
}
