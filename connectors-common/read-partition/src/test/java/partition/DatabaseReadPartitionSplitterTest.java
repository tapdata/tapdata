package partition;

import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.*;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.utils.test.AsyncTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author aplomb
 */
public class DatabaseReadPartitionSplitterTest extends AsyncTestBase {
	@Test
	public void testNoRecordForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
//		records.add(map(entry("a", 1)));
//		records.add(map(entry("a", 2)));
//		records.add(map(entry("a", 3)));
//		records.add(map(entry("a", 4)));
//		records.add(map(entry("a", 5)));
//		records.add(map(entry("a", 6)));
//		records.add(map(entry("a", 7)));


		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();

		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartition -> {
					readPartitionList.add(readPartition);
				}))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(1, readPartitionList.size());
					assertEquals("ReadPartition TapPartitionFilter ", readPartitionList.get(0).toString());
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}

	@Test
	public void testOnlyOneRecordForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
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

		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(3, readPartitionList.size());
					assertEquals("ReadPartition TapPartitionFilter rightBoundary a<1; ", readPartitionList.get(0).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 1, }", readPartitionList.get(1).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>1; ", readPartitionList.get(2).toString());
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}
	@Test
	public void testTwoRecordForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1)));
		records.add(map(entry("a", 2)));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(5, readPartitionList.size());
					assertEquals("ReadPartition TapPartitionFilter rightBoundary a<1; ", readPartitionList.get(0).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 1, }", readPartitionList.get(1).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>1; rightBoundary a<2; ", readPartitionList.get(2).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 2, }", readPartitionList.get(3).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>2; ", readPartitionList.get(4).toString());
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}
	@Test
	public void testThreeRecordForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1)));
		records.add(map(entry("a", 2)));
		records.add(map(entry("a", 3)));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(7, readPartitionList.size());
					assertEquals("ReadPartition TapPartitionFilter rightBoundary a<1; ", readPartitionList.get(0).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 1, }", readPartitionList.get(1).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>1; rightBoundary a<2; ", readPartitionList.get(2).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 2, }", readPartitionList.get(3).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>2; rightBoundary a<3; ", readPartitionList.get(4).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 3, }", readPartitionList.get(5).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>3; ", readPartitionList.get(6).toString());
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}

	@Test
	public void testManyRecordsTheSameForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
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
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartition -> readPartitionList.add(readPartition)))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(7, readPartitionList.size());
					assertEquals("ReadPartition TapPartitionFilter rightBoundary a<1; ", readPartitionList.get(0).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 1, }", readPartitionList.get(1).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>1; rightBoundary a<2; ", readPartitionList.get(2).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 2, }", readPartitionList.get(3).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>2; rightBoundary a<3; ", readPartitionList.get(4).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 3, }", readPartitionList.get(5).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>3; ", readPartitionList.get(6).toString());
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

	}

	@Test
	public void testThreeRecordForDouble() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", 1.0d)));
		records.add(map(entry("a", 2.0d)));
		records.add(map(entry("a", 3.0d)));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(5, readPartitionList.size()));
					assertEquals("ReadPartition TapPartitionFilter rightBoundary a<1.02; ", readPartitionList.get(0).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>=1.02; rightBoundary a<2.0; ", readPartitionList.get(1).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>=2.0; rightBoundary a<2.02; ", readPartitionList.get(2).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>=2.02; rightBoundary a<2.98; ", readPartitionList.get(3).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>=2.98; ", readPartitionList.get(4).toString());
				})
				.queryFieldMinMaxValue(new DoubleFieldMinMaxHandler(records))
				.countByPartitionFilter(new DoubleFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}

	@Test
	public void testThreeRecordForDateTime() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", new Date(1670861303123L))));
		records.add(map(entry("a", new Date(1670861305123L))));
		records.add(map(entry("a", new Date(1670861306123L))));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(9, readPartitionList.size()));
					assertEquals("ReadPartition TapPartitionFilter rightBoundary a<'DateTime nano 153000000 seconds 1670861303 timeZone null'; ", readPartitionList.get(0).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 153000000 seconds 1670861303 timeZone null'; rightBoundary a<'DateTime nano 473000000 seconds 1670861304 timeZone null'; ", readPartitionList.get(1).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 473000000 seconds 1670861304 timeZone null'; rightBoundary a<'DateTime nano 503000000 seconds 1670861305 timeZone null'; ", readPartitionList.get(2).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 503000000 seconds 1670861305 timeZone null'; rightBoundary a<'DateTime nano 103000000 seconds 1670861305 timeZone null'; ", readPartitionList.get(3).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 103000000 seconds 1670861305 timeZone null'; rightBoundary a<'DateTime nano 133000000 seconds 1670861305 timeZone null'; ", readPartitionList.get(4).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 133000000 seconds 1670861305 timeZone null'; rightBoundary a<'DateTime nano 493000000 seconds 1670861305 timeZone null'; ", readPartitionList.get(5).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 493000000 seconds 1670861305 timeZone null'; rightBoundary a<'DateTime nano 523000000 seconds 1670861306 timeZone null'; ", readPartitionList.get(6).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 523000000 seconds 1670861306 timeZone null'; rightBoundary a<'DateTime nano 93000000 seconds 1670861306 timeZone null'; ", readPartitionList.get(7).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 93000000 seconds 1670861306 timeZone null'; ", readPartitionList.get(8).toString());
				})
				.queryFieldMinMaxValue(new DateTimeFieldMinMaxHandler(records))
				.countByPartitionFilter(new DateTimeFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}

	@Test
	public void testThreeRecordForDateTime1() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", new DateTime(new Date(1670861303123L)))));
		records.add(map(entry("a", new DateTime(new Date(1670861305123L)))));
		records.add(map(entry("a", new DateTime(new Date(1670861306123L)))));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(9, readPartitionList.size()));
					assertEquals("ReadPartition TapPartitionFilter rightBoundary a<'DateTime nano 153000000 seconds 1670861303 timeZone null'; ", readPartitionList.get(0).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 153000000 seconds 1670861303 timeZone null'; rightBoundary a<'DateTime nano 473000000 seconds 1670861304 timeZone null'; ", readPartitionList.get(1).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 473000000 seconds 1670861304 timeZone null'; rightBoundary a<'DateTime nano 503000000 seconds 1670861305 timeZone null'; ", readPartitionList.get(2).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 503000000 seconds 1670861305 timeZone null'; rightBoundary a<'DateTime nano 103000000 seconds 1670861305 timeZone null'; ", readPartitionList.get(3).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 103000000 seconds 1670861305 timeZone null'; rightBoundary a<'DateTime nano 133000000 seconds 1670861305 timeZone null'; ", readPartitionList.get(4).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 133000000 seconds 1670861305 timeZone null'; rightBoundary a<'DateTime nano 493000000 seconds 1670861305 timeZone null'; ", readPartitionList.get(5).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 493000000 seconds 1670861305 timeZone null'; rightBoundary a<'DateTime nano 523000000 seconds 1670861306 timeZone null'; ", readPartitionList.get(6).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 523000000 seconds 1670861306 timeZone null'; rightBoundary a<'DateTime nano 93000000 seconds 1670861306 timeZone null'; ", readPartitionList.get(7).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>='DateTime nano 93000000 seconds 1670861306 timeZone null'; ", readPartitionList.get(8).toString());
				})
				.queryFieldMinMaxValue(new DateTimeFieldMinMaxHandler(records))
				.countByPartitionFilter(new DateTimeFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}

	@Test
	public void testThreeRecordForString() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", "aaaFd")));
		records.add(map(entry("a", "aaabc")));
		records.add(map(entry("a", "aaacd")));
		records.add(map(entry("a", "aaaed")));
		records.add(map(entry("a", "bbbbb")));
		records.add(map(entry("a", "bbbbb1")));
		records.add(map(entry("a", "bbbbb11")));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(14, readPartitionList.size()));
					String[] strs = new String[]{"ReadPartition TapPartitionFilter rightBoundary a<'aaaFd'; ",
							"ReadPartition TapPartitionFilter match {a: aaaFd, }",
							"ReadPartition TapPartitionFilter leftBoundary a>'aaaFd'; rightBoundary a<'aaabc'; ",
							"ReadPartition TapPartitionFilter match {a: aaabc, }",
							"ReadPartition TapPartitionFilter leftBoundary a>'aaabc'; rightBoundary a<'aaacd'; ",
							"ReadPartition TapPartitionFilter match {a: aaacd, }",
							"ReadPartition TapPartitionFilter leftBoundary a>'aaacd'; rightBoundary a<'aaaed'; ",
							"ReadPartition TapPartitionFilter match {a: aaaed, }",
							"ReadPartition TapPartitionFilter leftBoundary a>'aaaed'; rightBoundary a<'bbbbb'; ",
							"ReadPartition TapPartitionFilter match {a: bbbbb, }",
							"ReadPartition TapPartitionFilter leftBoundary a>'bbbbb'; rightBoundary a<'bbbbb0'; ",
							"ReadPartition TapPartitionFilter leftBoundary a>='bbbbb0'; rightBoundary a<'bbbbb1!'; ",
							"ReadPartition TapPartitionFilter leftBoundary a>='bbbbb1!'; rightBoundary a<'bbbbb10'; ",
							"ReadPartition TapPartitionFilter leftBoundary a>='bbbbb10'; "
							};
					for(int i = 0; i < strs.length; i++) {
						assertEquals(strs[i], readPartitionList.get(i).toString());
					}
				})
				.queryFieldMinMaxValue(new StringFieldMinMaxHandler(records))
				.countByPartitionFilter(new StringFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}

	@Test
	public void testThreeRecordForBoolean() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
		TapTable table = new TapTable("table").add(new TapField("a", "varchar")).add(new TapIndex().indexField(new TapIndexField().name("a").fieldAsc(true)));

		List<Map<String, Object>> records = new ArrayList<>();
		records.add(map(entry("a", false)));
		records.add(map(entry("a", true)));
		records.add(map(entry("a", true)));

		TestConnector testConnector = new TestConnector();
		List<ReadPartition> readPartitionList = new ArrayList<>();
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(2, readPartitionList.size());
					String[] strs = new String[] {
							"ReadPartition TapPartitionFilter match {a: false, }",
							"ReadPartition TapPartitionFilter match {a: true, }"
					};
					for(int i = 0; i < strs.length; i++) {
						assertEquals(strs[i], readPartitionList.get(i).toString());
					}
				})
				.queryFieldMinMaxValue(new BooleanFieldMinMaxHandler(records))
				.countByPartitionFilter(new BooleanFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();
	}

	@Test
	public void testMultiPrimaryKeysForInt() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
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
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(1L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(14, readPartitionList.size());
					assertEquals("ReadPartition TapPartitionFilter rightBoundary a<1; ", readPartitionList.get(0).toString());
					assertEquals("ReadPartition TapPartitionFilter rightBoundary b<5; match {a: 1, }", readPartitionList.get(1).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 1, b: 5, }", readPartitionList.get(2).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary b>5; match {a: 1, }", readPartitionList.get(3).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>1; rightBoundary a<2; ", readPartitionList.get(4).toString());
					assertEquals("ReadPartition TapPartitionFilter rightBoundary b<51; match {a: 2, }", readPartitionList.get(5).toString());
					assertEquals("ReadPartition TapPartitionFilter match {a: 2, b: 51, }", readPartitionList.get(6).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary b>51; match {a: 2, }", readPartitionList.get(7).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>2; rightBoundary a<3; ", readPartitionList.get(8).toString());
					assertEquals("ReadPartition TapPartitionFilter rightBoundary b<57; match {a: 3, }", readPartitionList.get(9).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary b>=57; rightBoundary b<61; match {a: 3, }", readPartitionList.get(10).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary b>=61; rightBoundary b<449; match {a: 3, }", readPartitionList.get(11).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary b>=449; match {a: 3, }", readPartitionList.get(12).toString());
					assertEquals("ReadPartition TapPartitionFilter leftBoundary a>3; ", readPartitionList.get(13).toString());

				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.startSplitting();

	}

	@Test
	public void testMultiPrimaryKeysForIntMaxRecord2() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
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
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(2L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(14, readPartitionList.size());
					String[] strs = new String[] {
							"ReadPartition TapPartitionFilter rightBoundary a<1; ",
					"ReadPartition TapPartitionFilter rightBoundary b<5; match {a: 1, }",
					"ReadPartition TapPartitionFilter match {a: 1, b: 5, }",
					"ReadPartition TapPartitionFilter leftBoundary b>5; match {a: 1, }",
					"ReadPartition TapPartitionFilter leftBoundary a>1; rightBoundary a<2; ",
					"ReadPartition TapPartitionFilter rightBoundary b<51; match {a: 2, }",
					"ReadPartition TapPartitionFilter match {a: 2, b: 51, }",
					"ReadPartition TapPartitionFilter leftBoundary b>51; match {a: 2, }",
					"ReadPartition TapPartitionFilter leftBoundary a>2; rightBoundary a<3; ",
					"ReadPartition TapPartitionFilter rightBoundary b<57; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=57; rightBoundary b<61; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=61; rightBoundary b<449; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=449; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary a>3; "
					};
					for(int i = 0; i < strs.length; i++) {
						assertEquals(strs[i], readPartitionList.get(i).toString());
					}
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
//				.countIsSlow(true)
				.startSplitting();
	}

	@Test
	public void testMultiPrimaryKeysForIntMaxRecord4() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
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
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(4L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					Assertions.assertEquals(14, readPartitionList.size());
					String[] strs = new String[] {
							"ReadPartition TapPartitionFilter rightBoundary a<1; ",
					"ReadPartition TapPartitionFilter rightBoundary b<5; match {a: 1, }",
					"ReadPartition TapPartitionFilter match {a: 1, b: 5, }",
					"ReadPartition TapPartitionFilter leftBoundary b>5; match {a: 1, }",
					"ReadPartition TapPartitionFilter leftBoundary a>1; rightBoundary a<2; ",
					"ReadPartition TapPartitionFilter rightBoundary b<51; match {a: 2, }",
					"ReadPartition TapPartitionFilter match {a: 2, b: 51, }",
					"ReadPartition TapPartitionFilter leftBoundary b>51; match {a: 2, }",
					"ReadPartition TapPartitionFilter leftBoundary a>2; rightBoundary a<3; ",
					"ReadPartition TapPartitionFilter rightBoundary b<57; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=57; rightBoundary b<61; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=61; rightBoundary b<449; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=449; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary a>3; "
					};
					for(int i = 0; i < strs.length; i++) {
						assertEquals(strs[i], readPartitionList.get(i).toString());
					}
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
//				.countIsSlow(true)
				.startSplitting();

	}

	@Test
	public void testMultiPrimaryKeysForIntMaxRecord4CountIsSlow() throws Throwable {
		TapNodeSpecification nodeSpecification = new TapNodeSpecification();
		nodeSpecification.setId("test");
		nodeSpecification.setGroup("group");
		nodeSpecification.setVersion("1.1");
		TapConnectorContext connectorContext = new TapConnectorContext(nodeSpecification, null, null, new TapLog());
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
		testConnector.calculateDatabaseReadPartitions(connectorContext, table, GetReadPartitionOptions.create().maxRecordInPartition(4L).minMaxSplitPieces(100).consumer(readPartitionList::add))
				.typeSplitterMap(new TypeSplitterMap())
				.splitCompleteListener((id) -> {
					$(() -> Assertions.assertEquals(14, readPartitionList.size()));
					String[] strs = new String[] {
							"ReadPartition TapPartitionFilter rightBoundary a<1; ",
					"ReadPartition TapPartitionFilter rightBoundary b<5; match {a: 1, }",
					"ReadPartition TapPartitionFilter match {a: 1, b: 5, }",
					"ReadPartition TapPartitionFilter leftBoundary b>5; match {a: 1, }",
					"ReadPartition TapPartitionFilter leftBoundary a>1; rightBoundary a<2; ",
					"ReadPartition TapPartitionFilter rightBoundary b<51; match {a: 2, }",
					"ReadPartition TapPartitionFilter match {a: 2, b: 51, }",
					"ReadPartition TapPartitionFilter leftBoundary b>51; match {a: 2, }",
					"ReadPartition TapPartitionFilter leftBoundary a>2; rightBoundary a<3; ",
					"ReadPartition TapPartitionFilter rightBoundary b<57; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=57; rightBoundary b<61; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=61; rightBoundary b<449; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary b>=449; match {a: 3, }",
					"ReadPartition TapPartitionFilter leftBoundary a>3; "
					};
					for(int i = 0; i < strs.length; i++) {
						assertEquals(strs[i], readPartitionList.get(i).toString());
					}
				})
				.queryFieldMinMaxValue(new IntFieldMinMaxHandler(records))
				.countByPartitionFilter(new IntFieldMinMaxHandler(records))
				.maxRecordRatio(2)
				.countIsSlow(true)
				.startSplitting();
	}
}
