package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.schema.TapTableMap;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/11/20 14:34 Create
 */
public class HazelcastSourcePdkBaseNodeTest {

	@Test
	public void testDoAsyncTableCount_UnSupportBatchCountFunction() {
		BatchCountFunction batchCountFunction = null;
		String testTableName = "testTable";

		MockHazelcastSourcePdkBaseNode mockPdkNode = Mockito.mock(MockHazelcastSourcePdkBaseNode.class);
		Mockito.when(mockPdkNode.doAsyncTableCount(null, testTableName)).thenCallRealMethod();

		try (AutoCloseable autoCloseable = mockPdkNode.doAsyncTableCount(batchCountFunction, testTableName)) {
		} catch (Exception e) {
			Assert.fail("failed: " + e.getMessage());
			return;
		}
	}

	@Test
	public void testDoAsyncTableCount_ExitWithNotRunning() {
		BatchCountFunction batchCountFunction = Mockito.mock(BatchCountFunction.class);
		String testTableName = "testTable";

		MockHazelcastSourcePdkBaseNode mockPdkNode = Mockito.mock(MockHazelcastSourcePdkBaseNode.class);
		Mockito.when(mockPdkNode.doAsyncTableCount(batchCountFunction, testTableName)).thenCallRealMethod();
		Mockito.when(mockPdkNode.isRunning()).thenReturn(false);

		try (AutoCloseable autoCloseable = mockPdkNode.doAsyncTableCount(batchCountFunction, testTableName)) {
		} catch (Exception e) {
			Assert.fail("failed: " + e.getMessage());
			return;
		}
	}

	@Test
	public void testDoTableCount_Exception() {
		BatchCountFunction batchCountFunction = Mockito.mock(BatchCountFunction.class);
		String testTableName = "testTable";

		MockHazelcastSourcePdkBaseNode mockPdkNode = Mockito.mock(MockHazelcastSourcePdkBaseNode.class);
		Mockito.when(mockPdkNode.doAsyncTableCount(batchCountFunction, testTableName)).thenCallRealMethod();
		Mockito.when(mockPdkNode.isRunning()).thenReturn(true);
		Mockito.when(mockPdkNode.getDataProcessorContext()).thenThrow(new RuntimeException("Exec exception test"));

		Exception error = null;
		// test thread exception if running
		try (AutoCloseable autoCloseable = mockPdkNode.doAsyncTableCount(batchCountFunction, testTableName)) {
		} catch (Exception e) {
			error = e;
		}
		Assert.assertNotNull(error);

		// test thread exception if not running
		try (AutoCloseable autoCloseable = mockPdkNode.doAsyncTableCount(batchCountFunction, testTableName)) {
			Mockito.when(mockPdkNode.isRunning()).thenReturn(false);
		} catch (Exception e) {
			error = e;
		}
		Assert.assertNotNull(error);
	}

	@Test
	public void testDoAsyncTableCount_SetSnapshotRowSizeMap() {
		long tableSize = 1000L; // Expected result

		// mock definition
		TapTable testTable = new TapTable("testTable");
		TapTable testSnapshotRowSizeMapNotNullTable = new TapTable("testTable2");
		ObsLogger mockObsLogger = Mockito.mock(ObsLogger.class);
		BatchCountFunction batchCountFunction = Mockito.mock(BatchCountFunction.class);
		DataProcessorContext mockDataProcessorContext = Mockito.mock(DataProcessorContext.class);

		TaskDto mockTask = Mockito.mock(TaskDto.class);
		Mockito.when(mockTask.getId()).thenReturn(new ObjectId());
		Mockito.when(mockDataProcessorContext.getTaskDto()).thenReturn(mockTask);

		Node mockNode = Mockito.mock(Node.class);
		Mockito.when(mockDataProcessorContext.getNode()).thenReturn(mockNode);

		TapTableMap mockTapTableMap = Mockito.mock(TapTableMap.class);
		Mockito.when(mockTapTableMap.get(testTable.getName())).thenReturn(testTable);
		Mockito.when(mockTapTableMap.get(testSnapshotRowSizeMapNotNullTable.getName())).thenReturn(testSnapshotRowSizeMapNotNullTable);
		Mockito.when(mockDataProcessorContext.getTapTableMap()).thenReturn(mockTapTableMap);

		MockHazelcastSourcePdkBaseNode mockPdkNode = Mockito.mock(MockHazelcastSourcePdkBaseNode.class);
		Mockito.when(mockPdkNode.doAsyncTableCount(batchCountFunction, testTable.getName())).thenCallRealMethod();
		Mockito.when(mockPdkNode.doBatchCountFunction(batchCountFunction, testTable)).thenReturn(tableSize);
		Mockito.when(mockPdkNode.getObsLogger()).thenReturn(mockObsLogger);
		Mockito.when(mockPdkNode.isRunning()).thenReturn(true);
		Mockito.when(mockPdkNode.getDataProcessorContext()).thenReturn(mockDataProcessorContext);

		// test SnapshotRowSizeMap is null
		try (AutoCloseable autoCloseable = mockPdkNode.doAsyncTableCount(batchCountFunction, testTable.getName())) {
		} catch (Exception e) {
			Assert.fail("failed: " + e.getMessage());
			return;
		}
		Long countResult = mockPdkNode.snapshotRowSizeMap.get(testTable.getName());
		Assert.assertEquals(tableSize, (long) countResult);

		// test SnapshotRowSizeMap not null
		try (AutoCloseable autoCloseable = mockPdkNode.doAsyncTableCount(batchCountFunction, testSnapshotRowSizeMapNotNullTable.getName())) {
		} catch (Exception e) {
			Assert.fail("failed: " + e.getMessage());
			return;
		}
		 countResult = mockPdkNode.snapshotRowSizeMap.get(testTable.getName());
		Assert.assertEquals(tableSize, (long) countResult);
	}
}
