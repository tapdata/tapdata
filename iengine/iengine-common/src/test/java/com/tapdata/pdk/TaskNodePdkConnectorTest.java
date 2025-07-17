package com.tapdata.pdk;

import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.exception.FindOneByKeysException;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.utils.UnitTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/14 17:36 Create
 */
class TaskNodePdkConnectorTest {

    @Test
    void testConstruct() throws Exception {
        String taskId = "test-task-id";
        String nodeId = "test-node-id";
        String associateId = nodeId + "-associate";

        DataParentNode node = mock(DataParentNode.class);
        doReturn(nodeId).when(node).getId();

        Connections connections = mock(Connections.class);
        Map<String, Object> connectionConfig = new HashMap<>();
        DatabaseTypeEnum.DatabaseType databaseType = new DatabaseTypeEnum.DatabaseType();
        doReturn(connectionConfig).when(connections).getConfig();

        TaskRetryConfig taskRetryConfig = mock(TaskRetryConfig.class);
        ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
        try (
            MockedStatic<PDKIntegration> integrationMockedStatic = mockStatic(PDKIntegration.class);
            MockedStatic<TaskNodePdkConnector> mockStatic = mockStatic(TaskNodePdkConnector.class, CALLS_REAL_METHODS);
            MockedStatic<PdkUtil> pdkUtilMockedStatic = mockStatic(PdkUtil.class);
        ) {
            integrationMockedStatic.when(() -> PDKIntegration.releaseAssociateId(eq(associateId))).then(invocationOnMock -> null);

            ConnectorFunctions connectorFunctions = mock(ConnectorFunctions.class);
            ConnectorNode connectorNode = mock(ConnectorNode.class);
            doReturn(connectorFunctions).when(connectorNode).getConnectorFunctions();
            doReturn(associateId).when(connectorNode).getAssociateId();

            PdkTableMap pdkTableMap = mock(PdkTableMap.class);
            mockStatic.when(() -> TaskNodePdkConnector.createPdkTableMap(nodeId)).thenReturn(pdkTableMap);
            PdkStateMap pdkStateMap = mock(PdkStateMap.class);
            mockStatic.when(() -> TaskNodePdkConnector.createPdkStateMap(nodeId)).thenReturn(pdkStateMap);
            PdkStateMap globalStateMap = mock(PdkStateMap.class);
            mockStatic.when(TaskNodePdkConnector::createGlobalStateMap).thenReturn(globalStateMap);
            Log log = mock(Log.class);
            mockStatic.when(TaskNodePdkConnector::createLog).thenReturn(log);

            pdkUtilMockedStatic.when(() -> PdkUtil.createNode(
                eq(taskId)
                , any(DatabaseTypeEnum.DatabaseType.class)
                , eq(clientMongoOperator)
                , eq(associateId)
                , eq(connectionConfig)
                , eq(pdkTableMap)
                , eq(pdkStateMap)
                , eq(globalStateMap)
                , eq(log)
            )).thenReturn(connectorNode);

            try (TaskNodePdkConnector pdkConnector = new TaskNodePdkConnector(clientMongoOperator, taskId, node, associateId, connections, databaseType, taskRetryConfig)) {
                assertNotNull(pdkConnector);
                pdkConnector.init();
            }
        }
    }

    @Test
    void testGetTapTable() throws Exception {
        String tableName = "test-table";

        try (TaskNodePdkConnector pdkConnector = mock(TaskNodePdkConnector.class)) {
            TapTable tapTable = mock(TapTable.class);
            KVReadOnlyMap<TapTable> tableMap = mock(KVReadOnlyMap.class);
            doReturn(tapTable).when(tableMap).get(eq(tableName));

            TapConnectorContext connectorContext = mock(TapConnectorContext.class);
            doReturn(tableMap).when(connectorContext).getTableMap();

            ConnectorNode connectorNode = mock(ConnectorNode.class);
            doReturn(connectorContext).when(connectorNode).getConnectorContext();

            UnitTestUtils.injectField(TaskNodePdkConnector.class, pdkConnector, "connectorNode", connectorNode);
            doCallRealMethod().when(pdkConnector).getTapTable(any());

            assertEquals(tapTable, pdkConnector.getTapTable(tableName));
        }
    }

    @Nested
    class EachAllTableTest {

        @Test
        void testEmpty() throws Exception {
            try (TaskNodePdkConnector pdkConnector = mock(TaskNodePdkConnector.class)) {
                Iterator<Entry<TapTable>> iterator = mock(Iterator.class);
                doReturn(false).when(iterator).hasNext();

                KVReadOnlyMap<TapTable> tableMap = mock(KVReadOnlyMap.class);
                doReturn(iterator).when(tableMap).iterator();

                TapConnectorContext connectorContext = mock(TapConnectorContext.class);
                doReturn(tableMap).when(connectorContext).getTableMap();

                ConnectorNode connectorNode = mock(ConnectorNode.class);
                doReturn(connectorContext).when(connectorNode).getConnectorContext();

                UnitTestUtils.injectField(TaskNodePdkConnector.class, pdkConnector, "connectorNode", connectorNode);
                doCallRealMethod().when(pdkConnector).eachAllTable(any());

                pdkConnector.eachAllTable(tapTable -> {
                    throw new RuntimeException("expect can't to call this function");
                });
            }
        }

        @Test
        void testNotEmpty() throws Exception {
            Function<String, Entry<TapTable>> createEntry = s -> new Entry<TapTable>() {
                @Override
                public String getKey() {
                    return s;
                }

                @Override
                public TapTable getValue() {
                    return new TapTable(s);
                }
            };

            try (TaskNodePdkConnector pdkConnector = mock(TaskNodePdkConnector.class)) {
                Iterator<Entry<TapTable>> iterator = mock(Iterator.class);
                doReturn(true, false, true).when(iterator).hasNext();
                doReturn(createEntry.apply("first"), createEntry.apply("skip"), createEntry.apply("last")).when(iterator).next();

                KVReadOnlyMap<TapTable> tableMap = mock(KVReadOnlyMap.class);
                doReturn(iterator).when(tableMap).iterator();

                TapConnectorContext connectorContext = mock(TapConnectorContext.class);
                doReturn(tableMap).when(connectorContext).getTableMap();

                ConnectorNode connectorNode = mock(ConnectorNode.class);
                doReturn(connectorContext).when(connectorNode).getConnectorContext();

                UnitTestUtils.injectField(TaskNodePdkConnector.class, pdkConnector, "connectorNode", connectorNode);
                doCallRealMethod().when(pdkConnector).eachAllTable(any());

                pdkConnector.eachAllTable(tapTable -> {
                    return "skip".equals(tapTable.getId());
                });
            }
        }
    }

    @Nested
    class FindOneByKeysTest {
        String tableName;
        LinkedHashMap<String, Object> keys;
        List<String> fields;

        @BeforeEach
        void setUp() {
            tableName = "test-table";
            keys = new LinkedHashMap<>();
            fields = new ArrayList<>();
        }

        @Test
        void testReturnData() throws Throwable {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            try (TaskNodePdkConnector pdkConnector = mock(TaskNodePdkConnector.class)) {
                doAnswer(answer -> {
                    AtomicReference<LinkedHashMap<String, Object>> ar = answer.getArgument(4);
                    ar.set(result);
                    return null;
                }).when(pdkConnector).consumerResults(eq(fields)
                    , any(TapTable.class)
                    , any()
                    , any()
                    , any()
                );
                assertEquals(result, testQueryByAdvanceFilterFunction(pdkConnector, tableName, keys, fields));
            }
        }

        @Test
        void testThrowable() throws Throwable {
            ExpectException error = new ExpectException();

            try (TaskNodePdkConnector pdkConnector = mock(TaskNodePdkConnector.class)) {
                doAnswer(answer -> {
                    AtomicReference<Throwable> ar = answer.getArgument(3);
                    ar.set(error);
                    return null;
                }).when(pdkConnector).consumerResults(eq(fields)
                    , any(TapTable.class)
                    , any()
                    , any()
                    , any()
                );

                FindOneByKeysException expectedErr = null;
                try {
                    testQueryByAdvanceFilterFunction(pdkConnector, tableName, keys, fields);
                } catch (FindOneByKeysException e) {
                    expectedErr = e;
                }
                assertNotNull(expectedErr);
                assertTrue(expectedErr.getCause() instanceof ExpectException);
            }
        }


        private LinkedHashMap<String, Object> testQueryByAdvanceFilterFunction(TaskNodePdkConnector pdkConnector, String tableName, LinkedHashMap<String, Object> keys, List<String> fields) throws Throwable {
            try (MockedStatic<PDKInvocationMonitor> mc = mockStatic(PDKInvocationMonitor.class)) {
                mc.when(() -> PDKInvocationMonitor.invoke(
                    any(io.tapdata.pdk.core.api.Node.class)
                    , any(PDKMethod.class)
                    , any(PDKMethodInvoker.class)
                )).then(answer -> {
                    PDKMethodInvoker mi = answer.getArgument(2);
                    CommonUtils.AnyError r = mi.getRunnable();
                    r.run();
                    return null;
                });

                QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = mock(QueryByAdvanceFilterFunction.class);
                doAnswer(answer -> {
                    Consumer<FilterResults> c = answer.getArgument(3);
                    c.accept(new FilterResults());
                    return null;
                }).when(queryByAdvanceFilterFunction).query(isNull()
                    , any(TapAdvanceFilter.class)
                    , any(TapTable.class)
                    , any()
                );

                doCallRealMethod().when(pdkConnector).findOneByKeys(any(), eq(keys), eq(fields));
                doReturn(mock(TapAdvanceFilter.class)).when(pdkConnector).createFilter(any(), any());
                doReturn(mock(TapTable.class)).when(pdkConnector).getTapTable(anyString());

                UnitTestUtils.injectField(TaskNodePdkConnector.class, pdkConnector, "connectorNode", mock(ConnectorNode.class));
                UnitTestUtils.injectField(TaskNodePdkConnector.class, pdkConnector, "taskRetryConfig", mock(TaskRetryConfig.class));
                UnitTestUtils.injectField(TaskNodePdkConnector.class, pdkConnector, "queryByAdvanceFilterFunction", queryByAdvanceFilterFunction);

                return pdkConnector.findOneByKeys(tableName, keys, fields);
            }
        }

        class ExpectException extends RuntimeException {

        }
    }

    @Nested
    class CreateFilterTest {
        @Test
        void testEmptyFields() throws Exception {
            LinkedHashMap<String, Object> keys = new LinkedHashMap<>();
            List<String> fields = new ArrayList<>();

            try (TaskNodePdkConnector pdkConnector = mock(TaskNodePdkConnector.class)) {
                doCallRealMethod().when(pdkConnector).createFilter(any(), any());

                TapAdvanceFilter filter = pdkConnector.createFilter(keys, fields);
                assertNotNull(filter);
                assertNotNull(filter.getMatch());
                assertNotNull(filter.getSortOnList());
                assertEquals(1, filter.getLimit());
                assertNull(filter.getProjection());
            }
        }

        @Test
        void testNotEmptyFields() throws Exception {
            LinkedHashMap<String, Object> keys = new LinkedHashMap<>();
            List<String> fields = Arrays.asList("id", "name");

            try (TaskNodePdkConnector pdkConnector = mock(TaskNodePdkConnector.class)) {
                doCallRealMethod().when(pdkConnector).createFilter(any(), any());

                TapAdvanceFilter filter = pdkConnector.createFilter(keys, fields);
                assertNotNull(filter);
                assertNotNull(filter.getMatch());
                assertNotNull(filter.getSortOnList());
                assertEquals(1, filter.getLimit());
                assertNotNull(filter.getProjection());
                assertNotNull(filter.getProjection().getIncludeFields());
                assertEquals(2, filter.getProjection().getIncludeFields().size());
            }
        }
    }

    @Nested
    class ConsumerResultsTest {
        @Mock
        private TapTable tapTable;
        @Mock
        private FilterResults filterResults;
        @Mock
        private TapCodecsFilterManager codecsFilterManager;
        @Mock
        private TapCodecsFilterManager defaultCodecsFilterManager;
        @Mock
        private TaskNodePdkConnector taskNodePdkConnector;

        @BeforeEach
        void setUp() {
            MockitoAnnotations.openMocks(this);

            doReturn(codecsFilterManager).when(taskNodePdkConnector).getCodecsFilterManager();
            doReturn(defaultCodecsFilterManager).when(taskNodePdkConnector).getDefaultCodecsFilterManager();
            doCallRealMethod().when(taskNodePdkConnector).consumerResults(any(), any(TapTable.class), any(), any(), any());
        }

        @Test
        void testConsumerResults_NormalWithFields() {
            // Arrange
            List<String> fields = Arrays.asList("field1", "field2");
            Map<String, Object> result = new HashMap<>();
            result.put("field1", "value1");
            result.put("field2", "value2");
            result.put("field3", "value3");

            List<Map<String, Object>> results = Collections.singletonList(result);
            when(filterResults.getError()).thenReturn(null);
            when(filterResults.getResults()).thenReturn(results);

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<LinkedHashMap<String, Object>> data = new AtomicReference<>();

            // Act
            taskNodePdkConnector.consumerResults(fields, tapTable, filterResults, throwable, data);

            // Assert
            assertNull(throwable.get());
            assertNotNull(data.get());
            assertEquals(2, data.get().size());
            assertEquals("value1", data.get().get("field1"));
            assertEquals("value2", data.get().get("field2"));
        }

        @Test
        void testConsumerResults_NormalWithoutFields() {
            // Arrange
            List<String> fields = Collections.emptyList();
            Map<String, Object> result = new HashMap<>();
            result.put("field1", "value1");
            result.put("field2", "value2");
            result.put("field3", "value3");

            List<Map<String, Object>> results = Collections.singletonList(result);
            when(filterResults.getError()).thenReturn(null);
            when(filterResults.getResults()).thenReturn(results);

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<LinkedHashMap<String, Object>> data = new AtomicReference<>();

            // Act
            taskNodePdkConnector.consumerResults(fields, tapTable, filterResults, throwable, data);

            // Assert
            assertNull(throwable.get());
            assertNotNull(data.get());
            assertEquals(3, data.get().size());
            assertEquals("value1", data.get().get("field1"));
            assertEquals("value2", data.get().get("field2"));
            assertEquals("value3", data.get().get("field3"));
        }

        @Test
        void testConsumerResults_WithException() {
            // Arrange
            List<String> fields = Arrays.asList("field1", "field2");
            Throwable exception = new RuntimeException("Test Exception");
            when(filterResults.getError()).thenReturn(exception);

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<LinkedHashMap<String, Object>> data = new AtomicReference<>();

            // Act
            taskNodePdkConnector.consumerResults(fields, tapTable, filterResults, throwable, data);

            // Assert
            assertNotNull(throwable.get());
            assertEquals(exception, throwable.get());
            assertNull(data.get());
        }

        @Test
        void testConsumerResults_EmptyResults() {
            // Arrange
            List<String> fields = Arrays.asList("field1", "field2");
            List<Map<String, Object>> results = Collections.emptyList();
            when(filterResults.getError()).thenReturn(null);
            when(filterResults.getResults()).thenReturn(results);

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<LinkedHashMap<String, Object>> data = new AtomicReference<>();

            // Act
            taskNodePdkConnector.consumerResults(fields, tapTable, filterResults, throwable, data);

            // Assert
            assertNull(throwable.get());
            assertNull(data.get());
        }

        @Test
        void testConsumerResults_NullResults() {
            // Arrange
            List<String> fields = Arrays.asList("field1", "field2");
            when(filterResults.getError()).thenReturn(null);
            when(filterResults.getResults()).thenReturn(null);

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            AtomicReference<LinkedHashMap<String, Object>> data = new AtomicReference<>();

            // Act
            taskNodePdkConnector.consumerResults(fields, tapTable, filterResults, throwable, data);

            // Assert
            assertNull(throwable.get());
            assertNull(data.get());
        }
    }
}
