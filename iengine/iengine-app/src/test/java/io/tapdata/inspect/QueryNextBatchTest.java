package io.tapdata.inspect;

import cn.hutool.core.map.MapUtil;
import com.tapdata.entity.Connections;
import io.tapdata.ConnectorNode.ConnectorNodeBase;
import io.tapdata.MockConnector.MockAdvanceFilterQueryFunction;
import io.tapdata.MockConnector.MockCountNormalExecuteCommandFunction;
import io.tapdata.MockConnector.MockExecuteCommandFunction;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.error.TaskInspectExCode_27;
import io.tapdata.exception.TapCodeException;
import io.tapdata.inspect.compare.PdkResult;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.CountByPartitionFilterFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryNextBatchTest extends ConnectorNodeBase {

    private List<SortOn> sortOnList = new LinkedList<>();

    private static Projection projection = null;

    private MockExecuteCommandFunction executeCommandFunction;

    @Before
    public void initParam(){
        TapTable table = new TapTable();
        table.setId("testID");
        table.setName("testID");
        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
        TapField tapField = new TapField();
        nameFieldMap.put("testID",tapField);
        table.setNameFieldMap(nameFieldMap);
        myTapTable = table;
    }

    public void init(QueryByAdvanceFilterFunction queryByAdvanceFilterFunction) throws Throwable {
        sortOnList.add(SortOn.ascending("id"));
        sqlConnectorNode = new ConnectorNode();
        TapNodeSpecification tapNodeSpecificationSql = new TapNodeSpecification();
        tapNodeSpecificationSql.setId("mysql");
        initConnectorNode(sqlConnectorNode, tapNodeSpecificationSql);




        ConnectorFunctions connectorFunction = new ConnectorFunctions();
        connectorFunction.supportQueryByAdvanceFilter(queryByAdvanceFilterFunction);
        invokeValueForFiled(ConnectorNode.class,"connectorFunctions",sqlConnectorNode,connectorFunction,false);

        BatchCountFunction batchCountFunction = mock(BatchCountFunction.class);
        ReflectionTestUtils.setField(connectorFunction,"batchCountFunction",batchCountFunction);

        this.executeCommandFunction = new MockCountNormalExecuteCommandFunction();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("COUNT(1)", 100);
        List<Map<String, Object>> listData = new ArrayList<>();
        listData.add(map);
        executeCommandFunction.setData(listData);
        ReflectionTestUtils.setField(connectorFunction,"executeCommandFunction",executeCommandFunction);
        TapNodeSpecification specification = sqlConnectorNode.getTapNodeInfo().getTapNodeSpecification();
        TapConnectorContext connectorContext = new TapConnectorContext(specification,
                new DataMap(), new DataMap(), new TapLog());
        when(batchCountFunction.count(connectorContext,myTapTable)).thenReturn(0l);

        TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("test", myTapTable);
        PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
        connectorContext.setTableMap(pdkTableMap);
        invokeValueForFiled(ConnectorNode.class,"connectorContext",sqlConnectorNode,connectorContext,false);


    }
    @Test
    public void testQueryNextBatchHasMatch() throws Throwable {
        System.out.println(System.currentTimeMillis());
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        List<List<Object>> diffKeyValues = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        values.add("test");
        diffKeyValues.add(values);
        List<String> dataKey = new ArrayList<>();
        dataKey.add("name");
        TapField tapField = new TapField();
        tapField.setName("name");
        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
        nameFieldMap.put("name",tapField);
        myTapTable.setNameFieldMap(nameFieldMap);
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, diffKeyValues, dataKey,
                null, true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        // execution method
        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        await().atMost(15, TimeUnit.SECONDS).until(() -> mockAdvanceFilterQueryFunction.isFlag());


        DataMap actualData  = mockAdvanceFilterQueryFunction.getTapAdvanceFilter().getMatch();

        // output results
        Assert.assertTrue(MapUtil.isNotEmpty(actualData));
        System.out.println(System.currentTimeMillis());


    }


    @Test
    public void testQueryNextBatchNoMatch() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(),
                null, true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        // execution method
        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        await().atMost(15, TimeUnit.SECONDS).until(() -> mockAdvanceFilterQueryFunction.isFlag());


        DataMap actualData  = mockAdvanceFilterQueryFunction.getTapAdvanceFilter().getMatch();

        // output results
        Assert.assertTrue(MapUtil.isEmpty(actualData));



    }


    public PdkResult handleMockAdvanceFilterFunction(QueryByAdvanceFilterFunction executeCommandFunction,
                                                     List<List<Object>> diffKeyValues, List<String> dataKey
                                                    , Map<String, Object> customCommand, boolean fullMatch,List<String> sortColumns,
                                                     Set<String> columns,List<QueryOperator> conditions) throws Throwable {
        init(executeCommandFunction);

        return new PdkResult(sortColumns, new Connections(), myTapTable.getId(),
                columns, sqlConnectorNode, fullMatch, dataKey, diffKeyValues, conditions,
                true, customCommand);
    }

    @Test
    public void testQueryNextBatchForHasDiff() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        List<List<Object>> diffKeyValues = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        objects.add("testID");
        diffKeyValues.add(objects);
        List<String> dataKey = new ArrayList<>();
        dataKey.add("testID");
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test","select * from test");
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, diffKeyValues, dataKey, customCommand,
                true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        // execution method
        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        await().atMost(30, TimeUnit.SECONDS).until(() -> mockAdvanceFilterQueryFunction.isFlag());


        boolean actualData  = mockAdvanceFilterQueryFunction.isFlag();

        // output results
        Assert.assertTrue(actualData);

    }

    @Test
    public void testQueryNextBatchForNoDiff() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test","select * from test");
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("params", customParam);
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(),
                customCommand, true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        // execution method
        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        await().atMost(15, TimeUnit.SECONDS).until(() -> executeCommandFunction.isFlag());


        boolean actualData  = mockAdvanceFilterQueryFunction.isFlag();

        // output results
        Assert.assertFalse(actualData);

    }


    @Test
    public void testJudgeExistFunctionReturnTrue() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test","select * from test");
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(),
                new ArrayList<>(), customCommand, true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        // execution method
        Boolean actualData = ReflectionTestUtils.invokeMethod(pdkResult, "judgeExistFunction", null, null, null);

        // output results
        Assert.assertTrue(actualData);

    }

    @Test
    public void testJudgeExistCountByPartitionFilterFunctionReturnTrue() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test","select * from test");
        List<QueryOperator> conditions = new ArrayList<>();
        QueryOperator queryOperator = new QueryOperator();
        queryOperator.setValue("test");
        conditions.add(queryOperator);
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(),
                new ArrayList<>(), customCommand, true, new ArrayList<>(), new HashSet<>(), conditions);

        // execution method
        Boolean actualData = ReflectionTestUtils.invokeMethod(pdkResult, "judgeExistFunction", mock(BatchCountFunction.class), null, null);

        // output results
        Assert.assertTrue(actualData);

    }

    @Test
    public void testJudgeExistExecuteCommandFunctionReturnTrue() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test","select * from test");
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(),
                new ArrayList<>(), customCommand, true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        // execution method
        Boolean actualData = ReflectionTestUtils.invokeMethod(pdkResult, "judgeExistFunction", mock(BatchCountFunction.class), null, null);

        // output results
        Assert.assertTrue(actualData);

    }

    @Test
    public void testJudgeExistFunctionReturnFalse() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test","select * from test");
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(),
                customCommand, true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        // execution method
        Boolean actualData = ReflectionTestUtils.invokeMethod(pdkResult, "judgeExistFunction", mock(BatchCountFunction.class),
                mock(CountByPartitionFilterFunction.class), mock(ExecuteCommandFunction.class));

        // output results
        Assert.assertFalse(actualData);

    }

    @Test
    public void testAssignProjectionReturnNull() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test","select * from test");
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(), customCommand,
                true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        // execution method
        Object actualData = ReflectionTestUtils.invokeMethod(pdkResult, "assignProjection", true, null, null);

        // output results
        Assert.assertEquals(null,actualData);

    }

    @Test
    public void testAssignProjectionSortColumnReturnObject() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test", "select * from test");
        List<String> sortColumns = new ArrayList<>();
        sortColumns.add("id");

        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(), customCommand,
                false, sortColumns, new HashSet<>(), new ArrayList<>());
        Object actualData = ReflectionTestUtils.getField(pdkResult, "projection");
        // output results
        Assert.assertNotNull(actualData);

    }


    @Test
    public void testAssignProjectionColumnsReturnObject() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test", "select * from test");
        Set<String> columns = new HashSet<>();
        columns.add("id");

        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(), customCommand,
                true, new ArrayList<>(), columns, new ArrayList<>());
        Object actualData = ReflectionTestUtils.getField(pdkResult, "projection");
        // output results
        Assert.assertNotNull(actualData);

    }

    @Test
    public void testHandleDataKeys() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test", "select * from test");
        List<List<Object>> diffKeyValues = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        String objectId = "64ba64a79a74c5816a0fbf95";
        objects.add(objectId);
        diffKeyValues.add(objects);
        List<String> dataKey = new ArrayList<>();
        dataKey.add("_id");

        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap();
        TapField tapField = new TapField();
        tapField.setDataType("OBJECT_ID");
        nameFieldMap.put("_id", tapField);
        myTapTable.setNameFieldMap(nameFieldMap);

        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, diffKeyValues, dataKey, customCommand,
                true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());
        List actualData = (List) ReflectionTestUtils.getField(pdkResult, "diffKeyValues");
        // output results
        Assert.assertEquals(objectId, ((ArrayList) actualData.get(0)).get(0).toString());

    }

    @Test
    public void testHandleDataKeyException() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("test", "select * from test");
        List<List<Object>> diffKeyValues = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        String objectId = "64ba64a";
        objects.add(objectId);
        diffKeyValues.add(objects);
        List<String> dataKey = new ArrayList<>();
        dataKey.add("_id");

        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap();
        TapField tapField = new TapField();
        tapField.setDataType("OBJECT_ID");
        nameFieldMap.put("_id", tapField);
        myTapTable.setNameFieldMap(nameFieldMap);

        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, diffKeyValues, dataKey, customCommand,
                true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());
        List actualData = (List) ReflectionTestUtils.getField(pdkResult, "diffKeyValues");
        // output results
        Assert.assertEquals(objectId, ((ArrayList) actualData.get(0)).get(0).toString());

    }


    @Test
    public void testCustomCommandCount() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("command", "aggregate");
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("params", customParam);

        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, null, new ArrayList<>(), customCommand,
                true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());

        long actualData = (long) ReflectionTestUtils.getField(pdkResult, "total");

        // output results
        Assert.assertEquals(100,actualData);
    }


    @Test
    public void testQueryNextBatch() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("command", "aggregate");
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("params", customParam);
        List<QueryOperator> conditions = new ArrayList<>();
        QueryOperator queryOperator = new QueryOperator();
        queryOperator.setValue("test");
        queryOperator.setKey("test");
        conditions.add(queryOperator);
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(),customCommand,
                true, new ArrayList<>(), new HashSet<>(), conditions);

        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        await().atMost(15, TimeUnit.SECONDS).until(() -> executeCommandFunction.isFlag());


        // output results
        long actualData = (long) ReflectionTestUtils.getField(pdkResult, "total");

        // output results
        Assert.assertEquals(0,actualData);
    }


    @Test
    public void testExecuteCommandFunctionException() {
        // init param
        Map<String, Object> customCommand = new HashMap<>();
        customCommand.put("command", "aggregate");
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("params", customParam);
        ConnectorNode connectorNode =  new ConnectorNode();
        ReflectionTestUtils.setField(connectorNode,"connectorFunctions",new ConnectorFunctions());
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("test");
        TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
        ReflectionTestUtils.setField(connectorNode, "connectorContext", tapConnectorContext);

        try {
            new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                    new HashSet<>(), connectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                    true, customCommand);
        }catch (TapCodeException tapCodeException){
            Assertions.assertEquals(TaskInspectExCode_27.CONNECTOR_NOT_SUPPORT_FUNCTION,tapCodeException.getCode());
        }
    }

    @Test
    public void testQueryByAdvanceFilterFunctionException() {
        // init param
        ConnectorNode connectorNode =  new ConnectorNode();
        ReflectionTestUtils.setField(connectorNode,"connectorFunctions",new ConnectorFunctions());
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("test");
        TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
        ReflectionTestUtils.setField(connectorNode, "connectorContext", tapConnectorContext);
        try {
            new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                    new HashSet<>(), connectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                    false, new HashMap<>());
        }catch (TapCodeException tapCodeException){
            Assertions.assertEquals(TaskInspectExCode_27.CONNECTOR_NOT_SUPPORT_FUNCTION,tapCodeException.getCode());
        }
    }

    @Test
    public void testTapTableNullException() {
        // init param
        ConnectorNode connectorNode =  new ConnectorNode();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        connectorFunctions.supportQueryByAdvanceFilter(Mockito.mock(QueryByAdvanceFilterFunction.class));
        ReflectionTestUtils.setField(connectorNode,"connectorFunctions",connectorFunctions);
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("test");
        TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
        ReflectionTestUtils.setField(connectorNode, "connectorContext", tapConnectorContext);
        KVReadOnlyMap<TapTable> tableMap = Mockito.mock(KVReadOnlyMap.class);
        ReflectionTestUtils.setField(tapConnectorContext, "tableMap", tableMap);

        try {
            new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                    new HashSet<>(), connectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                    false, new HashMap<>());
        }catch (TapCodeException tapCodeException){
            Assertions.assertEquals(TaskInspectExCode_27.TABLE_NO_EXISTS,tapCodeException.getCode());
        }
    }

    @Test
    public void testDataKeysSizeNotEqualException() {
        // init param
        ConnectorNode connectorNode =  new ConnectorNode();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        connectorFunctions.supportQueryByAdvanceFilter(Mockito.mock(QueryByAdvanceFilterFunction.class));
        ReflectionTestUtils.setField(connectorNode,"connectorFunctions",connectorFunctions);
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("test");
        TapConnectorContext tapConnectorContext = new TapConnectorContext(tapNodeSpecification, new DataMap(), new DataMap(), Mockito.mock(Log.class));
        ReflectionTestUtils.setField(connectorNode, "connectorContext", tapConnectorContext);
        KVReadOnlyMap<TapTable> tableMap = Mockito.mock(KVReadOnlyMap.class);
        when(tableMap.get("testID")).thenReturn(Mockito.mock(TapTable.class));
        ReflectionTestUtils.setField(tapConnectorContext, "tableMap", tableMap);
        List<String> dataKeys = new ArrayList<>();
        dataKeys.add("name");
        dataKeys.add("year");
        List<List<Object>> diffKeyValues = new ArrayList<>();
        List<Object> objects = new ArrayList<>();
        objects.add("test");
        diffKeyValues.add(objects);
        try {
            new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                    new HashSet<>(), connectorNode, true, dataKeys, diffKeyValues, new ArrayList<>(),
                    false, new HashMap<>());
        }catch (TapCodeException tapCodeException){
            Assertions.assertEquals(TaskInspectExCode_27.PARAM_ERROR,tapCodeException.getCode());
        }

    }

    @Test
    public void testTapAdvanceFilterReturnThrowable() throws Throwable {

        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
        MockAdvanceFilterQueryFunction advanceFilterFunction = new MockAdvanceFilterQueryFunction();
        advanceFilterFunction.setData(new SQLSyntaxErrorException("Table 'mydb.test1' doesn't exist",
                "42S02", 1146));
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(), new HashMap<>(),
                true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());
        ReflectionTestUtils.setField(pdkResult, "queryByAdvanceFilterFunction", advanceFilterFunction);
        pdkResult.tapAdvanceFilter(tapAdvanceFilter);
        Throwable actualData = (Throwable) ReflectionTestUtils.getField(pdkResult, "throwable");
        Assertions.assertEquals("42S02",((SQLSyntaxErrorException)actualData).getSQLState());
    }

    @Test
    public void testTapAdvanceFilterReturnEmpty() throws Throwable {

        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
        MockAdvanceFilterQueryFunction advanceFilterFunction = new MockAdvanceFilterQueryFunction();
        advanceFilterFunction.setData(new ArrayList<>());
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(), new HashMap<>(),
                true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());
        ReflectionTestUtils.setField(pdkResult, "queryByAdvanceFilterFunction", advanceFilterFunction);
        pdkResult.tapAdvanceFilter(tapAdvanceFilter);
        LinkedBlockingQueue<Map<String, Object>> actualData = (LinkedBlockingQueue<Map<String, Object>>) ReflectionTestUtils.getField(pdkResult, "queue");
        Assertions.assertTrue(CollectionUtils.isEmpty(actualData));
    }

    @Test
    public void testTapAdvanceFilterReturnObject() throws Throwable {

        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
        MockAdvanceFilterQueryFunction advanceFilterFunction = new MockAdvanceFilterQueryFunction();
        List list = new ArrayList();
        Map<String, Object> map = new HashMap<>();
        map.put("test", "test");
        list.add(map);
        advanceFilterFunction.setData(list);
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction, new ArrayList<>(), new ArrayList<>(), new HashMap<>(),
                true, new ArrayList<>(), new HashSet<>(), new ArrayList<>());
        ReflectionTestUtils.setField(pdkResult, "queryByAdvanceFilterFunction", advanceFilterFunction);
        pdkResult.tapAdvanceFilter(tapAdvanceFilter);
        LinkedBlockingQueue<Map<String, Object>> actualData = (LinkedBlockingQueue<Map<String, Object>>) ReflectionTestUtils.getField(pdkResult, "queue");
        Assertions.assertTrue(actualData.peek().containsKey("test"));
    }
}
