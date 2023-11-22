package inspect;

import ConnectorNode.ConnectorNodeBase;
import MockConnector.*;
import com.tapdata.entity.Connections;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.inspect.compare.PdkResult;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.error.TapPdkRunnerUnknownException;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class CustomSqlInspect extends ConnectorNodeBase {

    public final String querySql = "select *  from  test  where id>2";

    public void initConnectorNode(ExecuteCommandFunction ExecuteCommandFunction) throws IllegalAccessException, NoSuchFieldException {
        ConnectorFunctions connectorFunction = new ConnectorFunctions();
        connectorFunction.supportExecuteCommandFunction(ExecuteCommandFunction);
        invokeValueForFiled(ConnectorNode.class,"connectorFunctions",sqlConnectorNode,connectorFunction,false);


        TapNodeSpecification specification = sqlConnectorNode.getTapNodeInfo().getTapNodeSpecification();
        TapConnectorContext connectorContext = new TapConnectorContext(specification,
                new DataMap(), new DataMap(), new TapLog());
        TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("test", myTapTable);
        PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
        connectorContext.setTableMap(pdkTableMap);
        invokeValueForFiled(ConnectorNode.class,"connectorContext",sqlConnectorNode,connectorContext,false);

    }


    /**
     * 检查关系型数据库count返回
     * select *  from  test  where id>2
     * test executeCommand function
     */
    @Test
    public void testExecuteCommandQueryCountNormal() throws NoSuchFieldException, IllegalAccessException {
        // input param
        TapExecuteCommand tapExecuteCommand =
                CommandCountParamTest.setCustomCommandParam(new LinkedHashMap<>(), querySql, "", "", "");

        // input query data
        MockExecuteCommandFunction executeCommandFunction = new MockCountNormalExecuteCommandFunction();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("COUNT(1)", 100);
        List<Map<String, Object>> listData = new ArrayList<>();
        listData.add(map);
        handleMockExecuteCommandFunction(executeCommandFunction, listData, null, false);


        // execution method
        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(executeCommandFunction, tapExecuteCommand, sqlConnectorNode);

        // actual data
        long actualData = list.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();

        // expected data
        long expectedData = 100;

        // output results
        Assert.assertEquals(expectedData, actualData);
    }


    /**
     * 查询count返回值为instanceof 为long mongodb会返回long
     * select *  from  test  where id>2
     * test executeCommand function
     */
    @Test
    public void testExecuteCommandQueryCountLongQuery() throws NoSuchFieldException, IllegalAccessException {
        // input param
        TapExecuteCommand tapExecuteCommand =
                CommandCountParamTest.setCustomCommandParam(new LinkedHashMap<>(), querySql, "", "", "");

        // input query data
        MockExecuteCommandFunction executeCommandFunction = new MockCountNormalLongExecuteCommandFunction();
        handleMockExecuteCommandFunction(executeCommandFunction, 100L, null, false);

        // execution method
        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(executeCommandFunction, tapExecuteCommand, sqlConnectorNode);
        // actual data
        long actualData = list.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();

        // expected data
        long expectedData = 100;

        // output results
        Assert.assertEquals(expectedData, actualData);
    }

    /**
     * 查询count返回值为null
     * select *  from  test  where id>2
     * test executeCommand function
     */
    @Test
    public void testExecuteCommandQueryCountResultNull() throws NoSuchFieldException, IllegalAccessException {
        // input param
        TapExecuteCommand tapExecuteCommand =
                CommandCountParamTest.setCustomCommandParam(new LinkedHashMap<>(), querySql, "", "", "");


        // input query data
        MockExecuteCommandFunction executeCommandFunction = new MockCountResultNullExecuteCommandFunction();
        handleMockExecuteCommandFunction(executeCommandFunction, null, null, false);

        // execution method
        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(executeCommandFunction, tapExecuteCommand, sqlConnectorNode);

        // output results
        Assert.assertTrue(CollectionUtils.isEmpty(list));
    }

    /**
     * 查询count抛异常
     * select *  from  test  where id>2
     * test executeCommand function
     */
    @Test
    public void testExecuteCommandQueryCountException() throws NoSuchFieldException, IllegalAccessException {
        // input param
        TapExecuteCommand tapExecuteCommand =
                CommandCountParamTest.setCustomCommandParam(new LinkedHashMap<>(), querySql, "", "", "");

        // input query data
        MockExecuteCommandFunction executeCommandFunction = new MockExceptionExecuteCommandFunction();
        SQLException sqlException = new SQLSyntaxErrorException("Table 'mydb.test' doesn't exist",
                "42S02", 1146);
        handleMockExecuteCommandFunction(new MockExceptionExecuteCommandFunction(), sqlException, null, false);

        // execution method
        try {
            // execution method
            TableRowCountInspectJob.executeCommand(executeCommandFunction, tapExecuteCommand, sqlConnectorNode);
        }catch (Exception e){
            Assert.assertTrue(e instanceof TapPdkRunnerUnknownException);
        }
    }


    /**
     * 查询llist数据集合正常返回
     * select *  from  test  where id>2
     * test executeQueryCommand function
     */
    @Test
    public void testExecuteQueryCommandQueryListNormal() throws NoSuchFieldException, IllegalAccessException {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        TapExecuteCommand tapExecuteCommand =
                CommandCountParamTest.setCustomCommandParam(customCommand, querySql, "", "", "");

        // input query data
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", "100");
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(map);
        PdkResult pdkResult = handleMockExecuteCommandFunction(new MockListNormalExecuteCommandFunction(), list, customCommand, true);

        // execution method
        pdkResult.executeQueryCommand(tapExecuteCommand);

        // actual data
        LinkedBlockingQueue<Map<String, Object>> linkedBlockingQueue = pdkResult.getQueue();
        Map<String, Object> actualData = linkedBlockingQueue.poll();

        // expected data
        String expectedData = "100";

        // output results
        assert actualData != null;
        Assert.assertEquals(expectedData, actualData.get("id"));

    }


    /**
     * 查询llist数据集合返回为空
     * select *  from  test  where id>2
     * test executeQueryCommand function
     */
    @Test
    public void testExecuteQueryCommandQueryListEmpty() throws NoSuchFieldException, IllegalAccessException {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        TapExecuteCommand tapExecuteCommand =
                CommandCountParamTest.setCustomCommandParam(customCommand, querySql, "", "", "");

        // input query data
        PdkResult pdkResult = handleMockExecuteCommandFunction(new MockListNormalExecuteCommandFunction(), new ArrayList<>(), customCommand, true);


        // execution method
        pdkResult.executeQueryCommand(tapExecuteCommand);

        // actual data
        LinkedBlockingQueue<Map<String, Object>> ActualLinkedBlockingQueue = pdkResult.getQueue();

        // output results
        Assert.assertTrue(ActualLinkedBlockingQueue.isEmpty());
    }


    /**
     * 查询list数据集合返回异常
     * select *  from  test  where id>2
     * test executeQueryCommand function
     */
    @Test
    public void testExecuteQueryCommandQueryListException() throws NoSuchFieldException, IllegalAccessException {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        TapExecuteCommand tapExecuteCommand =
                CommandCountParamTest.setCustomCommandParam(customCommand, querySql, "", "", "");

        // input query data
        SQLException sqlException = new SQLSyntaxErrorException("Table 'mydb.test1' doesn't exist",
                "42S02", 1146);
        PdkResult pdkResult = handleMockExecuteCommandFunction(new MockExceptionExecuteCommandFunction(), sqlException, customCommand, true);

        try {
            // execution method
            pdkResult.executeQueryCommand(tapExecuteCommand);
        }catch (Exception e){
            Assert.assertTrue(e instanceof TapPdkRunnerUnknownException);
        }
    }


    public PdkResult handleMockExecuteCommandFunction(MockExecuteCommandFunction executeCommandFunction, Object data, Map<String, Object> customCommand, boolean init) throws NoSuchFieldException, IllegalAccessException {
        executeCommandFunction.setData(data);
        initConnectorNode(executeCommandFunction);
        PdkResult pdkResult = null;
        if (init) {
            pdkResult = new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                    new HashSet<>(), sqlConnectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                    true, customCommand);
        }
        return pdkResult;
    }


}
