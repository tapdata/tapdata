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

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class CustomSqlInspect extends ConnectorNodeBase {



    public void initConnectorNode(ExecuteCommandFunction ExecuteCommandFunction) throws IllegalAccessException, NoSuchFieldException {
        Class clazz = ConnectorNode.class;
        ConnectorFunctions connectorFunction = new ConnectorFunctions();
        connectorFunction.supportExecuteCommandFunction(ExecuteCommandFunction);
        Field connectorFunctions = clazz.getDeclaredField("connectorFunctions");
        connectorFunctions.setAccessible(true);
        connectorFunctions.set(sqlConnectorNode, connectorFunction);

        TapNodeSpecification specification = sqlConnectorNode.getTapNodeInfo().getTapNodeSpecification();
        TapConnectorContext connectorContext = new TapConnectorContext(specification,
                new DataMap(), new DataMap(), new TapLog());
        TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("test", myTapTable);
        PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
        connectorContext.setTableMap(pdkTableMap);

        Field connector = clazz.getDeclaredField("connectorContext");
        connector.setAccessible(true);
        connector.set(sqlConnectorNode, connectorContext);
    }


    /**
     * mysql count正确返回值
     */
    @Test
    public void customCountNormalQuery() throws NoSuchFieldException, IllegalAccessException {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery");
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));

        MockExecuteCommandFunction executeCommandFunction = new MockCountNormalExecuteCommandFunction();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("COUNT(1)", 100);
        List<Map<String, Object>> listData = new ArrayList<>();
        listData.add(map);
        executeCommandFunction.setData(listData);
        initConnectorNode(executeCommandFunction);

        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(executeCommandFunction, tapExecuteCommand, sqlConnectorNode);
        long count = list.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();
        Assert.assertEquals(100, count);
    }


    /**
     * 查询count返回值为instanceof 为long mongodb
     */
    @Test
    public void customCountNormalLongQuery() throws NoSuchFieldException, IllegalAccessException {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery");
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));

        MockExecuteCommandFunction executeCommandFunction = new MockCountNormalLongExecuteCommandFunction();
        Long countData = 100L;
        executeCommandFunction.setData(countData);
        initConnectorNode(executeCommandFunction);

        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(executeCommandFunction, tapExecuteCommand, sqlConnectorNode);
        long count = list.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();
        Assert.assertEquals(100, count);
    }

    /**
     * 查询count为null
     */
    @Test
    public void CustomCountResultNullQuery() throws NoSuchFieldException, IllegalAccessException {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery");
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));

        MockExecuteCommandFunction executeCommandFunction = new MockCountResultNullExecuteCommandFunction();
        executeCommandFunction.setData(null);
        initConnectorNode(executeCommandFunction);

        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(executeCommandFunction, tapExecuteCommand, sqlConnectorNode);
        Assert.assertTrue(CollectionUtils.isEmpty(list));
    }

    /**
     * 查询count异常
     */
    @Test(expected = TapPdkRunnerUnknownException.class)
    public void CustomCountResultException() throws NoSuchFieldException, IllegalAccessException {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery");
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));

        MockExecuteCommandFunction executeCommandFunction = new MockExceptionExecuteCommandFunction();
        SQLException sqlException = new SQLSyntaxErrorException("Table 'mydb.test' doesn't exist",
                "42S02", 1146);
        executeCommandFunction.setData(sqlException);
        initConnectorNode(executeCommandFunction);

        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(executeCommandFunction, tapExecuteCommand, sqlConnectorNode);
    }


    /**
     * 查询list正常返回值
     */
    @Test
    public void customAndQueryListNormal() throws NoSuchFieldException, IllegalAccessException {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery");
        MockExecuteCommandFunction executeCommandFunction = new MockListNormalExecuteCommandFunction();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", "100");
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(map);
        executeCommandFunction.setData(list);
        initConnectorNode(executeCommandFunction);

        PdkResult pdkResult = new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                new HashSet<>(), sqlConnectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                true, customCommand);
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        pdkResult.executeQueryCommand(tapExecuteCommand);
        LinkedBlockingQueue<Map<String, Object>> linkedBlockingQueue = pdkResult.getQueue();
        Map<String, Object> mapData = linkedBlockingQueue.poll();
        Assert.assertEquals(mapData.get("id").toString(), "100");

    }


    /**
     * 查询list为空
     */
    @Test
    public void customAndQueryListEmpty() throws NoSuchFieldException, IllegalAccessException {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery");


        MockExecuteCommandFunction executeCommandFunction = new MockListNormalExecuteCommandFunction();
        List<Map<String, Object>> list = new ArrayList<>();
        executeCommandFunction.setData(list);
        initConnectorNode(executeCommandFunction);

        PdkResult pdkResult = new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                new HashSet<>(), sqlConnectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                true, customCommand);
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        pdkResult.executeQueryCommand(tapExecuteCommand);
        LinkedBlockingQueue<Map<String, Object>> linkedBlockingQueue = pdkResult.getQueue();
        Assert.assertTrue(linkedBlockingQueue.isEmpty());
    }


    /**
     * 查询list异常
     */
    @Test(expected = TapPdkRunnerUnknownException.class)
    public void customAndQueryListException() throws NoSuchFieldException, IllegalAccessException {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery");

        MockExecuteCommandFunction executeCommandFunction = new MockExceptionExecuteCommandFunction();
        SQLException sqlException = new SQLSyntaxErrorException("Table 'mydb.test' doesn't exist",
                "42S02", 1146);
        executeCommandFunction.setData(sqlException);
        initConnectorNode(executeCommandFunction);


        PdkResult pdkResult = new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                new HashSet<>(), sqlConnectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                true, customCommand);
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        pdkResult.executeQueryCommand(tapExecuteCommand);
    }


}
