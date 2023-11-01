package inspect;

import MockConnector.MockExecuteCommandFunction;
import com.tapdata.entity.Connections;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.inspect.compare.PdkResult;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.error.TapPdkRunnerUnknownException;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class customSqlInspect {

    private  static ConnectorNode myConnectorNode = null;
    private  static TapTable myTapTable = null;


    private  static MockExecuteCommandFunction mockExecuteCommandFunction = null;

    @Before
    public void getMysqlConnectorNode() throws NoSuchFieldException, IllegalAccessException {

        TapTable table = new TapTable();
        table.setId("testID");
        table.setName("testID");
        myTapTable = table;
        ConnectorNode connectorNode = new ConnectorNode();
        mockExecuteCommandFunction = new MockExecuteCommandFunction();
        Class clazz = ConnectorNode.class;
        ConnectorFunctions connectorFunction  = new ConnectorFunctions();
        connectorFunction.supportExecuteCommandFunction(mockExecuteCommandFunction);
        Field connectorFunction1 = clazz.getDeclaredField("connectorFunctions");
        connectorFunction1.setAccessible(true);
        connectorFunction1.set(connectorNode, connectorFunction);
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("mysql");
        TapConnectorContext connectorContext = new TapConnectorContext(tapNodeSpecification,
                new DataMap(),new DataMap(),new TapLog());
        TapTableMap<String, TapTable> tapTableMap = TapTableMap.create("test", myTapTable);
        PdkTableMap pdkTableMap = new PdkTableMap(tapTableMap);
        connectorContext.setTableMap(pdkTableMap);


        Field connector = clazz.getDeclaredField("connectorContext");
        connector.setAccessible(true);
        connector.set(connectorNode, connectorContext);
        myConnectorNode = connectorNode;



    }

    /**
     *  mysql count正确返回值
     */
    @Test
    public void customAndNormalQuery() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery-normal");
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(mockExecuteCommandFunction, tapExecuteCommand, myConnectorNode);
        long count = list.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();
        Assert.assertEquals(100, count);
    }


    /**
     *  查询count返回值为instanceof 为long mongodb
     */
    @Test
    public void customAndNormalLongQuery() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery-normal-long");
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(mockExecuteCommandFunction, tapExecuteCommand, myConnectorNode);
        long count = list.get(0).values().stream().mapToLong(value -> Long.parseLong(value.toString())).sum();
        Assert.assertEquals(100, count);
    }

    /**
     *  查询count为null
     */
    @Test
    public void CustomResultNullQuery() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery-resultNull");
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(mockExecuteCommandFunction, tapExecuteCommand, myConnectorNode);
        Assert.assertTrue(CollectionUtils.isEmpty(list));
    }

    /**
     *  查询count异常
     */
    @Test(expected= TapPdkRunnerUnknownException.class)
    public void CustomResultException() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery-exception");
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        List<Map<String, Object>> list =
                TableRowCountInspectJob.executeCommand(mockExecuteCommandFunction, tapExecuteCommand, myConnectorNode);
    }


    /**
     *  查询list正常返回值
     */
    @Test
    public void  customAndQueryListNormal(){
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery-listNormal");
        PdkResult pdkResult =  new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                new HashSet<>(), myConnectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
        true,customCommand);
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        pdkResult.executeQueryCommand(tapExecuteCommand);
        LinkedBlockingQueue<Map<String, Object>>  linkedBlockingQueue=  pdkResult.getQueue();
        Map<String, Object> map = linkedBlockingQueue.poll();
        Assert.assertEquals(map.get("id").toString(),"100");

    }



    /**
     *  查询list为空
     */
    @Test
    public void customAndQueryListEmpty() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery-listNull");
        PdkResult pdkResult = new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                new HashSet<>(), myConnectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                true, customCommand);
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        pdkResult.executeQueryCommand(tapExecuteCommand);
        LinkedBlockingQueue<Map<String, Object>> linkedBlockingQueue = pdkResult.getQueue();
        Assert.assertTrue(linkedBlockingQueue.isEmpty());
    }


    /**
     *  查询list异常
     */
    @Test(expected = TapPdkRunnerUnknownException.class)
    public void customAndQueryListException() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", "select *     from  test  where id>2");
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery-exception");
        PdkResult pdkResult = new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                new HashSet<>(), myConnectorNode, true, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                true, customCommand);
        TapExecuteCommand tapExecuteCommand = TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
        pdkResult.executeQueryCommand(tapExecuteCommand);
    }




}
