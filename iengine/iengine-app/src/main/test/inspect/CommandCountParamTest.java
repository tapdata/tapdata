package inspect;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

public class CommandCountParamTest {

    private  static ConnectorNode mysqlConnectorNode = null;
    private  static TapTable myTapTable = null;

    private  static ConnectorNode mongoConnectorNode = null;
    @Before
    public void getMysqlConnectorNode() throws NoSuchFieldException, IllegalAccessException {
        ConnectorNode connectorNode = new ConnectorNode();
        TapNodeInfo tapNodeInfo = new TapNodeInfo();
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("mysql");
        tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
        Class clazz = ConnectorNode.class;
        Class<?> superClass = clazz.getSuperclass();
        Field field_name = superClass.getDeclaredField("tapNodeInfo");
        field_name.setAccessible(true);
        field_name.set(connectorNode, tapNodeInfo);
        mysqlConnectorNode = connectorNode;

        TapTable table = new TapTable();
        table.setId("testID");
        myTapTable = table;
    }

    @Before
    public void getMongodbConnectorNode() throws NoSuchFieldException, IllegalAccessException {
        ConnectorNode connectorNode = new ConnectorNode();
        TapNodeInfo tapNodeInfo = new TapNodeInfo();
        TapNodeSpecification tapNodeSpecification = new TapNodeSpecification();
        tapNodeSpecification.setId("mongodb");
        tapNodeInfo.setTapNodeSpecification(tapNodeSpecification);
        Class clazz = ConnectorNode.class;
        Class<?> superClass = clazz.getSuperclass();
        Field field_name = superClass.getDeclaredField("tapNodeInfo");
        field_name.setAccessible(true);
        field_name.set(connectorNode, tapNodeInfo);
        mongoConnectorNode = connectorNode;
    }

    /**
     * 检查替换查询输入语句关系型数据库
     * select *     from  test  where id>2
     */
    @Test
    public void  querySqlTest(){
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam= new LinkedHashMap<>();
        customParam.put("sql","select *     from  test  where id>2");
        customCommand.put("params",customParam);
        Map<String, Object> copyCustomCommand =  TableRowCountInspectJob.setCommandCountParam(customCommand,mysqlConnectorNode,myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertTrue(sql.contains("SELECT COUNT(1) FROM"));
    }

    @Test
    public void  querySqlTest1(){
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam= new LinkedHashMap<>();
        customParam.put("sql","select id,name     from  test  where id>2");
        customCommand.put("params",customParam);
        Map<String, Object> copyCustomCommand =  TableRowCountInspectJob.setCommandCountParam(customCommand,mysqlConnectorNode,myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertTrue(sql.contains("SELECT COUNT(1) FROM"));
    }

    /**
     * 检查替换查询输入语句关系型数据库
     * select *     from  test  where id>2 order by id desc
     */
    @Test
    public void  querySqlOrderByTest() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam= new LinkedHashMap<>();
        customParam.put("sql","select *     from  test  where id>2 order by id desc");
        customCommand.put("params",customParam);
        Map<String, Object> copyCustomCommand =  TableRowCountInspectJob.setCommandCountParam(customCommand,mysqlConnectorNode,myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertTrue(!sql.contains("order by id") && sql.contains("SELECT COUNT(1) FROM"));
    }


    /**
     * 检查替换查询输入语句关系型数据库
     * select *     from  test  where id>2 order by id desc
     */
    @Test
    public void  querySqlGroupByTest(){
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam= new LinkedHashMap<>();
        customParam.put("sql","select  id     from  test  where id>2 group by id");
        customCommand.put("params",customParam);
        Map<String, Object> copyCustomCommand =  TableRowCountInspectJob.setCommandCountParam(customCommand,mysqlConnectorNode,myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertTrue(sql.contains("SELECT COUNT(1) FROM"));
    }




    @Test
    public void  queryMongoCountTest() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam= new LinkedHashMap<>();
        customCommand.put("command","executeQuery");
        customParam.put("op","find");
        customParam.put("filter","{id:2}");
        customCommand.put("params",customParam);
        Map<String, Object> copyCustomCommand =  TableRowCountInspectJob.setCommandCountParam(customCommand,mongoConnectorNode,myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String collection = (String) params.get("collection");
        Assert.assertTrue(collection.equals(myTapTable.getId()) &&
                copyCustomCommand.get("command").equals("count"));
    }



}
