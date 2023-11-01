package inspect;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.inspect.compare.PdkResult;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CommandQueryListParamTest {

    private  static ConnectorNode mysqlConnectorNode = null;
    private  static TapTable myTapTable = null;
    private  List<SortOn> sortOnList = new LinkedList<>();

    private  static ConnectorNode mongoConnectorNode = null;

    private static Projection projection =null;


    @Before
    public void getMysqlConnectorNode() throws NoSuchFieldException, IllegalAccessException, InstantiationException {
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
        sortOnList.add(SortOn.ascending("id"));

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
        PdkResult.setCommandQueryParam(customCommand,mysqlConnectorNode,myTapTable,sortOnList,projection);
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertTrue(sql.contains("ORDER BY"));
    }

    @Test
    public void  querySqlTest1(){
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam= new LinkedHashMap<>();
        customParam.put("sql","select *     from  test  where id >2  order by id desc");
        customCommand.put("params",customParam);
        PdkResult.setCommandQueryParam(customCommand,mysqlConnectorNode,myTapTable,sortOnList,projection);
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertTrue(sql.contains("order by id desc"));
    }



    @Test
    public void  queryMongoQueryTest() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam= new LinkedHashMap<>();
        customCommand.put("command","executeQuery");
        customParam.put("op","find");
        customParam.put("filter","{id:2}");
        customCommand.put("params",customParam);
        PdkResult.setCommandQueryParam(customCommand,mongoConnectorNode,myTapTable,sortOnList,projection);
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String collection = (String) params.get("collection");
        Map<String, Object> sortMap = (Map<String, Object>) params.get("sort");
        Assert.assertTrue(collection.equals(myTapTable.getId()));
        Assert.assertTrue(sortMap.containsKey("id"));
    }

}
