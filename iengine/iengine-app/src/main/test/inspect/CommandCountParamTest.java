package inspect;

import ConnectorNode.ConnectorNodeBase;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class CommandCountParamTest extends ConnectorNodeBase {


    public final String SELECT_COUNT_QUERY_SQL = "SELECT COUNT(1) FROM";

    /**
     * 检查替换查询输入语句关系型数据库,查询所有字段
     * select *  from  test  where id>2
     * test SetCommandCountParam function
     */
    @Test
    public void testSetCommandCountParamQueryCountBySelectAll() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test  where id>2";
        customParam.put("sql", querySql + whereSql);
        customCommand.put("params", customParam);

        // execution method
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, sqlConnectorNode, myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String actualData = (String) params.get("sql");

        // expected data
        String expectedData = SELECT_COUNT_QUERY_SQL + whereSql;

        // output results
        Assert.assertEquals(expectedData, actualData);
    }

    /**
     * 检查替换查询输入语句关系型数据库查询 部分字段
     * select id,name from  test  where id>2
     * test SetCommandCountParam function
     */
    @Test
    public void testSetCommandCountParamQueryCountBySelectPart() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        String querySql = "select id,name     from";
        String whereSql = "test  where id>2";
        setCustomCommandParam(customCommand, querySql, whereSql, "", "");

        // execution method
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, sqlConnectorNode, myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String actualData = (String) params.get("sql");

        // expected data
        String expectedData = SELECT_COUNT_QUERY_SQL + whereSql;

        // output results
        Assert.assertEquals(expectedData, actualData);
    }

    /**
     * select * from  test  where id>2 order by id desc
     * test SetCommandCountParam function
     * 检查替换查询输入语句关系型数据库 排序查询
     */
    @Test
    public void testSetCommandCountParamQueryCountSqlByOrder() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test where id>2";
        String orderSql = " order by id desc";
        setCustomCommandParam(customCommand, querySql, whereSql, orderSql, "");

        // execution method
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, sqlConnectorNode, myTapTable);

        // actual data
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String actualData = (String) params.get("sql");

        // expected data
        String expectedData = SELECT_COUNT_QUERY_SQL+ whereSql;

        // output results
        Assert.assertEquals(expectedData, actualData);
    }


    /**
     * select * from  test  where id>2 group by id
     * test SetCommandCountParam function
     * 检查替换查询输入语句关系型数据库 聚合查询
     */
    @Test
    public void testSetCommandCountParamQueryCountSqlByGroup() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test where id>2";
        String groupSql = " group by id";
        setCustomCommandParam(customCommand, querySql, whereSql, "", groupSql);

        // execution method
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, sqlConnectorNode, myTapTable);

        // actual data
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String actualData = (String) params.get("sql");

        // expected data
        String expectedData = SELECT_COUNT_QUERY_SQL + " (" + querySql + whereSql + groupSql + ")";

        // output results
        Assert.assertEquals(expectedData, actualData);
    }


    /**
     * 测试mongodb count
     */
    @Test
    public void testSetCommandCountParamQueryMongoCountTest() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("command", "executeQuery");
        customParam.put("op", "find");
        customParam.put("filter", "{id:2}");
        customCommand.put("params", customParam);

        // execution method
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, mongoConnectorNode, myTapTable);

        // actual data
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String collection = (String) params.get("collection");

        // output results
        Assert.assertTrue(collection.equals(myTapTable.getId()) &&
                copyCustomCommand.get("command").equals("count"));
    }


    public static TapExecuteCommand setCustomCommandParam(Map<String, Object> customCommand, String querySql,
                                                          String whereSql, String orderSql, String groupSql) {
        Map<String, Object> customParam = new LinkedHashMap<>();
        customParam.put("sql", querySql + whereSql + orderSql + groupSql);
        customCommand.put("params", customParam);
        customCommand.put("command", "executeQuery");
        return TapExecuteCommand.create()
                .command((String) customCommand.get("command")).params((Map<String, Object>) customCommand.get("params"));
    }

}
