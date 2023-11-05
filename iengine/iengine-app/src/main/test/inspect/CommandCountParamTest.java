package inspect;

import ConnectorNode.ConnectorNodeBase;
import io.tapdata.inspect.compare.TableRowCountInspectJob;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class CommandCountParamTest extends ConnectorNodeBase {


    /**
     * 检查替换查询输入语句关系型数据库,查询所有字段
     * select *  from  test  where id>2
     */
    @Test
    public void queryCountBySelectAll() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test  where id>2";
        customParam.put("sql", querySql + whereSql);
        customCommand.put("params", customParam);
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, sqlConnectorNode, myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertEquals("SELECT COUNT(1) FROM" + whereSql, sql);
    }

    /**
     * 检查替换查询输入语句关系型数据库查询部分字段
     * select id,name from  test  where id>2
     */
    @Test
    public void queryCountBySelectPart() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        String querySql = "select id,name     from";
        String whereSql = "test  where id>2";
        customParam.put("sql", querySql + whereSql);
        customCommand.put("params", customParam);
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, sqlConnectorNode, myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertEquals("SELECT COUNT(1) FROM" + whereSql, sql);
    }

    /**
     * 检查替换查询输入语句关系型数据库 排序查询
     * select *     from  test  where id>2 order by id desc
     */
    @Test
    public void queryCountSqlByOrder() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test where id>2";
        String orderSql = " order by id desc";
        customParam.put("sql", querySql + whereSql + orderSql);
        customCommand.put("params", customParam);
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, sqlConnectorNode, myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertEquals("SELECT COUNT(1) FROM" + whereSql, sql);
    }


    /**
     * 检查替换查询输入语句关系型数据库 聚合查询
     * select * from  test  where id>2 group by id
     */
    @Test
    public void queryCountSqlByGroup() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test where id>2";
        String orderSql = " group by id";
        customParam.put("sql", querySql + whereSql + orderSql);
        customCommand.put("params", customParam);
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, sqlConnectorNode, myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertEquals("SELECT COUNT(1) FROM " + "(" + querySql + whereSql + orderSql + ")", sql);
    }


    /**
     * 测试mongodb count
     */
    @Test
    public void queryMongoCountTest() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("command", "executeQuery");
        customParam.put("op", "find");
        customParam.put("filter", "{id:2}");
        customCommand.put("params", customParam);
        Map<String, Object> copyCustomCommand = TableRowCountInspectJob.setCommandCountParam(customCommand, mongoConnectorNode, myTapTable);
        Map<String, Object> params = (Map<String, Object>) copyCustomCommand.get("params");
        String collection = (String) params.get("collection");
        Assert.assertTrue(collection.equals(myTapTable.getId()) &&
                copyCustomCommand.get("command").equals("count"));
    }


}
