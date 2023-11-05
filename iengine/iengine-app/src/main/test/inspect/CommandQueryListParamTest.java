package inspect;

import ConnectorNode.ConnectorNodeBase;
import io.tapdata.inspect.compare.PdkResult;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.SortOn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CommandQueryListParamTest extends ConnectorNodeBase {
    private List<SortOn> sortOnList = new LinkedList<>();

    private static Projection projection = null;


    @Before
    public void initSort(){
        sortOnList.add(SortOn.ascending("id"));
    }


    /**
     * 检查替换查询输入语句关系型数据库
     * select * from  test  where id>2
     */
    @Test
    public void queryListSql() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test  where id>2";
        customParam.put("sql", querySql + whereSql);
        customCommand.put("params", customParam);
        PdkResult.setCommandQueryParam(customCommand, sqlConnectorNode, myTapTable, sortOnList, projection);
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String sql = (String) params.get("sql");
        char escapeChar = '"';
        StringBuilder builder = new StringBuilder();
        builder.append("  ORDER BY ");
        builder.append(sortOnList.stream().map(v -> v.toString(String.valueOf(escapeChar))).collect(Collectors.joining(", "))).append(' ');
        Assert.assertEquals(querySql + whereSql + builder, sql);
    }

    /**
     * 检查替换查询输入语句关系型数据库 order by 排序
     * select * from  test  where id>2   order by id desc
     */
    @Test
    public void queryListSqlByOrder() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test  where id>2";
        String orderSql = "  order by id desc";
        customParam.put("sql", querySql + whereSql + orderSql);
        customCommand.put("params", customParam);
        PdkResult.setCommandQueryParam(customCommand, sqlConnectorNode, myTapTable, sortOnList, projection);
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String sql = (String) params.get("sql");
        Assert.assertEquals(querySql + whereSql + orderSql, sql);
    }


    /**
     * 查询mongodb数据库
     * select * from  test  where id>2   order by id desc
     */
    @Test
    public void queryMongoQueryTest() {
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("command", "executeQuery");
        customParam.put("op", "find");
        customParam.put("filter", "{id:2}");
        customCommand.put("params", customParam);
        PdkResult.setCommandQueryParam(customCommand, mongoConnectorNode, myTapTable, sortOnList, projection);
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String collection = (String) params.get("collection");
        Map<String, Object> sortMap = (Map<String, Object>) params.get("sort");
        Assert.assertEquals(myTapTable.getId(), collection);
        Assert.assertTrue(sortMap.containsKey("id"));
    }

}
