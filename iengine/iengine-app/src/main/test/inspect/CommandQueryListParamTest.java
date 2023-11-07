package inspect;

import ConnectorNode.ConnectorNodeBase;
import cn.hutool.core.map.MapUtil;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.inspect.compare.PdkResult;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
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
    public void initSort() throws NoSuchFieldException, IllegalAccessException {
        sortOnList.add(SortOn.ascending("id"));
        sqlConnectorNode = new ConnectorNode();
        TapNodeSpecification tapNodeSpecificationSql = new TapNodeSpecification();
        tapNodeSpecificationSql.setId("mysql");
        initConnectorNode(sqlConnectorNode, tapNodeSpecificationSql);

        mongoConnectorNode = new ConnectorNode();
        TapNodeSpecification tapNodeSpecificationMongo = new TapNodeSpecification();
        tapNodeSpecificationMongo.setId("mongodb");
        initConnectorNode(mongoConnectorNode, tapNodeSpecificationMongo);

        TapTable table = new TapTable();
        table.setId("testID");
        myTapTable = table;
    }


    /**
     * 检查替换查询输入语句关系型数据库
     * select * from  test  where id>2
     * test SetCommandQueryParam function
     */
    @Test
    public void testSetCommandQueryParamQueryListSql() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test  where id>2";
        CommandCountParamTest.setCustomCommandParam(customCommand, querySql, whereSql, "", "");

       // execution method
        PdkResult.setCommandQueryParam(customCommand, sqlConnectorNode, myTapTable, sortOnList, projection);

        // actual data
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String actualData = (String) params.get("sql");

        // expected data
        char escapeChar = '"';
        StringBuilder builder = new StringBuilder();
        builder.append("  ORDER BY ");
        builder.append(sortOnList.stream().map(v -> v.toString(String.valueOf(escapeChar))).collect(Collectors.joining(", "))).append(' ');
        String expectedData = querySql + whereSql + builder;

        // output results
        Assert.assertEquals(expectedData, actualData);
    }

    /**
     * 检查替换查询输入语句关系型数据库 order by 排序
     * select * from  test  where id>2   order by id desc
     * test SetCommandQueryParam function
     */
    @Test
    public void testSetCommandQueryParamQueryListSqlByOrder() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        String querySql = "select *     from";
        String whereSql = " test  where id>2";
        String orderSql = "  order by id desc";
        CommandCountParamTest.setCustomCommandParam(customCommand, querySql, whereSql, orderSql, "");

        // execution method
        PdkResult.setCommandQueryParam(customCommand, sqlConnectorNode, myTapTable, sortOnList, projection);

        // actual data
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String actualData = (String) params.get("sql");

        // expected data
        String expectedData =querySql + whereSql + orderSql;

        // output results
        Assert.assertEquals(expectedData, actualData);
    }


    /**
     * 查询mongodb数据库
     * select * from  test  where id>2   order by id desc
     * test SetCommandQueryParam function
     */
    @Test
    public void testSetCommandQueryParamQueryMongoQuery() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("command", "executeQuery");
        customParam.put("op", "find");
        customParam.put("filter", "{id:2}");
        customCommand.put("params", customParam);

        // execution method
        PdkResult.setCommandQueryParam(customCommand, mongoConnectorNode, myTapTable, sortOnList, projection);

        // actual data
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String actualCollection = (String) params.get("collection");
        Map<String, Object> actualSortMap = (Map<String, Object>) params.get("sort");

        // expected data
        String expectedCollection = myTapTable.getId();

        // output results
        Assert.assertEquals(expectedCollection, actualCollection);
        Assert.assertTrue(actualSortMap.containsKey(sortOnList.get(0)));
    }


    @Test
    public void testSetCommandQueryParamQuerySortEmpty() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("command", "executeQuery");
        customParam.put("op", "find");
        customParam.put("filter", "{id:2}");
        customCommand.put("params", customParam);


        // execution method
        PdkResult.setCommandQueryParam(customCommand, mongoConnectorNode, myTapTable, new LinkedList<>(), projection);

        // actual data
        Map<String, Object> params = (Map<String, Object>) customCommand.get("params");
        String actualCollection = (String) params.get("collection");
        Map<String, Object> actualSortMap = (Map<String, Object>) params.get("sort");

        // expected data
        String expectedCollection = myTapTable.getId();

        // output results
        Assert.assertEquals(expectedCollection, actualCollection);
        Assert.assertTrue(MapUtil.isEmpty(actualSortMap));
    }

    /**
     * command 为null
     * {id:2}
     * test SetCommandQueryParam function
     */
    @Test
    public void testSetCommandQueryParamCommandNull() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("command", null);
        customParam.put("op", "find");
        customParam.put("filter", "{id:2}");
        customCommand.put("params", customParam);

        try {
            // execution method
            PdkResult.setCommandQueryParam(customCommand, mongoConnectorNode, myTapTable, sortOnList, projection);
        }catch (Exception e){
            Assert.assertTrue(e instanceof NullPointerException);
        }

    }


    /**
     * params 为null
     * {id:2}
     * test SetCommandQueryParam function
     */
    @Test
    public void testSetCommandQueryParamParamNull() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("command", "executeQuery");
        customParam.put("op", "find");
        customParam.put("filter", "{id:2}");
        customCommand.put("params", null);

        try {
            // execution method
            PdkResult.setCommandQueryParam(customCommand, mongoConnectorNode, myTapTable, sortOnList, projection);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NullPointerException);
        }
    }

    /**
     * params 为""
     * {id:2}
     * test SetCommandQueryParam function
     */
    @Test
    public void testSetCommandQueryParamParamEmpty() {
        // input param
        Map<String, Object> customCommand = new LinkedHashMap<>();
        Map<String, Object> customParam = new LinkedHashMap<>();
        customCommand.put("command", "executeQuery");
        customParam.put("op", "find");
        customParam.put("filter", "{id:2}");
        customCommand.put("params", "");

        try {
            // execution method
            PdkResult.setCommandQueryParam(customCommand, mongoConnectorNode, myTapTable, sortOnList, projection);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ClassCastException);
        }
    }

}
