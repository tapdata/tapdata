package inspect;

import ConnectorNode.ConnectorNodeBase;
import MockConnector.MockAdvanceFilterQueryFunction;
import cn.hutool.core.map.MapUtil;
import com.tapdata.entity.Connections;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.inspect.compare.PdkResult;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryNextBatchTest extends ConnectorNodeBase {

    private List<SortOn> sortOnList = new LinkedList<>();

    private static Projection projection = null;


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

        ExecuteCommandFunction  executeCommandFunction= mock(ExecuteCommandFunction.class);
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
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction,diffKeyValues,dataKey,false,null);

        // execution method
        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        Thread.sleep(3000);

        DataMap actualData  = mockAdvanceFilterQueryFunction.getTapAdvanceFilter().getMatch();

        // output results
        Assert.assertTrue(MapUtil.isNotEmpty(actualData));


    }


    @Test
    public void testQueryNextBatchNoMatch() throws Throwable {
        // init param
        MockAdvanceFilterQueryFunction mockAdvanceFilterQueryFunction = new MockAdvanceFilterQueryFunction();
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction,new ArrayList<>(),new ArrayList<>(),
        false,null);

        // execution method
        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        Thread.sleep(3000);

        DataMap actualData  = mockAdvanceFilterQueryFunction.getTapAdvanceFilter().getMatch();

        // output results
        Assert.assertTrue(MapUtil.isEmpty(actualData));



    }


    public PdkResult handleMockAdvanceFilterFunction(QueryByAdvanceFilterFunction executeCommandFunction,
                                                     List<List<Object>> diffKeyValues, List<String> dataKey,
                                                     boolean enableCustomCommand,Map<String, Object> customCommand) throws Throwable {
        init(executeCommandFunction);

        return new PdkResult(new ArrayList<>(), new Connections(), myTapTable.getId(),
                new HashSet<>(), sqlConnectorNode, true, dataKey, diffKeyValues, new ArrayList<>(),
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
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction,diffKeyValues,dataKey,true,customCommand);

        // execution method
        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        Thread.sleep(3000);

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
        PdkResult pdkResult = handleMockAdvanceFilterFunction(mockAdvanceFilterQueryFunction,new ArrayList<>(),new ArrayList<>()
        ,true,customCommand);

        // execution method
        ReflectionTestUtils.invokeMethod(pdkResult,"queryNextBatch",null);

        Thread.sleep(3000);

        boolean actualData  = mockAdvanceFilterQueryFunction.isFlag();

        // output results
        Assert.assertFalse(actualData);

    }

}
