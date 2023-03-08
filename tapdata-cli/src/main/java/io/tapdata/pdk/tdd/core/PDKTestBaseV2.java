package io.tapdata.pdk.tdd.core;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.tdd.core.base.*;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;

import java.lang.reflect.Method;
import java.util.*;

public class PDKTestBaseV2 extends PDKTestBase {
    protected static final LangUtil langUtil = LangUtil.lang(LangUtil.LANG_PATH_V2);

    protected void execTest(TestStart start, TestExec exec, TestStop stop) throws NoSuchMethodException{
        this.execTest(
                Thread.currentThread().getStackTrace()[2].getMethodName(),
                start,
                exec,
                stop
        );
    }
    protected void execTest(TestExec exec, TestStop stop) throws NoSuchMethodException{
        this.execTest(
                Thread.currentThread().getStackTrace()[2].getMethodName(),
                null,
                exec,
                stop
        );
    }
    protected void execTest(TestExec exec) throws NoSuchMethodException{
        this.execTest(
                Thread.currentThread().getStackTrace()[2].getMethodName(),
                null,
                exec,
                null
        );
    }


    protected void execTest(String testCaseName, TestStart start, TestExec exec, TestStop stop) throws NoSuchMethodException {
        Method testCase = super.getMethod(testCaseName);
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            execute.testCase(testCase);
            try {
                Optional.ofNullable(start).ifPresent(e->e.start(prepare,testCase));
                super.connectorOnStart(prepare);
                Optional.ofNullable(exec).ifPresent(e->e.exec(prepare,testCase));
            } catch (Exception e) {
                if ( !(e instanceof TapAssertException) && !(e instanceof StreamStopException)) {
                    TapAssert.error(testCase, langUtil.formatLang("fieldModification.all.throw", e.getMessage()));
                }
            } finally {
                Optional.ofNullable(stop).ifPresent(e->e.stop(prepare,testCase));
                super.connectorOnStop(prepare);
            }
        });
    }

    /**
     * @deprecated
     * */
    protected void execTest(String testCaseName,TestExec exec) throws NoSuchMethodException {
        this.execTest(testCaseName,null,exec,null);
    }

    /**
     * @deprecated
     * */
    protected void execTest(String testCaseName,TestExec exec, TestStop stop) throws NoSuchMethodException {
        this.execTest(testCaseName,null,exec,stop);
    }

    protected List<Map<String,Object>> queryRecords(TestNode node, TapTable tapTable, Record[] records){
        Method testCase = node.recordEventExecute().testCase();
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        TapConnectorContext context = node.connectorNode().getConnectorContext();
        ConnectorFunctions connectorFunctions = node.connectorNode().getConnectorFunctions();
        QueryByFilterFunction queryByFilter = connectorFunctions.getQueryByFilterFunction();
        QueryByAdvanceFilterFunction advanceFilter = connectorFunctions.getQueryByAdvanceFilterFunction();

        DataMap dataMap = DataMap.create();
        for (Record record : records) {
            for (String primaryKey : primaryKeys) {
                dataMap.kv(primaryKey, record.get(primaryKey));
            }
        }
        List<TapFilter> filters = new ArrayList<>();
        List<Map<String,Object>> result = new ArrayList<>();
        if (Objects.nonNull(queryByFilter)) {
            TapFilter filter = new TapFilter();
            filter.setMatch(dataMap);
            filters.add(filter);
            try {
                queryByFilter.query(context, filters, tapTable, consumer -> {
                    if (Objects.nonNull(consumer) && !consumer.isEmpty()) {
                        consumer.forEach(res-> result.add(transform(node,targetTable,res.getResult())));
                    }
                });
            }catch (Throwable e){
                TapAssert.error(testCase,"QueryByAdvanceFilterFunction 抛出了一个异常，error: %s.");
            }
        } else {
            Optional.ofNullable(advanceFilter).ifPresent(filter -> {
                TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
                tapAdvanceFilter.match(dataMap);
                try {
                    filter.query(context, tapAdvanceFilter, tapTable, consumer -> {
                        for (Map<String, Object> data : consumer.getResults()) {
                            result.add(transform(node,targetTable,data));
                        }
                    });
                } catch (Throwable throwable) {
                    TapAssert.error(testCase,"QueryByAdvanceFilterFunction 抛出了一个异常，error: %s.");
                }
            });
        }
        return result;
    }

    protected void translation(TestNode node) {
        //@TODO 打开大事物
        DataMap nodeConfig = node.connectorNode().getConnectorContext().getNodeConfig();
        nodeConfig.kv("", "");
    }
}




