package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Date;

@DisplayName("createTableTest.test")//CreateTableFunction/CreateTableV2Function建表
@TapGo(sort = 9)
public class CreateTableTest extends PDKTestBase {

    @DisplayName("createTableV2")//用例1，CreateTableFunction已过期， 应使用CreateTableV2Function
    @TapTestCase(sort = 1)
    @Test
    /**
     * CreateTableFunction和CreateTableV2Function的差距是，
     * V2版本会返回CreateTableOptions对象，
     * 以下测试用例这两个方法都需要测试，
     * 如果同时实现了两个方法只需要测试CreateTableV2Function。
     * 检查如果只实现了CreateTableFunction，没有实现CreateTableV2Function时，报出警告， 推荐使用CreateTableV2Function方法来实现建表
     * */
    void createTableV2(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("createTableV2");
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();

                if (super.verifyFunctions(functions,testCase)){
                    return;
                }
                String tableId = targetTable.getId().replaceAll("-","_");

                CreateTableV2Function createTableV2 = functions.getCreateTableV2Function();
                TapAssert.asserts(()->
                        Assertions.assertNotNull(createTableV2, TapSummary.format("createTable.v2Null",targetTable.getId()))
                ).acceptAsWarn(testCase,TapSummary.format("createTable.v2NotNull",tableId));
                CreateTableFunction createTable = null;
                if(null == createTableV2) {
                    createTable = functions.getCreateTableFunction();
                    CreateTableFunction finalCreateTable = createTable;
                    TapAssert.asserts(() ->
                            Assertions.assertNotNull(finalCreateTable, TapSummary.format("createTable.null", targetTable.getId()))
                    ).acceptAsWarn(testCase, TapSummary.format("createTable.notNull", tableId));
                }

                boolean isV1 = null != createTable;
                boolean isV2 = null != createTableV2;
                if (!isV1 && !isV2){
                    return;
                }
                targetTable.setId(tableId);
                targetTable.setName(this.targetTable.getId());
                TapCreateTableEvent createTableEvent = super.modelDeductionForCreateTableEvent(prepare.connectorNode());
                //如果同时实现了两个方法只需要测试CreateTableV2Function。(V2优先)
                if (isV2){
                    CreateTableOptions table = createTableV2.createTable(connectorContext, createTableEvent);
                    hasCreateTable = this.verifyTableIsCreated("CreateTableV2Function", prepare);
                    return;
                }
                //检查如果只实现了CreateTableFunction，没有实现CreateTableV2Function时，报出警告，
                //推荐使用CreateTableV2Function方法来实现建表
                createTable.createTable(connectorContext, createTableEvent);
                hasCreateTable = this.verifyTableIsCreated("CreateTableFunction", prepare);
            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (hasCreateTable) {
                    prepare.recordEventExecute().dropTable();
                }
                if (null != prepare.connectorNode()){
                    super.connectorOnStop(prepare);
                }
            }
        });
    }

    boolean verifyTableIsCreated(String createMethod,TestNode prepare){
        ConnectorNode connectorNode = prepare.connectorNode();
        TapConnector connector = connectorNode.getConnector();
        TapConnectorContext connectorContext = connectorNode.getConnectorContext();
        String tableIdTarget = targetTable.getId();
        Method testBase = prepare.recordEventExecute().testCase();
        List<TapTable> consumer = new ArrayList<>();
        try {
            connector.discoverSchema(connectorContext,list(tableIdTarget),1000,con->{
                if(null!=con) consumer.addAll(con);
            });
        } catch (Throwable throwable) {
        }
        TapAssert.asserts(()->{
            Assertions.assertFalse(consumer.isEmpty(), TapSummary.format("verifyTableIsCreated.error",tableIdTarget,consumer.size(), createMethod, tableIdTarget));
        }).acceptAsError(testBase,TapSummary.format("verifyTableIsCreated.succeed",tableIdTarget,consumer.size(), createMethod,tableIdTarget));
        return !consumer.isEmpty();
    }

    TapTable getTableForAllTapType(){
        return table(tableNameCreator.tableName())
                .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("TYPE_ARRAY", JAVA_Array).tapType(tapArray()))
                .add(field("TYPE_BINARY", JAVA_Binary).tapType(tapBinary().bytes(100L)))
                .add(field("TYPE_BOOLEAN", JAVA_Boolean).tapType(tapBoolean()))
                .add(field("TYPE_DATE", JAVA_Date).tapType(tapDate()))
                .add(field("TYPE_DATETIME", "Date_Time").tapType(tapDateTime().fraction(3)))
                .add(field("TYPE_MAP", JAVA_Map).tapType(tapMap()))
                .add(field("TYPE_NUMBER_Long", JAVA_Long).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("TYPE_NUMBER_INTEGER", JAVA_Integer).tapType(tapNumber().maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))))
                .add(field("TYPE_NUMBER_BigDecimal", JAVA_BigDecimal).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(10000).scale(1000).fixed(true)))
                .add(field("TYPE_NUMBER_Float", JAVA_Float).tapType(tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).fixed(false).scale(8).precision(38)))
                .add(field("TYPE_NUMBER_Double", JAVA_Double).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).scale(17).precision(309).fixed(false)))
                .add(field("TYPE_STRING_1", JAVA_String).tapType(tapString().bytes(20L)))
                .add(field("TYPE_STRING_2", JAVA_String).tapType(tapString().bytes(20L)))
                .add(field("TYPE_INT64", "INT64").tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("TYPE_TIME", "Time").tapType(tapTime()))
                .add(field("TYPE_YEAR", "Year").tapType(tapYear()));
    }



    @DisplayName("allTapType")//用例2， 使用TapType全类型11个类型推演建表测试
    @TapTestCase(sort = 2)
    @Test
    /**
     * 使用TapType的11种类型（类型的长度尽量短小）经过模型推演生成TapTable中的11个字段，
     * 里面包含name和dataType，
     * 采用随机表名建表， 建表成功之后， 返回的CreateTableOptions#tableExists应该等于false，
     * 如果没有就警告； 通过调用discoverSchema指定tableName来获取随机建立的表， 能查出这个表算是成功，
     * 对比两个TapTable里的字段名以及类型， 不同的地方需要警告。
     * */
    void allTapType(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = this.prepare(nodeInfo);
            ConnectorNode connectorNode = prepare.connectorNode();
            //使用TapType的11种类型组织表结构（类型的长度尽量短小）
            this.targetTable = getTableForAllTapType();
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("allTapType");
                execute.testCase(testCase);
                TapConnector connector = connectorNode.getConnector();
                //经过模型推演生成TapTable中的11个字段，
                LinkedHashMap<String, TapField> sourceFields = super.modelDeduction(connectorNode);
                //采用随机表名建表， 建表成功之后， 返回的CreateTableOptions#tableExists应该等于false，
                if (!(hasCreateTable = super.createTable(prepare,false))){
                    return;
                }
                String tableId = targetTable.getId();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                //如果没有就警告； 通过调用discoverSchema指定tableName来获取随机建立的表， 能查出这个表算是成功，
                Map<String,TapTable> tableMap = new HashMap<>();
                connector.discoverSchema(connectorContext,list(tableId),1000,con->{
                    if (null!=con)
                        tableMap.putAll(con.stream().filter(Objects::nonNull).collect(Collectors.toMap(TapTable::getId,tap->tap,(t1,t2)->t2)));
                });
                if (!tableMap.isEmpty()){
                    TapTable tapTable = tableMap.get(tableId);
                    TapAssert.asserts(()->{
                        Assertions.assertNotNull(tapTable, TapSummary.format("createTable.allTapType.discoverSchema.error", tableId));
                    }).acceptAsError(testCase,TapSummary.format("createTable.allTapType.discoverSchema.succeed",tableId));
                    if (null!=tapTable){
                        //对比两个TapTable里的字段名以及类型， 不同的地方需要警告。
                        LinkedHashMap<String, TapField> targetFields = tapTable.getNameFieldMap();
                        super.contrastTableFieldNameAndType(testCase,sourceFields,targetFields);
                    }
                }else {
                    TapAssert.asserts(()->{
                        Assertions.fail(TapSummary.format("createTable.allTapType.discoverSchema.error",tableId));
                    }).error(testCase);
                }
            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (hasCreateTable){
                    execute.dropTable();
                }
                super.connectorOnStop(prepare);
            }
        });
    }


    TapTable getTable(){
        return table(UUID.randomUUID().toString())
                .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("TYPE_ARRAY", JAVA_Array).tapType(tapArray()))
                .add(field("TYPE_BINARY", JAVA_Binary).tapType(tapBinary().bytes(100L)))
                .add(field("TYPE_BOOLEAN", JAVA_Boolean).tapType(tapBoolean()))
                .add(field("TYPE_DATE", JAVA_Date).tapType(tapDate()))
                .add(field("TYPE_DATETIME", "Date_Time").tapType(tapDateTime().fraction(3)))
                .add(field("TYPE_MAP", JAVA_Map).tapType(tapMap()))
                .add(field("TYPE_NUMBER_Long", JAVA_Long).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("TYPE_NUMBER_INTEGER", JAVA_Integer).tapType(tapNumber().maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))))
                .add(field("TYPE_NUMBER_BigDecimal", JAVA_BigDecimal).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(10000).scale(100).fixed(true)))
                .add(field("TYPE_NUMBER_Float", JAVA_Float).tapType(tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).fixed(false).scale(8).precision(38)))
                .add(field("TYPE_NUMBER_Double", JAVA_Double).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).scale(17).precision(309).fixed(false)))
                .add(field("TYPE_STRING_1", "STRING(100)").tapType(tapString().bytes(100L)))
                .add(field("TYPE_STRING_2", "STRING(100)").tapType(tapString().bytes(100L)))
                .add(field("TYPE_INT64", "INT64").tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("TYPE_TIME", "Time").tapType(tapTime()))
                .add(field("TYPE_YEAR", "Year").tapType(tapYear()));
    }

    @DisplayName("addIndex")//用例3， 建表时增加索引信息进行测试
    @TapTestCase(sort = 3)
    @Test
    /**
     * 使用TapType的11种类型（类型的长度尽量短小）经过模型推演生成TapTable中的11个字段， 里面包含name和dataType，
     * 同时指定一个单字段（字符串类型， 100的长度）索引和一个两个字段（字符串类型， 100的长度加上一个数字类型）的索引，
     * 不用测试unique， primary和其他的，
     * 采用随机表名建表， 建表成功之后，
     * 通过调用discoverSchema指定tableName来获取随机建立的表， 能查出这个表算是成功，
     * 对比两个TapTable里的索引信息， 不同的地方需要警告。
     * */
    void addIndex(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            this.targetTable = getTable();
            PDKTestBase.TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreatedTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("addIndex");
                execute.testCase(testCase);
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                //采用随机表名建表， 建表成功之后，
                //使用TapType的11种类型（类型的长度尽量短小）经过模型推演生成TapTable中的11个字段， 里面包含name和dataType，
                if(!(hasCreatedTable = super.createTable(prepare,false))){
                    return;
                }
                String tableId = targetTable.getId();
                LinkedHashMap<String, TapField> fieldMap = targetTable.getNameFieldMap();
                if (null == fieldMap || fieldMap.isEmpty()){
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.notFieldMap",tableId))).error(testCase);
                    return;
                }
                TapField string1 = fieldMap.get("TYPE_STRING_1");
                if (null == string1){
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.notSuchField",tableId,"TYPE_STRING_1"))).error(testCase);
                    return;
                }
                TapField string2 = fieldMap.get("TYPE_STRING_2");
                if (null == string2){
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.notSuchField",tableId,"TYPE_STRING_2"))).error(testCase);
                    return;
                }

                TapField int64 = fieldMap.get("TYPE_INT64");
                if (null == int64){
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.notSuchField",tableId,"TYPE_INT64"))).error(testCase);
                    return;
                }

                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions,testCase)){
                    return;
                }
                CreateIndexFunction createIndex = functions.getCreateIndexFunction();
                if (null == createIndex){
                    TapAssert.asserts(()->
                        Assertions.fail(TapSummary.format("createIndex.noiImplement.createIndexFun"))
                    ).warn(testCase);
                    return;
                }
                List<TapIndex> indexList = list(
                        (new TapIndex())
                                .name("index_01")
                                .indexField((new TapIndexField()).name(string1.getName()).fieldAsc(true)),
                        (new TapIndex())
                                .name("index_02")
                                .indexField((new TapIndexField()).name(string2.getName()).fieldAsc(true))
                                .indexField((new TapIndexField()).name(int64.getName()).fieldAsc(false))
                );
                //同时指定一个单字段（字符串类型， 100的长度）索引和一个两个字段（字符串类型， 100的长度加上一个数字类型）的索引，
                TapCreateIndexEvent event = new TapCreateIndexEvent();
                event.indexList(indexList);
                event.setTableId(tableId);
                event.setReferenceTime(System.currentTimeMillis());

                StringJoiner indexStr = new StringJoiner(",");
                indexList.stream().forEach(indexItem->{
                    StringBuilder builder = new StringBuilder("(");
                    String name = indexItem.getName();
                    List<TapIndexField> indexFields = indexItem.getIndexFields();
                    builder.append(name).append(":");
                    StringJoiner joiner = new StringJoiner(",");
                    indexFields.stream().forEach(field-> joiner.add(field.getName()));
                    builder.append(joiner.toString()).append(")");
                    indexStr.add(builder.toString());
                });
               try {
                   createIndex.createIndex(connectorContext,targetTable,event);
                   TapAssert.asserts(()->{}).acceptAsError(testCase,TapSummary.format("createIndex.succeed",indexStr.toString(),tableId));
               }catch (Throwable e){
                   TapAssert.asserts(()->
                       Assertions.fail(TapSummary.format("createIndex.error",indexStr.toString(),tableId))
                   ).error(testCase);
                   return;
               }
                List<TapTable> consumer = new ArrayList<>();
                //通过调用discoverSchema指定tableName来获取随机建立的表， 能查出这个表算是成功，
                final int discoverCount = 1;
                connector.discoverSchema(connectorContext,list(tableId),discoverCount,con->{
                    if (null!=con) consumer.addAll(con);
                });
                if (consumer.size()==1){
                    TapTable tapTable = consumer.get(0);
                    String id = tapTable.getId();
                    if (!tableId.equals(id)){
                        TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.discoverSchema.error",tableId))).warn(testCase);
                        return;
                    }
                    List<TapIndex> indexListAfter = tapTable.getIndexList();

                    //对比两个TapTable里的索引信息， 不同的地方需要警告。
                    super.checkIndex(testCase,indexList,indexListAfter);
                }else {
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.discoverSchema.tooMany.error",discoverCount,consumer.size(),tableId))).error(testCase);
                }
            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (hasCreatedTable){
                    execute.dropTable();
                }
                super.connectorOnStop(prepare);
            }
        });
    }


    @DisplayName("tableIfExist")//用例4， 建表时表是否存在的测试
    @TapTestCase(sort = 4)
    @Test
    /**
     * 随机生成表名， 调用CreateTableV2Function方法进行建表， 返回的CreateTableOptions#tableExists应该是为false，
     * 再次使用相同表名进行建表， 返回的CreateTableOptions#tableExists应该为true，
     * 不通过的时候显示警告， 不做错误处理。
     * */
    void tableIfExist(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("tableIfExist");
                execute.testCase(testCase);
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions,testCase)){
                    return;
                }
                CreateTableV2Function createTableV2 = functions.getCreateTableV2Function();
                if (null==createTableV2){
                    TapAssert.asserts(()->
                        Assertions.assertNotNull(createTableV2, TapSummary.format("createTable.v2Null",targetTable.getId()))
                    ).warn(testCase);
                    return;
                }
                TapCreateTableEvent event = super.modelDeductionForCreateTableEvent(prepare.connectorNode());
                String tableId = targetTable.getId();
                CreateTableOptions table = createTableV2.createTable(connectorContext, event);
                TapAssert.asserts(()->
                    Assertions.assertTrue(null!=table&&!table.getTableExists(),TapSummary.format("tableIfExists.error",tableId))
                ).acceptAsError(testCase,TapSummary.format("tableIfExists.succeed",tableId));
                if (( hasCreateTable = (null!=table&&!table.getTableExists()) )){
                    event.setReferenceTime(System.currentTimeMillis());
                    CreateTableOptions tableAgain = createTableV2.createTable(connectorContext, event);
                    TapAssert.asserts(()->
                        Assertions.assertTrue(null!=tableAgain&&tableAgain.getTableExists(),TapSummary.format("tableIfExists.again.error",tableId))
                    ).acceptAsError(testCase,TapSummary.format("tableIfExists.again.succeed",tableId));
                    prepare.recordEventExecute().dropTable();
                }
            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (hasCreateTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
            //support(DropTableFunction.class,TapSummary.format(inNeedFunFormat,"DropTableFunction")),
            support(CreateIndexFunction.class,TapSummary.format(inNeedFunFormat,"CreateIndexFunction")),
            supportAny(
                    list(WriteRecordFunction.class,CreateTableFunction.class,CreateTableV2Function.class),
                    TapSummary.format(anyOneFunFormat,"WriteRecordFunction,CreateTableFunction,CreateTableV2Function"))
        );
    }
}
