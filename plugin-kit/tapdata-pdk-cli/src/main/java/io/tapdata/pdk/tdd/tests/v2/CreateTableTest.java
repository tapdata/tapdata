package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Date;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            try {
                PDKInvocationMonitor.invoke(prepare.connectorNode(),
                        PDKMethod.INIT,
                        prepare.connectorNode()::connectorInit,
                        "Init PDK","TEST mongodb"
                );
                Method testCase = super.getMethod("createTableV2");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();

                if (super.verifyFunctions(functions,testCase)){
                    return;
                }

                CreateTableFunction createTable = functions.getCreateTableFunction();
                TapAssert.asserts(()->
                        Assertions.assertNotNull(createTable, TapSummary.format("createTable.null"))
                ).acceptAsWarn(testCase,TapSummary.format("createTable.notNull"));


                CreateTableV2Function createTableV2 = functions.getCreateTableV2Function();
                TapAssert.asserts(()->
                        Assertions.assertNotNull(createTableV2, TapSummary.format("createTable.v2Null"))
                ).acceptAsWarn(testCase,TapSummary.format("createTable.v2NotNull"));

                boolean isV1 = null != createTable;
                boolean isV2 = null != createTableV2;

                if (!isV1 && !isV2){
                    return;
                }
                String tableId = UUID.randomUUID().toString();
                targetTable.setId(tableId);
                TapCreateTableEvent tableEvent = new TapCreateTableEvent().table(targetTable);
                //如果同时实现了两个方法只需要测试CreateTableV2Function。(V2优先)
                if (isV2){
                    CreateTableOptions table = createTableV2.createTable(
                            connectorContext,
                            tableEvent
                    );
                    this.verifyTableIsCreated("CreateTableV2Function",prepare);
                    return;
                }
                //检查如果只实现了CreateTableFunction，没有实现CreateTableV2Function时，报出警告，
                //推荐使用CreateTableV2Function方法来实现建表
                createTable.createTable(connectorContext, tableEvent);
                if (this.verifyTableIsCreated("CreateTableFunction", prepare)) {
                    prepare.recordEventExecute().dropTable();
                }
            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (null != prepare.connectorNode()){
                    PDKInvocationMonitor.invoke(prepare.connectorNode(),
                            PDKMethod.STOP,
                            prepare.connectorNode()::connectorStop,
                            "Stop PDK",
                            "TEST mongodb"
                    );
                    PDKIntegration.releaseAssociateId("releaseAssociateId");
                }
            }
        });
    }

    boolean verifyTableIsCreated(String createMethod,TestNode prepare) throws Throwable {
        ConnectorNode connectorNode = prepare.connectorNode();
        TapConnector connector = connectorNode.getConnector();
        TapConnectorContext connectorContext = connectorNode.getConnectorContext();
        String tableIdTarget = targetTable.getId();
        Method testBase = prepare.recordEventExecute().testCase();
        List<TapTable> consumer = new ArrayList<>();
        connector.discoverSchema(connectorContext,list(tableIdTarget),1000,con->{
            if(null!=con) consumer.addAll(con);
        });
        TapAssert.asserts(()->{
            Assertions.assertFalse(consumer.isEmpty(), TapSummary.format("verifyTableIsCreated.error", createMethod, tableIdTarget));
        }).acceptAsError(testBase,TapSummary.format("verifyTableIsCreated.succeed",createMethod,tableIdTarget));
        return !consumer.isEmpty();
    }

    TapTable getTableForAllTapType(){
        return table(UUID.randomUUID().toString())
                .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1))
                .add(field("TYPE_ARRAY", JAVA_Array))
                .add(field("TYPE_BINARY", JAVA_Binary))
                .add(field("TYPE_BOOLEAN", JAVA_Boolean))
                .add(field("TYPE_DATE", JAVA_Date))
                .add(field("TYPE_DATETIME", JAVA_Date))
                .add(field("TYPE_MAP", JAVA_Map))
                .add(field("TYPE_NUMBER_Long", JAVA_Long))
                .add(field("TYPE_NUMBER_INTEGER", JAVA_Integer))
                .add(field("TYPE_NUMBER_BigDecimal", JAVA_BigDecimal))
                .add(field("TYPE_NUMBER_Float", JAVA_Float))
                .add(field("TYPE_NUMBER_Double", JAVA_Double))
                .add(field("TYPE_STRING_1", "STRING(20)"))
                .add(field("TYPE_STRING_2", "STRING(20)"))
                .add(field("TYPE_INT64", "INT64"))
                .add(field("TYPE_TIME", JAVA_Date))
                .add(field("TYPE_YEAR", JAVA_Date));
    }


    private TableFieldTypesGenerator tableFieldTypesGenerator;
    private TargetTypesGenerator targetTypesGenerator;
    private TapCodecsFilterManager targetCodecFilterManager;
    private TapCodecsRegistry codecRegistry;
    final String types = "{\n" +
            "    \"DOUBLE\": {\n" +
            "      \"to\": \"TapNumber\",\n" +
            "      \"value\": [\n" +
            "        \"-1.7976931348623157E+308\",\n" +
            "        \"1.7976931348623157E+308\"\n" +
            "      ],\n" +
            "      \"preferPrecision\": 20,\n" +
            "      \"preferScale\": 8,\n" +
            "      \"scale\": 17,\n" +
            "      \"precision\": 309,\n" +
            "      \"fixed\": true\n" +
            "    },\n" +
            "    \"STRING[($byte)]\": {\n" +
            "      \"to\": \"TapString\",\n" +
            "      \"preferByte\": \"100\",\n" +
            "      \"byte\": \"16m\"\n" +
            "    },\n" +
            "    \"DOCUMENT\": {\n" +
            "      \"to\": \"TapMap\",\n" +
            "      \"byte\": \"16m\"\n" +
            "    },\n" +
            "    \"ARRAY\": {\n" +
            "      \"to\": \"TapArray\",\n" +
            "      \"byte\": \"16m\"\n" +
            "    },\n" +
            "    \"BINARY\": {\n" +
            "      \"to\": \"TapBinary\",\n" +
            "      \"byte\": \"16m\"\n" +
            "    },\n" +
            "    \"OBJECT_ID\": {\n" +
            "      \"to\": \"TapString\",\n" +
            "      \"byte\": \"24\",\n" +
            "      \"queryOnly\": true\n" +
            "    },\n" +
            "    \"BOOLEAN\": {\n" +
            "      \"to\": \"TapBoolean\"\n" +
            "    },\n" +
            "    \"DATE_TIME\": {\n" +
            "      \"to\": \"TapDateTime\",\n" +
            "      \"range\": [\n" +
            "        \"1000-01-01T00:00:00.001Z\",\n" +
            "        \"9999-12-31T23:59:59.999Z\"\n" +
            "      ],\n" +
            "      \"pattern\": \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\",\n" +
            "      \"fraction\": [\n" +
            "        0,\n" +
            "        3\n" +
            "      ],\n" +
            "      \"defaultFraction\": 3\n" +
            "    },\n" +
            "    \"JAVASCRIPT\": {\n" +
            "      \"to\": \"TapString\",\n" +
            "      \"byte\": \"16m\",\n" +
            "      \"queryOnly\": true\n" +
            "    },\n" +
            "    \"SYMBOL\": {\n" +
            "      \"to\": \"TapString\",\n" +
            "      \"byte\": \"16m\",\n" +
            "      \"queryOnly\": true\n" +
            "    },\n" +
            "    \"INT32\": {\n" +
            "      \"to\": \"TapNumber\",\n" +
            "      \"bit\": 32,\n" +
            "      \"precision\": 10,\n" +
            "      \"value\": [\n" +
            "        -2147483648,\n" +
            "        2147483647\n" +
            "      ]\n" +
            "    },\n" +
            "    \"TIMESTAMP\": {\n" +
            "      \"to\": \"TapString\",\n" +
            "      \"queryOnly\": true\n" +
            "    },\n" +
            "    \"INT64\": {\n" +
            "      \"to\": \"TapNumber\",\n" +
            "      \"bit\": 64,\n" +
            "      \"value\": [\n" +
            "        -9223372036854775808,\n" +
            "        9223372036854775807\n" +
            "      ]\n" +
            "    },\n" +
            "    \"DECIMAL128\": {\n" +
            "      \"to\": \"TapNumber\",\n" +
            "      \"value\": [\n" +
            "        -1E+6145,\n" +
            "        1E+6145\n" +
            "      ],\n" +
            "      \"scale\": 1000\n" +
            "    },\n" +
            "    \"MIN_KEY\": {\n" +
            "      \"to\": \"TapString\",\n" +
            "      \"byte\": \"16m\",\n" +
            "      \"queryOnly\": true\n" +
            "    },\n" +
            "    \"MAX_KEY\": {\n" +
            "      \"to\": \"TapString\",\n" +
            "      \"byte\": \"16m\",\n" +
            "      \"queryOnly\": true\n" +
            "    },\n" +
            "    \"NULL\": {\n" +
            "      \"to\": \"TapRaw\"\n" +
            "    }\n" +
            "  }";
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
            //使用TapType的11种类型组织表结构（类型的长度尽量短小）
            this.targetTable = getTableForAllTapType();
            RecordEventExecute execute = prepare.recordEventExecute();
            targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
            if(targetTypesGenerator == null)
                throw new CoreException(PDKRunnerErrorCodes.SOURCE_TARGET_TYPES_GENERATOR_NOT_FOUND, "TargetTypesGenerator's implementation is not found in current classloader");
            tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
            if(tableFieldTypesGenerator == null)
                throw new CoreException(PDKRunnerErrorCodes.SOURCE_TABLE_FIELD_TYPES_GENERATOR_NOT_FOUND, "TableFieldTypesGenerator's implementation is not found in current classloader");
            codecRegistry = TapCodecsRegistry.create();
            targetCodecFilterManager = TapCodecsFilterManager.create(codecRegistry);
            boolean hasCreateTable = false;
            try {
                Method testCase = super.getMethod("allTapType");
                execute.testCase(testCase);

                tableFieldTypesGenerator.autoFill(
                        targetTable.getNameFieldMap(),
                        DefaultExpressionMatchingMap.map(types)
                );
                TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(
                        targetTable.getNameFieldMap(),
                        DefaultExpressionMatchingMap.map(types),
                        targetCodecFilterManager
                );
                //经过模型推演生成TapTable中的11个字段，
                LinkedHashMap<String, TapField> sourceFields = tapResult.getData();

                super.connectorOnStart(prepare);
                //采用随机表名建表， 建表成功之后， 返回的CreateTableOptions#tableExists应该等于false，
                if (!(hasCreateTable = super.createTable(prepare))){
                    return;
                }
                String tableId = targetTable.getId();
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                //如果没有就警告； 通过调用discoverSchema指定tableName来获取随机建立的表， 能查出这个表算是成功，
                List<TapTable> tables = new ArrayList<>();
                connector.discoverSchema(connectorContext,list(tableId),1,con->{
                    if (null!=con&&!con.isEmpty()){
                        tables.addAll(con);
                    }
                });
                if (tables.size()==1){
                    TapTable tapTable = tables.get(0);
                    TapAssert.asserts(()->{
                        Assertions.assertEquals(tapTable.getId(), tableId, TapSummary.format(""));
                    }).acceptAsError(testCase,TapSummary.format(""));
                    if (tableId.equals(tapTable.getId())){
                        //对比两个TapTable里的字段名以及类型， 不同的地方需要警告。
                        LinkedHashMap<String, TapField> targetFields = tapTable.getNameFieldMap();
                        super.contrastTableFieldNameAndType(testCase,sourceFields,targetFields);
                    }
                }else {
                    TapAssert.asserts(()->{
                        Assertions.fail(TapSummary.format(""));
                    }).acceptAsError(testCase,null);
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
                .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1))
                .add(field("TYPE_ARRAY", JAVA_Array))
                .add(field("TYPE_BINARY", JAVA_Binary))
                .add(field("TYPE_BOOLEAN", JAVA_Boolean))
                .add(field("TYPE_DATE", JAVA_Date))
                .add(field("TYPE_DATETIME", JAVA_Date))
                .add(field("TYPE_MAP", JAVA_Map))
                .add(field("TYPE_NUMBER_Long", JAVA_Long))
                .add(field("TYPE_NUMBER_INTEGER", JAVA_Integer))
                .add(field("TYPE_NUMBER_BigDecimal", JAVA_BigDecimal))
                .add(field("TYPE_NUMBER_Float", JAVA_Float))
                .add(field("TYPE_NUMBER_Double", JAVA_Double))
                .add(field("TYPE_STRING_1", "STRING(100)"))
                .add(field("TYPE_STRING_2", "STRING(100)"))
                .add(field("TYPE_INT64", "INT64"))
                .add(field("TYPE_TIME", JAVA_Date))
                .add(field("TYPE_YEAR", JAVA_Date));
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
                if(!(hasCreatedTable = super.createTable(prepare))){
                    return;
                }
                String tableId = targetTable.getId();
                LinkedHashMap<String, TapField> fieldMap = targetTable.getNameFieldMap();
                if (null == fieldMap || fieldMap.isEmpty()){
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.notFieldMap",tableId))).acceptAsError(testCase,null);
                    return;
                }
                TapField string1 = fieldMap.get("TYPE_STRING_1");
                if (null == string1){
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.notSuchField",tableId,"TYPE_STRING_1"))).acceptAsError(testCase,null);
                    return;
                }
                TapField string2 = fieldMap.get("TYPE_STRING_2");
                if (null == string2){
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.notSuchField",tableId,"TYPE_STRING_2"))).acceptAsError(testCase,null);
                    return;
                }

                TapField int64 = fieldMap.get("TYPE_INT64");
                if (null == int64){
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.notSuchField",tableId,"TYPE_INT64"))).acceptAsError(testCase,null);
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
                    ).acceptAsError(testCase,null);
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
                   ).acceptAsError(testCase,null);
                   return;
               }
//                indexList.add((new TapIndex())
//                        .name("index_03")
//                        .indexField((new TapIndexField()).name(string1.getName()).fieldAsc(true)));
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
                        TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.discoverSchema.error",tableId))).acceptAsError(testCase,null);
                        return;
                    }
                    List<TapIndex> indexListAfter = tapTable.getIndexList();

                    //对比两个TapTable里的索引信息， 不同的地方需要警告。
                    super.checkIndex(testCase,indexList,indexListAfter);
                }else {
                    TapAssert.asserts(()->Assertions.fail(TapSummary.format("createIndex.discoverSchema.tooMany.error",discoverCount,consumer.size(),tableId))).acceptAsError(testCase,null);
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
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("tableIfExist");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();

                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions,testCase)){
                    return;
                }
                CreateTableV2Function createTableV2 = functions.getCreateTableV2Function();
                if (null==createTableV2){
                    TapAssert.asserts(()->
                        Assertions.assertNotNull(createTableV2, TapSummary.format("createTable.v2Null"))
                    ).acceptAsWarn(testCase,null);
                    return;
                }
                TapCreateTableEvent event = new TapCreateTableEvent();
                String tableId = targetTable.getId();
                event.table(targetTable);
                event.setReferenceTime(System.currentTimeMillis());
                CreateTableOptions table = createTableV2.createTable(connectorContext, event);
                TapAssert.asserts(()->
                    Assertions.assertTrue(null!=table&&!table.getTableExists(),TapSummary.format("tableIfExists.error",tableId))
                ).acceptAsError(testCase,TapSummary.format("tableIfExists.succeed",tableId));
                if (null!=table&&!table.getTableExists()){
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
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
            support(DropTableFunction.class,TapSummary.format(inNeedFunFormat,"DropTableFunction")),
            support(CreateIndexFunction.class,TapSummary.format(inNeedFunFormat,"CreateIndexFunction")),
            supportAny(
                    list(WriteRecordFunction.class,CreateTableFunction.class,CreateTableV2Function.class),
                    TapSummary.format(anyOneFunFormat,"WriteRecordFunction,CreateTableFunction,CreateTableV2Function"))
        );
    }
}
