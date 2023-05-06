package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.index.TapDeleteIndexEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapdata.pdk.apis.functions.connector.target.DeleteIndexFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryIndexesFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * 创建索引验证（依赖QueryIndexesFunction和CreateIndexFunction）
 * 创建索引之后查出索引进行验证
 * 测试失败按错误上报
 */
@DisplayName("checkCreateIndex")
@TapGo(tag = "V3", sort = 10080)
public class CheckCreateIndexTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("checkCreateIndex.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(QueryIndexesFunction.class, LangUtil.format(inNeedFunFormat, "QueryIndexesFunction")),
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
                support(CreateIndexFunction.class, LangUtil.format(inNeedFunFormat, "CreateIndexFunction"))
        );
    }

    /**
     * 用例1，创建唯一索引
     * 利用CreateIndexFunction创建唯一的索引，
     * 不用指定索引字段的升序降序， 唯一主键等情况，
     * 创建之后再通过QueryIndexesFunction查询出创建的索引， 进行验证。
     * 如果实现了WriteRecordFunction， 创建两个唯一索引相同的数据， 应该能报错
     */
    @DisplayName("checkCreateIndex.unique")
    @TapTestCase(sort = 1)
    @Test
    public void unique() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("checkCreateIndex.unique.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            hasCreatedTable.set(super.createTable(node));
            if (!hasCreatedTable.get()) {
                // 建表失败;
                return;
            }

            ConnectorNode connectorNode = node.connectorNode();
            TapTable targetTableModel = super.getTargetTable(connectorNode);
            ConnectorFunctions functions = connectorNode.getConnectorFunctions();
            CreateIndexFunction indexFunction = functions.getCreateIndexFunction();
            List<TapIndex> indexList = new ArrayList<>();
            final String indexName = "CREATE_INDEX_TEST";
            final String fieldName = "TYPE_STRING_1";
            TapIndexField indexField = new TapIndexField().name(fieldName);
            TapIndex tapIndex1 = new TapIndex()
                    .name(indexName)
                    .indexField(indexField)
                    .unique(true);
            indexList.add(tapIndex1);
            TapCreateIndexEvent indexEvent = new TapCreateIndexEvent().indexList(indexList);
            try {
                indexFunction.createIndex(connectorNode.getConnectorContext(), targetTableModel, indexEvent);
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.create.fail", throwable.getMessage()));
            }

            QueryIndexesFunction query = functions.getQueryIndexesFunction();
            List<TapIndex> indexArr = new ArrayList<>();
            try {
                query.query(connectorNode.getConnectorContext(), targetTableModel, consumer -> {
                    for (TapIndex tapIndex : consumer) {
                        if (Objects.nonNull(tapIndex)) {
                            indexArr.add(tapIndex);
                        }
                    }
                });
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.query.fail", throwable.getMessage()));
            }

            boolean indexTrue = true;
            StringBuilder builder1 = new StringBuilder();
            StringBuilder builder2 = new StringBuilder();
            for (TapIndex tapIndex : indexArr) {
                String name = tapIndex.getName();
                if (tapIndex1.getName().equals(name)) {
                    Boolean unique = tapIndex.getUnique();
                    List<TapIndexField> indexFields = tapIndex.getIndexFields();
                    if (null == unique || unique != tapIndex1.getUnique()) {
                        indexTrue = false;
                    }
                    builder1.append("Index: ").append(name).append(" | ")
                            .append(" Field: [name: ").append(indexField.getName()).append(" | fieldAsc: ").append(indexField.getFieldAsc()).append(" ]");
                    builder2.append("Index: ").append(tapIndex1.getName()).append(" | ");
                    if (null == indexFields || indexFields.isEmpty()) {
                        indexTrue = false;
                        builder2.append(" Field: [ ]");
                    } else {
                        if (indexFields.size() > 1) {
                            indexTrue = false;
                            StringJoiner joiner = new StringJoiner(", ");
                            for (TapIndexField field : indexFields) {
                                String name1 = field.getName();
                                Boolean fieldAsc = field.getFieldAsc();
                                if (!indexField.getName().equals(name1)) {
                                    indexTrue = false;
                                }
                                if (!indexField.getFieldAsc().equals(fieldAsc)) {
                                    indexTrue = false;
                                }
                                joiner.add(" Field: [name: " + field.getName() + " | fieldAsc: " + field.getFieldAsc() + " ]");
                            }
                            builder2.append(joiner.toString());
                        } else {
                            TapIndexField field = indexFields.get(0);
                            String name1 = field.getName();
                            Boolean fieldAsc = field.getFieldAsc();
                            if (!indexField.getName().equals(name1)) {
                                indexTrue = false;
                            }
                            if (!indexField.getFieldAsc().equals(fieldAsc)) {
                                indexTrue = false;
                            }
                            builder2.append(" Field: [name: ").append(field.getName()).append(" | fieldAsc: ").append(field.getFieldAsc()).append(" ]");
                        }
                    }
                    break;
                }
            }
            if ((!indexTrue) && "".equals(builder1.toString()) && builder2.toString().equals("")) {
                TapAssert.succeed(testCase, langUtil.formatLang("checkCreateIndex.unique.create.succeed", builder1.toString(), builder2.toString()));
                return;
            } else {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.create.error", builder1.toString(), builder2.toString()));
            }

            RecordEventExecute recordEventExecute = node.recordEventExecute();
            WriteListResult<TapRecordEvent> insert = null;
            final int insertCount = 1;
            Record[] records = Record.testRecordWithTapTable(targetTable, insertCount);
            recordEventExecute.builderRecordCleanBefore(records);
            try {
                insert = recordEventExecute.insert();
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.insert.throw", insertCount, throwable.getMessage()));
                return;
            }
            if (Objects.isNull(insert) || insert.getInsertedCount() != insertCount) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.insert.error",
                        insertCount,
                        null == insert ? 0 : insert.getInsertedCount(),
                        null == insert ? 0 : insert.getModifiedCount(),
                        null == insert ? 0 : insert.getRemovedCount()
                ));
            } else {
                TapAssert.succeed(testCase, langUtil.formatLang("checkCreateIndex.unique.insert.succeed",
                        insertCount,
                        insert.getInsertedCount(),
                        insert.getModifiedCount(),
                        insert.getRemovedCount()));
                return;
            }
            insert = null;
            try {
                insert = recordEventExecute.insert();
            } catch (Throwable throwable) {
                TapAssert.warn(testCase, langUtil.formatLang("checkCreateIndex.unique.insert1.throw", insertCount, throwable.getMessage()));
                return;
            }
            if (null == insert || (insert.getInsertedCount() == 0 && insert.getModifiedCount() == 0 && insert.getRemovedCount() == 0)) {
                TapAssert.succeed(testCase, langUtil.formatLang("checkCreateIndex.unique.insert1.succeed",
                        insertCount,
                        null == insert ? 0 : insert.getInsertedCount(),
                        null == insert ? 0 : insert.getModifiedCount(),
                        null == insert ? 0 : insert.getRemovedCount()
                ));
            } else {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.insert1.error",
                        insertCount,
                        insert.getInsertedCount(),
                        insert.getModifiedCount(),
                        insert.getRemovedCount()));
            }

        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }

    /**
     * 用例2，创建升序和降序的索引
     * 利用CreateIndexFunction创建的索引， 指定多个索引字段，
     * 覆盖升序和降序的场景，
     * 创建之后再通过QueryIndexesFunction查询出创建的索引， 进行验证。
     */
    @DisplayName("checkCreateIndex.ascDesc")
    @TapTestCase(sort = 2)
    @Test
    public void ascDesc() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("checkCreateIndex.ascDesc.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            hasCreatedTable.set(super.createTable(node));
            if (!hasCreatedTable.get()) {
                // 建表失败;
                return;
            }

            ConnectorNode connectorNode = node.connectorNode();
            ConnectorFunctions functions = connectorNode.getConnectorFunctions();
            CreateIndexFunction indexFunction = functions.getCreateIndexFunction();
            List<TapIndex> indexList = new ArrayList<>();
            final String indexName = "CREATE_INDEX_TEST_DESC";
            final String fieldName = "TYPE_STRING_1";
            TapIndexField indexField = new TapIndexField().name(fieldName).fieldAsc(false);
            TapIndex tapIndex1 = new TapIndex()
                    .name(indexName)
                    .indexField(indexField)
                    .unique(true);
            indexList.add(tapIndex1);

            final String indexName1 = "CREATE_INDEX_TEST_ASC";
            final String fieldName1 = "TYPE_STRING_2";
            TapIndexField indexField1 = new TapIndexField().name(fieldName1).fieldAsc(true);
            TapIndex tapIndex2 = new TapIndex()
                    .name(indexName1)
                    .indexField(indexField1)
                    .unique(true);
            indexList.add(tapIndex2);
            TapCreateIndexEvent indexEvent = new TapCreateIndexEvent().indexList(indexList);
            try {
                indexFunction.createIndex(connectorNode.getConnectorContext(), targetTable, indexEvent);
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.create.fail", throwable.getMessage()));
            }
            TapTable targetTableModel = super.getTargetTable(connectorNode);
            QueryIndexesFunction query = functions.getQueryIndexesFunction();
            List<TapIndex> indexArr = new ArrayList<>();
            try {
                query.query(connectorNode.getConnectorContext(), targetTableModel, consumer -> {
                    for (int index = 0; index < consumer.size(); index++) {
                        TapIndex tapIndex = consumer.get(index);
                        if (Objects.nonNull(tapIndex)) {
                            indexArr.add(tapIndex);
                        }
                    }
                });
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.query.fail", throwable.getMessage()));
            }

            boolean indexTrue = true;
            StringBuilder builder1 = new StringBuilder();
            StringBuilder builder2 = new StringBuilder();
            for (TapIndex tapIndex : indexArr) {
                String name = tapIndex.getName();
                if (tapIndex1.getName().equals(name)) {

                    Boolean unique = tapIndex.getUnique();
                    List<TapIndexField> indexFields = tapIndex.getIndexFields();
                    if (null == unique || unique != tapIndex1.getUnique()) {
                        indexTrue = false;
                    }
                    builder1.append(" Index: ").append(name).append(" | ")
                            .append(" Field: [name: ").append(indexField.getName()).append(" | fieldAsc: ").append(indexField.getFieldAsc()).append(" ]");
                    builder2.append(" Index: ").append(tapIndex1.getName()).append(" | ");
                    if (null == indexFields || indexFields.isEmpty()) {
                        indexTrue = false;
                        builder2.append(" Field: [ ]");
                    } else {
                        if (indexFields.size() > 1) {
                            indexTrue = false;
                            StringJoiner joiner = new StringJoiner(", ");
                            for (int i = 0; i < indexFields.size(); i++) {
                                TapIndexField field = indexFields.get(i);
                                String name1 = field.getName();
                                Boolean fieldAsc = field.getFieldAsc();
                                if (!indexField.getName().equals(name1)) {
                                    indexTrue = false;
                                }
                                if (!indexField.getFieldAsc().equals(fieldAsc)) {
                                    indexTrue = false;
                                }
                                joiner.add(" Field: [name: " + field.getName() + " | fieldAsc: " + field.getFieldAsc() + " ]");
                            }
                            builder2.append(joiner.toString());
                        } else {
                            TapIndexField field = indexFields.get(0);
                            String name1 = field.getName();
                            Boolean fieldAsc = field.getFieldAsc();
                            if (!indexField.getName().equals(name1)) {
                                indexTrue = false;
                            }
                            if (!indexField.getFieldAsc().equals(fieldAsc)) {
                                indexTrue = false;
                            }
                            builder2.append(" Field: [name: ").append(field.getName()).append(" | fieldAsc: ").append(field.getFieldAsc()).append(" ]");
                        }
                    }
                    break;
                }
                if (tapIndex2.getName().equals(name)) {
                    Boolean unique = tapIndex.getUnique();
                    List<TapIndexField> indexFields = tapIndex.getIndexFields();
                    if (null == unique || unique != tapIndex2.getUnique()) {
                        indexTrue = false;
                    }
                    builder1.append(" Index: ").append(name).append(" | ")
                            .append(" Field: [name: ").append(indexField1.getName()).append(" | fieldAsc: ").append(indexField1.getFieldAsc()).append(" ]");
                    builder2.append(" Index: ").append(tapIndex2.getName()).append(" | ");
                    if (null == indexFields || indexFields.isEmpty()) {
                        indexTrue = false;
                        builder2.append(" Field: [ ]");
                    } else {
                        if (indexFields.size() > 1) {
                            indexTrue = false;
                            StringJoiner joiner = new StringJoiner(", ");
                            for (int i = 0; i < indexFields.size(); i++) {
                                TapIndexField field = indexFields.get(i);
                                String name1 = field.getName();
                                Boolean fieldAsc = field.getFieldAsc();
                                if (!indexField1.getName().equals(name1)) {
                                    indexTrue = false;
                                }
                                if (!indexField1.getFieldAsc().equals(fieldAsc)) {
                                    indexTrue = false;
                                }
                                joiner.add(" Field: [name: " + field.getName() + " | fieldAsc: " + field.getFieldAsc() + " ]");
                            }
                            builder2.append(joiner.toString());
                        } else {
                            TapIndexField field = indexFields.get(0);
                            String name1 = field.getName();
                            Boolean fieldAsc = field.getFieldAsc();
                            if (!indexField1.getName().equals(name1)) {
                                indexTrue = false;
                            }
                            if (!indexField1.getFieldAsc().equals(fieldAsc)) {
                                indexTrue = false;
                            }
                            builder2.append(" Field: [name: ").append(field.getName()).append(" | fieldAsc: ").append(field.getFieldAsc()).append(" ]");
                        }
                    }
                    break;
                }
            }
            if ((!indexTrue) && "".equals(builder1.toString()) && builder2.toString().equals("")) {
                TapAssert.succeed(testCase, langUtil.formatLang("checkCreateIndex.unique.create.succeed", builder1.toString(), builder2.toString()));
            } else {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.create.error", builder1.toString(), builder2.toString()));
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }

    /**
     * 用例3，删除索引验证
     */
    @DisplayName("checkCreateIndex.delete")
    @TapTestCase(sort = 3)
    @Test
    public void delete() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("checkCreateIndex.delete.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            hasCreatedTable.set(super.createTable(node));
            if (!hasCreatedTable.get()) {
                // 建表失败;
                return;
            }

            ConnectorNode connectorNode = node.connectorNode();
            ConnectorFunctions functions = connectorNode.getConnectorFunctions();
            CreateIndexFunction indexFunction = functions.getCreateIndexFunction();
            List<TapIndex> indexList = new ArrayList<>();
            final String indexName = "CREATE_INDEX_TEST";
            final String fieldName = "TYPE_STRING_1";
            TapIndexField indexField = new TapIndexField().name(fieldName);
            TapIndex tapIndex1 = new TapIndex()
                    .name(indexName)
                    .indexField(indexField)
                    .unique(true);
            indexList.add(tapIndex1);
            TapCreateIndexEvent indexEvent = new TapCreateIndexEvent().indexList(indexList);
            TapTable targetTableModel = super.getTargetTable(connectorNode);
            try {
                indexFunction.createIndex(connectorNode.getConnectorContext(), targetTableModel, indexEvent);
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.create.fail", throwable.getMessage()));
            }

            QueryIndexesFunction query = functions.getQueryIndexesFunction();
            List<TapIndex> indexArr = new ArrayList<>();
            try {
                query.query(connectorNode.getConnectorContext(), targetTableModel, consumer -> {
                    for (int index = 0; index < consumer.size(); index++) {
                        TapIndex tapIndex = consumer.get(index);
                        if (Objects.nonNull(tapIndex)) {
                            indexArr.add(tapIndex);
                        }
                    }
                });
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.query.fail", throwable.getMessage()));
            }

            boolean indexTrue = true;
            StringBuilder builder1 = new StringBuilder();
            StringBuilder builder2 = new StringBuilder();
            for (TapIndex tapIndex : indexArr) {
                String name = tapIndex.getName();
                if (tapIndex1.getName().equals(name)) {
                    Boolean unique = tapIndex.getUnique();
                    List<TapIndexField> indexFields = tapIndex.getIndexFields();
                    if (null == unique || unique != tapIndex1.getUnique()) {
                        indexTrue = false;
                    }
                    builder1.append("Index: ").append(name).append(" | ")
                            .append(" Field: [name: ").append(indexField.getName()).append(" | fieldAsc: ").append(indexField.getFieldAsc()).append(" ]");
                    builder2.append("Index: ").append(tapIndex1.getName()).append(" | ");
                    if (null == indexFields || indexFields.isEmpty()) {
                        indexTrue = false;
                        builder2.append(" Field: [ ]");
                    } else {
                        if (indexFields.size() > 1) {
                            indexTrue = false;
                            StringJoiner joiner = new StringJoiner(", ");
                            for (int i = 0; i < indexFields.size(); i++) {
                                TapIndexField field = indexFields.get(i);
                                String name1 = field.getName();
                                Boolean fieldAsc = field.getFieldAsc();
                                if (!indexField.getName().equals(name1)) {
                                    indexTrue = false;
                                }
                                if (!indexField.getFieldAsc().equals(fieldAsc)) {
                                    indexTrue = false;
                                }
                                joiner.add(" Field: [name: " + field.getName() + " | fieldAsc: " + field.getFieldAsc() + " ]");
                            }
                            builder2.append(joiner.toString());
                        } else {
                            TapIndexField field = indexFields.get(0);
                            String name1 = field.getName();
                            Boolean fieldAsc = field.getFieldAsc();
                            if (!indexField.getName().equals(name1)) {
                                indexTrue = false;
                            }
                            if (!indexField.getFieldAsc().equals(fieldAsc)) {
                                indexTrue = false;
                            }
                            builder2.append(" Field: [name: ").append(field.getName()).append(" | fieldAsc: ").append(field.getFieldAsc()).append(" ]");
                        }
                    }
                    break;
                }
            }
            if ((!indexTrue) && "".equals(builder1.toString()) && builder2.toString().equals("")) {
                TapAssert.succeed(testCase, langUtil.formatLang("checkCreateIndex.unique.create.error", builder1.toString(), builder2.toString()));
                return;
            } else {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.create.succeed", builder1.toString(), builder2.toString()));
            }

            RecordEventExecute recordEventExecute = node.recordEventExecute();
            WriteListResult<TapRecordEvent> insert = null;
            final int insertCount = 1;
            targetTableModel = super.getTargetTable(connectorNode);
            Record[] records = Record.testRecordWithTapTable(targetTableModel, insertCount);
            recordEventExecute.builderRecordCleanBefore(records);
            try {
                insert = recordEventExecute.insert();
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.insert.throw", insertCount, throwable.getMessage()));
                return;
            }
            if (Objects.isNull(insert) || insert.getInsertedCount() != insertCount) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.insert.error",
                        insertCount,
                        null == insert ? 0 : insert.getInsertedCount(),
                        null == insert ? 0 : insert.getModifiedCount(),
                        null == insert ? 0 : insert.getRemovedCount()
                ));
            } else {
                TapAssert.succeed(testCase, langUtil.formatLang("checkCreateIndex.unique.insert.succeed",
                        insertCount,
                        insert.getInsertedCount(),
                        insert.getModifiedCount(),
                        insert.getRemovedCount()));
                return;
            }

            //删除索引
            DeleteIndexFunction deleteIndex = functions.getDeleteIndexFunction();
            TapDeleteIndexEvent deleteEvent = new TapDeleteIndexEvent();
            targetTableModel = super.getTargetTable(connectorNode);
            try {
                if (Objects.isNull(deleteIndex))
                    throw new RuntimeException("Not implements DeleteIndexFunction, try again after implemented this function in connector please.");
                deleteIndex.deleteIndex(node.connectorNode().getConnectorContext(), targetTableModel, deleteEvent);
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.index.delete.throw", throwable.getMessage()));
                return;
            }

            List<TapIndex> indexArr1 = new ArrayList<>();
            try {
                query.query(connectorNode.getConnectorContext(), targetTableModel, consumer -> {
                    for (TapIndex tapIndex : consumer) {
                        if (Objects.nonNull(tapIndex)) {
                            indexArr1.add(tapIndex);
                        }
                    }
                });
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.unique.query.fail", throwable.getMessage()));
            }
            indexTrue = true;
            for (TapIndex tapIndex : indexArr1) {
                String name = tapIndex.getName();
                if (tapIndex1.getName().equals(name)) {
                    indexTrue = false;
                    break;
                }
            }
            if (indexTrue) {
                TapAssert.succeed(testCase, langUtil.formatLang("checkCreateIndex.delete.succeed"));
            } else {
                TapAssert.error(testCase, langUtil.formatLang("checkCreateIndex.delete.fail"));
            }
        });
    }
}