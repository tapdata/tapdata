package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.support.DataTypesHandler;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.list;

//数据类型建表（依赖CreateTableFunction）

/**
 * 验证开发者描述的每个字段类型都是可以成功建表的，
 * 这里在不同的数据库版本的时候很容易出现类型不兼容的错误
 * 测试失败按警告上报
 */
@DisplayName("createTableUsingField")
@TapGo(tag = "V3", sort = 10010, debug = false)
public class CreateTableUsingFieldTypesInTurnTest extends PDKTestBaseV2 {
    public static List<SupportFunction> testFunctions() {
        return list(supportAny(
                langUtil.formatLang(anyOneFunFormat, "CreateTableFunction,CreateTableV2Function"),
                CreateTableFunction.class, CreateTableV2Function.class)
        );
    }


    /**
     * 用例1， 尝试每个字段是否建表成功
     * 从spec.json里的dataTypes里读取所有的类型字段，
     * 有变量的类型取最大值和最小值，
     * 那每个类型去建表，
     * 建表失败算用例失败
     */
    @DisplayName("createTableUsingField.all")
    @TapTestCase(sort = 1)
    @Test
    public void createTableUsingFieldTypesInTurn() throws NoSuchMethodException {
        super.execTest((node, testCase) -> {
            TapNodeSpecification tapNodeSpecification = node.nodeInfo().getTapNodeSpecification();
            DefaultExpressionMatchingMap dataTypesMap = tapNodeSpecification.getDataTypesMap();
            if (Objects.isNull(dataTypesMap) || dataTypesMap.isEmpty()) {
                TapAssert.error(testCase, langUtil.formatLang("createTableUsingField.notDataTypes"));
                return;
            }
            RecordEventExecute execute = node.recordEventExecute();
            DataTypesHandler handler = DataTypesHandler.create();
            try {
                dataTypesMap.iterate(entry -> {
                    TapTable tapTable = new TapTable(super.testTableId, super.testTableId);
                    String typeName = entry.getKey();
                    DataMap typeConfig = entry.getValue();
                    Object queryOnlyObj = Optional.ofNullable(typeConfig.get("queryOnly")).orElse(Boolean.FALSE);
                    if (!((queryOnlyObj instanceof Boolean) && (Boolean) queryOnlyObj)) {
                        List<TapTable> tapTables = new ArrayList<>();
                        TapMapping tapMapping = (TapMapping) typeConfig.get(TapMapping.FIELD_TYPE_MAPPING);
                        handler.fillTestFields(tapTable, typeName, tapMapping);
                        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
                        if (nameFieldMap.size() > 1) {
                            nameFieldMap.forEach((name, f) -> {
                                TapTable table = new TapTable(super.testTableId, super.testTableId);
                                LinkedHashMap<String, TapField> subFieldMap = new LinkedHashMap<>();
                                subFieldMap.put(name, f);
                                table.setNameFieldMap(subFieldMap);
                                tapTables.add(table);
                            });
                        } else {
                            tapTables.add(tapTable);
                        }
                        for (TapTable table : tapTables) {
                            // 建表并验证
                            if (super.createTable(node, table)) {
                                // 删除表
                                execute.dropTable(table);
                            }
                        }

                    }
                    return false;
                });
            } catch (Throwable e) {
                TapAssert.error(testCase, langUtil.formatLang("createTableUsingField.all.errorFiled", "", "", e.getMessage()));
            }
        });
    }
}
