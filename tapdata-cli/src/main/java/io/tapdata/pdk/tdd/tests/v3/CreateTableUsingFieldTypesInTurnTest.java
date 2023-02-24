package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.support.DataTypesHandler;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

import static io.tapdata.entity.simplify.TapSimplify.list;

//数据类型建表（依赖CreateTableFunction）
/**
 * 验证开发者描述的每个字段类型都是可以成功建表的，
 * 这里在不同的数据库版本的时候很容易出现类型不兼容的错误
 * 测试失败按警告上报
 */
@DisplayName("createTableUsingField")
@TapGo(tag = "V3", sort = 13, debug = true)
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
        super.execTest("createTableUsingFieldTypesInTurn", (node, testCase) -> {
            TapNodeSpecification tapNodeSpecification = node.nodeInfo().getTapNodeSpecification();
            DefaultExpressionMatchingMap dataTypesMap = tapNodeSpecification.getDataTypesMap();
            if (Objects.isNull(dataTypesMap) || dataTypesMap.isEmpty()) {

                return;
            }
            ConnectorNode connectorNode = node.connectorNode();
            RecordEventExecute execute = node.recordEventExecute();
            execute.testCase(testCase);
            DataTypesHandler handler = DataTypesHandler.create();
            dataTypesMap.iterate(entry -> {
                TapTable tapTable = new TapTable(super.testTableId,super.testTableId);
                String typeName = entry.getKey();
                DataMap typeConfig = entry.getValue();
                TapMapping tapMapping = (TapMapping) typeConfig.get(TapMapping.FIELD_TYPE_MAPPING);
                handler.fillTestFields(tapTable, typeName, tapMapping);
                //@TODO 建表并验证
                if (super.createTable(node, tapTable)) {
                    //@TODO 删除表
                    execute.dropTable(tapTable);
                }
                return false;
            });

        });
    }
}
