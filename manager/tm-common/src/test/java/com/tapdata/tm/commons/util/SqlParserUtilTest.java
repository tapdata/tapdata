package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.process.DuckDbSqlNode;
import com.tapdata.tm.commons.dag.process.FromTableConfig;
import com.tapdata.tm.commons.dag.process.duck.JoinInfo;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

class SqlParserUtilTest {

    @Test
    void shouldTransferPrimaryKeyWhenWrappedByFunction() throws Exception {
        List<String> wideTablePkColumns = new ArrayList<>(Collections.singletonList("id"));

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT upper(t.id) AS id_upper, t.name FROM main_table t",
                Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                Collections.singletonList(buildSchema()),
                buildNode(),
                wideTablePkColumns,
                new ArrayList<JoinInfo>(),
                new HashMap<>()
        );

        Assertions.assertEquals(Collections.singletonList("id_upper"), wideTablePkColumns);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("id_upper", fields.get(0).getFieldName());
        Assertions.assertTrue(fields.get(0).getPrimaryKey());
        Assertions.assertEquals(1, fields.get(0).getPrimaryKeyPosition());
    }

    @Test
    void shouldTransferPrimaryKeyWhenWrappedByOtherExpressionType() throws Exception {
        List<String> wideTablePkColumns = new ArrayList<>(Collections.singletonList("id"));

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT cast(t.id AS VARCHAR) AS id_cast, t.name FROM main_table t",
                Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                Collections.singletonList(buildSchema()),
                buildNode(),
                wideTablePkColumns,
                new ArrayList<JoinInfo>(),
                new HashMap<>()
        );

        Assertions.assertEquals(Collections.singletonList("id_cast"), wideTablePkColumns);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("id_cast", fields.get(0).getFieldName());
        Assertions.assertTrue(fields.get(0).getPrimaryKey());
        Assertions.assertEquals(1, fields.get(0).getPrimaryKeyPosition());
    }

    @Test
    void shouldMarkFunctionFieldAsPrimaryKeyWhenWideTablePrimaryKeyUsesSelectAlias() throws Exception {
        List<String> wideTablePkColumns = new ArrayList<>(Collections.singletonList("wide_user_id"));

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT upper(o.user_id) AS wide_user_id, o.name FROM users u LEFT JOIN orders o ON u.id = o.user_id",
                List.of(new FromTableConfig("source-1", "u"), new FromTableConfig("source-2", "o")),
                List.of(buildSchema("users", buildField("id", "String"), buildField("name", "String")),
                        buildSchema("orders", buildField("user_id", "String"), buildField("name", "String"))),
                buildNode("u"),
                wideTablePkColumns,
                new ArrayList<>(),
                new HashMap<>()
        );

        Assertions.assertEquals(Collections.singletonList("wide_user_id"), wideTablePkColumns);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("wide_user_id", fields.get(0).getFieldName());
        Assertions.assertTrue(fields.get(0).getPrimaryKey());
        Assertions.assertEquals(1, fields.get(0).getPrimaryKeyPosition());
    }

    private static DuckDbSqlNode buildNode() {
        return buildNode("main_table");
    }

    private static DuckDbSqlNode buildNode(String mainTableName) {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setMainTableName(mainTableName);
        return node;
    }

    private static Schema buildSchema() {
        return buildSchema("main_table", buildField("id", "String"), buildField("name", "String"));
    }

    private static Schema buildSchema(String tableName, Field... fields) {
        Schema schema = new Schema();
        schema.setName(tableName);
        schema.setOriginalName(tableName);
        schema.setQualifiedName(tableName);
        schema.setFields(new ArrayList<>());
        Collections.addAll(schema.getFields(), fields);
        return schema;
    }

    private static Field buildField(String fieldName, String javaType) {
        Field field = new Field();
        field.setFieldName(fieldName);
        field.setOriginalFieldName(fieldName);
        field.setJavaType(javaType);
        field.setDataType("VARCHAR");
        return field;
    }
}
