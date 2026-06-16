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

    private static DuckDbSqlNode buildNode() {
        DuckDbSqlNode node = new DuckDbSqlNode();
        node.setMainTableName("main_table");
        return node;
    }

    private static Schema buildSchema() {
        Schema schema = new Schema();
        schema.setName("main_table");
        schema.setOriginalName("main_table");
        schema.setQualifiedName("main_table");
        schema.setFields(new ArrayList<>());
        schema.getFields().add(buildField("id", "String"));
        schema.getFields().add(buildField("name", "String"));
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
