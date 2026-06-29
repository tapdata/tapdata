package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.process.FromTableConfig;
import com.tapdata.tm.commons.dag.process.duck.JoinInfo;
import com.tapdata.tm.commons.dag.process.duck.JoinKeyPair;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SqlParserUtilTest {

    @Test
    void shouldKeepWideTablePrimaryKeyUnchangedWhenWrappedByFunction() throws Exception {
        List<String> wideTablePkColumns = new ArrayList<>(Collections.singletonList("id"));

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT upper(t.id) AS id_upper, t.name FROM main_table t",
                Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                Collections.singletonList(buildSchema()),
                "mainTable",
                wideTablePkColumns,
                new ArrayList<JoinInfo>(),
                new HashMap<>()
        );

        Assertions.assertEquals(Collections.singletonList("id"), wideTablePkColumns);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("id_upper", fields.get(0).getFieldName());
        Assertions.assertEquals("id", fields.get(0).getOriginalFieldName());
        Assertions.assertNull(fields.get(0).getPrimaryKey());
        Assertions.assertNull(fields.get(0).getPrimaryKeyPosition());
        Assertions.assertEquals("VARCHAR", fields.get(0).getDataType());
        Assertions.assertEquals("String", fields.get(0).getJavaType());
    }

    @Test
    void shouldKeepWideTablePrimaryKeyUnchangedWhenWrappedByOtherExpressionType() throws Exception {
        List<String> wideTablePkColumns = new ArrayList<>(Collections.singletonList("id"));

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT cast(t.id AS VARCHAR) AS id_cast, t.name FROM main_table t",
                Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                Collections.singletonList(buildSchema()),
                "mainTable",
                wideTablePkColumns,
                new ArrayList<JoinInfo>(),
                new HashMap<>()
        );

        Assertions.assertEquals(Collections.singletonList("id"), wideTablePkColumns);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("id_cast", fields.get(0).getFieldName());
        Assertions.assertEquals("id", fields.get(0).getOriginalFieldName());
        Assertions.assertNull(fields.get(0).getPrimaryKey());
        Assertions.assertNull(fields.get(0).getPrimaryKeyPosition());
        Assertions.assertEquals("VARCHAR", fields.get(0).getDataType());
    }

    @Test
    void shouldKeepFunctionFieldWithoutPrimaryKeyMetadataWhenWideTablePrimaryKeyUsesSelectAlias() throws Exception {
        List<String> wideTablePkColumns = new ArrayList<>(Collections.singletonList("wide_user_id"));

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT upper(o.user_id) AS wide_user_id, o.name FROM users u LEFT JOIN orders o ON u.id = o.user_id",
                List.of(new FromTableConfig("source-1", "u"), new FromTableConfig("source-2", "o")),
                List.of(buildSchema("users", buildField("id", "String"), buildField("name", "String")),
                        buildSchema("orders", buildField("user_id", "String"), buildField("name", "String"))),
                "mainTable",
                wideTablePkColumns,
                new ArrayList<>(),
                new HashMap<>()
        );

        Assertions.assertEquals(Collections.singletonList("wide_user_id"), wideTablePkColumns);
        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("wide_user_id", fields.get(0).getFieldName());
        Assertions.assertEquals("user_id", fields.get(0).getOriginalFieldName());
        Assertions.assertNull(fields.get(0).getPrimaryKey());
        Assertions.assertNull(fields.get(0).getPrimaryKeyPosition());
        Assertions.assertEquals("VARCHAR", fields.get(0).getDataType());
    }

    @Test
    void shouldRejectBlankSql() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class, () ->
                SqlParserUtil.parseSelectFields(
                        "   ",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        "main_table",
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new HashMap<>()
                )
        );

        Assertions.assertEquals("SQL query cannot be blank", ex.getMessage());
    }

    @Test
    void shouldRejectSelectStar() {
        UnsupportedOperationException ex = Assertions.assertThrows(UnsupportedOperationException.class, () ->
                SqlParserUtil.parseSelectFields(
                        "SELECT * FROM main_table",
                        Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                        Collections.singletonList(buildSchema()),
                        "main_table",
                        new ArrayList<>(Collections.singletonList("id")),
                        new ArrayList<>(),
                        new HashMap<>()
                )
        );

        Assertions.assertEquals("SELECT * is not supported, please specify fields explicitly", ex.getMessage());
    }

    @Test
    void shouldRejectDuplicateFieldName() {
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () ->
                SqlParserUtil.parseSelectFields(
                        "SELECT t.id AS dup, t.name AS dup FROM main_table t",
                        Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                        Collections.singletonList(buildSchema()),
                        "main_table",
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new HashMap<>()
                )
        );

        Assertions.assertEquals("Duplicate field name: dup", ex.getMessage());
    }

    @Test
    void shouldBuildAliasMapForSubQueryAlias() throws Exception {
        PlainSelect plainSelect = (PlainSelect) ((Select) CCJSqlParserUtil.parse(
                "SELECT sq.id FROM (SELECT u.id FROM users u) sq"
        )).getSelectBody();

        Map<String, String> aliasMap = SqlParserUtil.buildAliasMap(plainSelect);

        Assertions.assertEquals("users", aliasMap.get("u"));
        Assertions.assertEquals("users", aliasMap.get("users"));
        Assertions.assertEquals("users", aliasMap.get("sq"));
    }

    @Test
    void shouldParseJoinConditionWithAndExpression() throws Exception {
        PlainSelect plainSelect = (PlainSelect) ((Select) CCJSqlParserUtil.parse(
                "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND u.tenant_id = o.tenant_id"
        )).getSelectBody();

        List<JoinInfo> joinInfos = new ArrayList<>();
        Map<String, String> aliasMap = new HashMap<>();
        aliasMap.put("u", "users");
        aliasMap.put("o", "orders");

        SqlParserUtil.parseJoinCondition(plainSelect, joinInfos, aliasMap);

        Assertions.assertEquals(1, joinInfos.size());
        Assertions.assertEquals("orders", joinInfos.get(0).getTable());
        Assertions.assertEquals(2, joinInfos.get(0).getJoinKeys().size());

        JoinKeyPair first = joinInfos.get(0).getJoinKeys().get(0);
        Assertions.assertEquals("users", first.getLeft().getTable());
        Assertions.assertEquals("id", first.getLeft().getField());
        Assertions.assertEquals("orders", first.getRight().getTable());
        Assertions.assertEquals("user_id", first.getRight().getField());

        JoinKeyPair second = joinInfos.get(0).getJoinKeys().get(1);
        Assertions.assertEquals("tenant_id", second.getLeft().getField());
        Assertions.assertEquals("tenant_id", second.getRight().getField());
    }

    @Test
    void shouldCopyColumnMetadataUsingAliasResolvedSchema() throws Exception {
        Schema schema = buildSchema();
        schema.getFields().get(0).setDataType("BIGINT");
        schema.getFields().get(0).setJavaType("Long");
        schema.getFields().get(0).setIsNullable(false);

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT t.id, t.name FROM main_table t",
                Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                Collections.singletonList(schema),
                "main_table",
                new ArrayList<>(Collections.singletonList("id")),
                new ArrayList<>(),
                new HashMap<>()
        );

        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("id", fields.get(0).getFieldName());
        Assertions.assertEquals("BIGINT", fields.get(0).getDataType());
        Assertions.assertEquals("Long", fields.get(0).getJavaType());
        Assertions.assertTrue((Boolean) fields.get(0).getIsNullable());
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
