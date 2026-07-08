package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.process.FromTableConfig;
import com.tapdata.tm.commons.dag.process.duck.JoinInfo;
import com.tapdata.tm.commons.dag.process.duck.JoinKeyPair;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
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
    void shouldRejectSelectTableStar() {
        UnsupportedOperationException ex = Assertions.assertThrows(UnsupportedOperationException.class, () ->
                SqlParserUtil.parseSelectFields(
                        "SELECT t.* FROM main_table t",
                        Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                        Collections.singletonList(buildSchema()),
                        "main_table",
                        new ArrayList<>(Collections.singletonList("id")),
                        new ArrayList<>(),
                        new HashMap<>()
                )
        );

        Assertions.assertEquals("SELECT table.* is not supported, please specify fields explicitly", ex.getMessage());
    }

    @Test
    void shouldRejectInvalidSqlAndNonSelectSql() {
        RuntimeException parseEx = Assertions.assertThrows(RuntimeException.class, () ->
                SqlParserUtil.parseSelectFields(
                        "SELECT FROM",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        "main_table",
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new HashMap<>()
                )
        );
        RuntimeException nonSelectEx = Assertions.assertThrows(RuntimeException.class, () ->
                SqlParserUtil.parseSelectFields(
                        "UPDATE main_table SET id = 1",
                        Collections.emptyList(),
                        Collections.emptyList(),
                        "main_table",
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new HashMap<>()
                )
        );

        Assertions.assertTrue(parseEx.getMessage().startsWith("Failed to parse SQL:"));
        Assertions.assertEquals("Only SELECT statements are supported", nonSelectEx.getMessage());
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
    void shouldBuildAliasMapForSubQueryWithMultipleTablesAndSetOperation() throws Exception {
        PlainSelect plainSelect = (PlainSelect) ((Select) CCJSqlParserUtil.parse(
                "SELECT sq.id FROM (SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id) sq JOIN payments p ON sq.id = p.user_id"
        )).getSelectBody();
        PlainSelect setSelect = (PlainSelect) ((Select) CCJSqlParserUtil.parse(
                "SELECT * FROM (SELECT id FROM users UNION SELECT id FROM orders) u"
        )).getSelectBody();

        Map<String, String> aliasMap = SqlParserUtil.buildAliasMap(plainSelect);
        Map<String, String> setAliasMap = SqlParserUtil.buildAliasMap(setSelect);

        Assertions.assertEquals("sq", aliasMap.get("sq"));
        Assertions.assertEquals("payments", aliasMap.get("p"));
        Assertions.assertEquals("u", setAliasMap.get("u"));
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
    void shouldIgnoreJoinConditionWithoutColumnEquality() throws Exception {
        PlainSelect plainSelect = (PlainSelect) ((Select) CCJSqlParserUtil.parse(
                "SELECT * FROM users u JOIN orders o ON u.id = 1"
        )).getSelectBody();

        List<JoinInfo> joinInfos = new ArrayList<>();
        SqlParserUtil.parseJoinCondition(plainSelect, joinInfos, new HashMap<>());

        Assertions.assertEquals(1, joinInfos.size());
        Assertions.assertTrue(joinInfos.get(0).getJoinKeys().isEmpty());
    }

    @Test
    void shouldHandleJoinConditionWithoutJoinsOrExpression() throws Exception {
        PlainSelect noJoin = (PlainSelect) ((Select) CCJSqlParserUtil.parse(
                "SELECT id FROM users"
        )).getSelectBody();
        List<JoinInfo> joinInfos = new ArrayList<>();

        SqlParserUtil.parseJoinCondition(noJoin, joinInfos, new HashMap<>());

        Assertions.assertTrue(joinInfos.isEmpty());
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

    @Test
    void shouldFallbackToAllInputSchemasWhenFromTablesDoNotMatch() throws Exception {
        Schema schema = buildSchema("orders", buildField("amount", "Double"));

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT amount FROM orders",
                Collections.singletonList(new FromTableConfig("source-1", "missing")),
                Collections.singletonList(schema),
                "orders",
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>()
        );

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals("amount", fields.get(0).getFieldName());
        Assertions.assertEquals("Double", fields.get(0).getJavaType());
    }

    @Test
    void shouldCreateExpressionFieldNameWhenNoAlias() throws Exception {
        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT 1 + 2 FROM main_table",
                Collections.emptyList(),
                Collections.singletonList(buildSchema()),
                "main_table",
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>()
        );

        Assertions.assertEquals(1, fields.size());
        Assertions.assertTrue(fields.get(0).getFieldName().startsWith("expr_"));
        Assertions.assertEquals(fields.get(0).getFieldName(), fields.get(0).getOriginalFieldName());
        Assertions.assertEquals("VARCHAR", fields.get(0).getDataType());
    }

    @Test
    void shouldCopyUnqualifiedColumnFromAvailableSchemaAndKeepMainTableNullable() throws Exception {
        Schema schema = buildSchema();
        schema.getFields().get(0).setIsNullable(false);

        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT id FROM main_table",
                Collections.singletonList(new FromTableConfig("source-1", "main_table")),
                Collections.singletonList(schema),
                "main_table",
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>()
        );

        Assertions.assertEquals(1, fields.size());
        Assertions.assertTrue((Boolean) fields.get(0).getIsNullable());
    }

    @Test
    void privatePrimaryKeyHelpersShouldTransferAliases() throws Exception {
        List<Column> referencedColumns = List.of(new Column(new Table("o"), "user_id"), new Column("tenant_id"));
        List<String> wideTablePkColumns = new ArrayList<>(List.of("user_id", "tenant_id"));
        List<String> pkColumns = List.of("o.user_id", "tenant_id");
        Field field = new Field();

        invokePrivate("transferPrimaryKeyField",
                new Class[]{List.class, String.class, List.class, List.class, Field.class},
                referencedColumns, "wide_user_id", wideTablePkColumns, pkColumns, field);

        Assertions.assertEquals(List.of("wide_user_id", "wide_user_id"), wideTablePkColumns);
        Assertions.assertEquals("o.user_id", invokePrivate("buildFullColumnName", new Class[]{Column.class}, new Column(new Table("o"), "user_id")));
        Assertions.assertNull(invokePrivate("buildFullColumnName", new Class[]{Column.class}, new Column("id")));
    }

    @Test
    void privateHelpersShouldHandleNullsAndNonMatchingColumns() throws Exception {
        Assertions.assertFalse((Boolean) invokePrivate("isPrimaryKeyColumn",
                new Class[]{Column.class, List.class, List.class}, null, List.of("id"), List.of("t.id")));
        Assertions.assertFalse((Boolean) invokePrivate("isPrimaryKeyColumn",
                new Class[]{Column.class, List.class, List.class}, new Column("id"), Collections.emptyList(), Collections.emptyList()));
        Assertions.assertTrue((Boolean) invokePrivate("isPrimaryKeyColumn",
                new Class[]{Column.class, List.class, List.class}, new Column(new Table("o"), "user_id"), Collections.emptyList(), List.of("o.user_id")));
        Assertions.assertNull(invokePrivate("buildFullColumnName", new Class[]{Column.class}, (Object) null));

        Field target = new Field();
        SqlParserUtil.copyFieldProperties(null, "id", target, new ArrayList<>(), true);
        Assertions.assertNull(target.getDataType());

        Schema schema = new Schema();
        SqlParserUtil.copyFieldProperties(schema, "id", target, new ArrayList<>(), true);
        Assertions.assertNull(target.getDataType());

        schema.setFields(List.of(buildField("other", "String")));
        SqlParserUtil.copyFieldProperties(schema, "id", target, new ArrayList<>(), true);
        Assertions.assertNull(target.getDataType());
    }

    @Test
    void privateParseSelectItemShouldHandleUnexpectedSelectItem() throws Exception {
        Field field = (Field) invokePrivate("parseSelectItem",
                new Class[]{SelectItem.class, Map.class, Map.class, List.class, String.class},
                new SelectItem() {
                    @Override
                    public void accept(SelectItemVisitor selectItemVisitor) {
                    }

                    @Override
                    public SimpleNode getASTNode() {
                        return null;
                    }

                    @Override
                    public void setASTNode(SimpleNode simpleNode) {
                    }
                }, new HashMap<>(), new HashMap<>(), new ArrayList<>(), "main_table");

        Assertions.assertEquals(Field.SOURCE_JOB_ANALYZE, field.getSource());
        Assertions.assertNull(field.getFieldName());
    }

    @Test
    void privateJoinConditionHelpersShouldHandleNulls() throws Exception {
        List<JoinKeyPair> pairs = new ArrayList<>();

        invokePrivate("parseJoinCondition", new Class[]{net.sf.jsqlparser.expression.Expression.class, List.class, Map.class},
                null, pairs, new HashMap<>());
        invokePrivate("parseJoinCondition", new Class[]{net.sf.jsqlparser.expression.Expression.class, List.class, Map.class},
                new EqualsTo(new LongValue(1), new LongValue(1)), pairs, new HashMap<>());

        Assertions.assertTrue(pairs.isEmpty());
    }

    @Test
    void shouldUseFunctionNameAsOriginalFieldNameWhenNoColumnsAreReferenced() throws Exception {
        List<Field> fields = SqlParserUtil.parseSelectFields(
                "SELECT now() AS current_ts FROM main_table",
                Collections.emptyList(),
                Collections.singletonList(buildSchema()),
                "main_table",
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>()
        );

        Assertions.assertEquals("current_ts", fields.get(0).getFieldName());
        Assertions.assertEquals("now", fields.get(0).getOriginalFieldName());
    }

    @Test
    void privateAliasAndColumnHelpersShouldCoverRemainingBranches() throws Exception {
        PlainSelect plainSelect = (PlainSelect) ((Select) CCJSqlParserUtil.parse(
                "SELECT users.id FROM users JOIN orders ON users.id = orders.user_id"
        )).getSelectBody();
        Map<String, String> aliasMap = new HashMap<>();

        invokePrivate("buildAliasTableMap", new Class[]{PlainSelect.class, Map.class}, plainSelect, aliasMap);

        Assertions.assertEquals("users", aliasMap.get("users"));
        Assertions.assertEquals("orders", aliasMap.get("orders"));
        Assertions.assertNull(SqlParserUtil.buildField(new Column("id"), new HashMap<>()).getTable());
        Assertions.assertTrue(SqlParserUtil.buildAliasMap(null).isEmpty());
        Assertions.assertEquals(Collections.emptyList(),
                invokePrivate("extractColumns", new Class[]{net.sf.jsqlparser.expression.Expression.class}, (Object) null));

        List<String> primaryKeys = new ArrayList<>(List.of("id"));
        invokePrivate("transferPrimaryKeyField",
                new Class[]{List.class, String.class, List.class, List.class, Field.class},
                Collections.emptyList(), "alias", primaryKeys, List.of("id"), new Field());
        Assertions.assertEquals(List.of("id"), primaryKeys);

        List<String> missingPrimaryKeys = new ArrayList<>(List.of("id"));
        invokePrivate("transferPrimaryKeyField",
                new Class[]{List.class, String.class, List.class, List.class, Field.class},
                List.of(new Column("other")), "alias", missingPrimaryKeys, List.of("id"), new Field());
        Assertions.assertEquals(List.of("id"), missingPrimaryKeys);
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

    private static Object invokePrivate(String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = SqlParserUtil.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(null, args);
    }
}
