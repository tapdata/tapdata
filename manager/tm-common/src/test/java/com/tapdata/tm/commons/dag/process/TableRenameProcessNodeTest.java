package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.error.TapDynamicTableNameExCode_35;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/5/21 11:13 Create
 */
class TableRenameProcessNodeTest {

    private final static String prefix = "T_";
    private final static String suffix = "2024";
    private final static String replaceBefore = "Before";
    private final static String replaceAfter = "After";
    private final static String transferCase = "toUpperCase";

    private final static String customOrigin = "Custom";
    private final static String customPrevious = "CustomPrevious";
    private final static String customCurrent = "CustomCurrent";
    private final static String customEmptyCurrent = "CustomEmptyCurrent";

    private TableRenameProcessNode instance;

    @BeforeEach
    public void setUp() {
        instance = new TableRenameProcessNode();
        instance.setPrefix(prefix);
        instance.setSuffix(suffix);
        instance.setReplaceBefore(replaceBefore);
        instance.setReplaceAfter(replaceAfter);
        instance.setTransferCase(transferCase);
        instance.setTableNames(new LinkedHashSet<>());
        instance.getTableNames().add(new TableRenameTableInfo(customOrigin, customPrevious, customCurrent));
        instance.getTableNames().add(new TableRenameTableInfo(customEmptyCurrent, customEmptyCurrent, ""));
    }

    private Schema initSchema(Map<String, String> expectedMap, String tableName, String expectedName) {
        Schema schema = new Schema();
        schema.setAncestorsName(tableName);
        schema.setOriginalName(tableName);
        schema.setName(tableName);
        expectedMap.put(tableName, expectedName);
        return schema;
    }

    private String expectedTableName(String tableName) {
        return (prefix + tableName + suffix).toUpperCase(Locale.ROOT);
    }



    @Nested
    class InfoMapTest {
        String originalName = "origin";
        String previousName = "previous";
        String currentName = "current";

        private void testTableNamesIsNull(Function<TableRenameProcessNode, Map<String, TableRenameTableInfo>> mapGetter) {
            TableRenameProcessNode instance = new TableRenameProcessNode();
            Map<String, TableRenameTableInfo> map = mapGetter.apply(instance);
            Assertions.assertNotNull(map);
            Assertions.assertTrue(map.isEmpty());
        }

        private void testKeyWithPrevious(String expectedValue, Function<TableRenameProcessNode, Map<String, TableRenameTableInfo>> mapGetter) {
            TableRenameTableInfo info = new TableRenameTableInfo(originalName, previousName, currentName);

            instance.setTableNames(new LinkedHashSet<>());
            instance.getTableNames().add(info);

            Map<String, TableRenameTableInfo> map = mapGetter.apply(instance);
            Assertions.assertNotNull(map);
            Assertions.assertNotNull(map.get(expectedValue));
            Assertions.assertEquals(info, map.get(expectedValue));
        }

        @Test
        void testOriginalMap() {
            Function<TableRenameProcessNode, Map<String, TableRenameTableInfo>> mapGetter = TableRenameProcessNode::originalMap;

            testTableNamesIsNull(mapGetter);
            testKeyWithPrevious(originalName, mapGetter);
        }

        @Test
        void testPreviousMap() {
            Function<TableRenameProcessNode, Map<String, TableRenameTableInfo>> mapGetter = TableRenameProcessNode::previousMap;

            testTableNamesIsNull(mapGetter);
            testKeyWithPrevious(previousName, mapGetter);
        }

        @Test
        void testCurrentMap() {
            Function<TableRenameProcessNode, Map<String, TableRenameTableInfo>> mapGetter = TableRenameProcessNode::currentMap;

            testTableNamesIsNull(mapGetter);
            testKeyWithPrevious(currentName, mapGetter);
        }
    }

    @Test
    void testMergeSchema() {
        // init config
        Map<String, String> expectedMap = new HashMap<>();

        DAG.Options options = new DAG.Options();
        options.setIncludes(new ArrayList<>());
        options.getIncludes().add("TEST");

        // test empty schemas
        List<List<Schema>> inputSchemas = new ArrayList<>();
        List<Schema> outputSchemas = instance.mergeSchema(inputSchemas, null, options);
        Assertions.assertTrue(outputSchemas.isEmpty());

        // test rules
        inputSchemas.add(Arrays.asList(
            initSchema(expectedMap, "TEST", expectedTableName("TEST")), // prefix + suffix + upperCase
            initSchema(expectedMap, "TestBefore", expectedTableName("TestAfter")), // keywords
            initSchema(expectedMap, customOrigin, customCurrent) // custom table name
        ));

        outputSchemas = instance.mergeSchema(inputSchemas, null, options);
        Assertions.assertNotNull(outputSchemas);
        for (Schema schema : outputSchemas) {
            Assertions.assertNotNull(schema);

            String expectedName = expectedMap.get(schema.getAncestorsName());
            Assertions.assertNotNull(expectedName);
            Assertions.assertEquals(expectedName, schema.getName());
            Assertions.assertEquals(expectedName, schema.getOriginalName());
        }
    }

    @Test
    void testConvertTableName() {
        String tableName = "TEST";
        String expectedValue;
        Map<String, TableRenameTableInfo> infoMap = instance.originalMap();

        // test normal
        expectedValue = expectedTableName(tableName);
        Assertions.assertEquals(expectedValue, instance.convertTableName(infoMap, tableName, false));

        // test in custom-table-name
        Assertions.assertEquals(customCurrent, instance.convertTableName(infoMap, customOrigin, false));

        // test in custom-table-name, conflict with RenameDDL
        assertThrows(TapCodeException.class, () -> instance.convertTableName(infoMap, customOrigin, true));

        // test empty currentName
        Assertions.assertEquals(customEmptyCurrent, instance.convertTableName(infoMap, customEmptyCurrent, false));

        // test empty rules
        instance = new TableRenameProcessNode();
        Assertions.assertEquals(tableName, instance.convertTableName(tableName));
        // test illegal transferCase
        instance.setTransferCase("illegalTransferCase");
        instance.setReplaceBefore("test replaceBefore not blank & replaceAfter is null");
        Assertions.assertEquals(tableName, instance.convertTableName(tableName));
        // test toLowerCase
        instance.setTransferCase("toLowerCase");
        Assertions.assertEquals(tableName.toLowerCase(Locale.ROOT), instance.convertTableName(tableName));
    }

    @Test
    void testConvertTableNameConflict() {
        Map<String, TableRenameTableInfo> infoMap = instance.originalMap();
        TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
            instance.convertTableName(infoMap, "CustomEmptyCurrent", true);
        });
        assertEquals(TapDynamicTableNameExCode_35.RENAME_DDL_CONFLICTS_WITH_CUSTOM_TABLE_NAME, tapCodeException.getCode());
    }
}
