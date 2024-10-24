package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.CacheNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.exception.NoPrimaryKeyException;
import com.tapdata.tm.commons.schema.*;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUnknownRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/10/18 10:12 Create
 */
public class NoPrimaryKeyVirtualFieldTest {

    Field createVirualField(String tableName) {
        Field field = new Field();
        field.setTableName(tableName);
        field.setFieldName(NoPrimaryKeyVirtualField.FIELD_NAME);
        field.setOriginalFieldName(NoPrimaryKeyVirtualField.FIELD_NAME);
        field.setSource(Field.SOURCE_VIRTUAL_HASH);
        field.setCreateSource(Field.SOURCE_VIRTUAL_HASH);
        return field;
    }

    Field createNormalField(String tableName, String fieldName) {
        return createNormalField(tableName, fieldName, 0);
    }

    Field createNormalField(String tableName, String fieldName, Integer pkPos) {
        Field field = new Field();
        field.setTableName(tableName);
        field.setFieldName(fieldName);
        field.setOriginalFieldName(fieldName);
        field.setSource(Field.SOURCE_AUTO);
        field.setCreateSource(Field.SOURCE_AUTO);
        if (null != pkPos && pkPos > 0) {
            field.setPrimaryKey(true);
            field.setPrimaryKeyPosition(pkPos);
        }
        return field;
    }

    @Nested
    class addVirtualField {
        String tableName;
        List<Field> fields;
        Schema schema;
        Graph<? extends Element, ? extends Element> graph;

        @BeforeEach
        void setUp() {
            tableName = "no_pk_test";

            fields = new ArrayList<>();
            fields.add(createNormalField(tableName, "title"));
            fields.add(createNormalField(tableName, "created"));

            schema = new Schema();
            schema.setOriginalName(tableName);
            schema.setFields(fields);

            graph = new Graph<>();
        }

        @Test
        void testIgnoreWithoutPk() {
            TableNode sourceNode = new TableNode();
            sourceNode.setId(UUID.randomUUID().toString());
            TableNode targetNode = new TableNode();
            targetNode.setId(UUID.randomUUID().toString());

            graph.setEdge(sourceNode.getId(), targetNode.getId());
            targetNode.setGraph(graph);

            schema.setHasPrimaryKey(true);
            NoPrimaryKeyVirtualField.addVirtualField((Object) schema, (Node<?>) targetNode);
            Assertions.assertEquals(0, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
        }

        @Test
        void testIgnoreWithoutNotDataParentNode() {
            TableNode sourceNode = new TableNode();
            sourceNode.setId(UUID.randomUUID().toString());
            CacheNode targetNode = new CacheNode();
            targetNode.setId(UUID.randomUUID().toString());

            graph.setEdge(sourceNode.getId(), targetNode.getId());
            targetNode.setGraph(graph);

            NoPrimaryKeyVirtualField.addVirtualField((Object) schema, (Node<?>) targetNode);
            Assertions.assertEquals(0, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
        }

        @Test
        void testAdd2TableNode() {
            TableNode sourceNode = new TableNode();
            sourceNode.setId(UUID.randomUUID().toString());
            TableNode targetNode = new TableNode();
            targetNode.setWriteStrategy("updateOrInsert");
            targetNode.setId(UUID.randomUUID().toString());

            graph.setEdge(sourceNode.getId(), targetNode.getId());
            targetNode.setGraph(graph);
            sourceNode.setGraph(graph);

            // not add to source
            NoPrimaryKeyVirtualField.addVirtualField((Object) schema, (Node<?>) sourceNode);
            Assertions.assertEquals(0, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
            // not add with appoint conditions
            targetNode.setUpdateConditionFields(Collections.singletonList("title"));
            NoPrimaryKeyVirtualField.addVirtualField((Object) schema, (Node<?>) targetNode);
            Assertions.assertEquals(0, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
            targetNode.setUpdateConditionFields(new ArrayList<>());
            // add to target
            NoPrimaryKeyVirtualField.addVirtualField((Object) schema, (Node<?>) targetNode);
            Assertions.assertEquals(1, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
            // virtual filed exists
            targetNode.setUpdateConditionFields(Collections.singletonList(NoPrimaryKeyVirtualField.FIELD_NAME));
            NoPrimaryKeyVirtualField.addVirtualField((Object) schema, (Node<?>) targetNode);
            Assertions.assertEquals(1, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
        }

        @Test
        void testAdd2DatabaseNode() {
            List<List<Schema>> schemas = Collections.singletonList(Collections.singletonList(schema));

            DatabaseNode sourceNode = new DatabaseNode();
            sourceNode.setId(UUID.randomUUID().toString());
            DatabaseNode targetNode = new DatabaseNode();
            targetNode.setId(UUID.randomUUID().toString());

            graph.setEdge(sourceNode.getId(), targetNode.getId());
            targetNode.setGraph(graph);
            sourceNode.setGraph(graph);

            // not add to source
            NoPrimaryKeyVirtualField.addVirtualField(schemas, sourceNode);
            Assertions.assertEquals(0, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
            // not add with appendWrite
            targetNode.setWriteStrategy("appendWrite");
            NoPrimaryKeyVirtualField.addVirtualField(schemas, targetNode);
            Assertions.assertEquals(0, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
            targetNode.setWriteStrategy("updateOrInsert");
            // not add with appoint conditions
            targetNode.setUpdateConditionFieldMap(new HashMap<>());
            targetNode.getUpdateConditionFieldMap().put(schema.getName(), Collections.singletonList("title"));
            NoPrimaryKeyVirtualField.addVirtualField(schemas, targetNode);
            Assertions.assertEquals(0, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
            targetNode.setUpdateConditionFieldMap(new HashMap<>());
            // add to target
            NoPrimaryKeyVirtualField.addVirtualField(schemas, targetNode);
            Assertions.assertEquals(1, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
            // virtual filed exists
            NoPrimaryKeyVirtualField.addVirtualField(schemas, targetNode);
            Assertions.assertEquals(1, fields.stream().filter(f -> NoPrimaryKeyVirtualField.FIELD_NAME.equals(f.getFieldName())).count());
        }
    }

    @Nested
    class hasPrimaryOrUniqueOrKeys {
        Schema schema;

        @BeforeEach
        void setUp() {
            schema = new Schema();
            schema.setFields(new ArrayList<>());
            schema.getFields().add(createNormalField("no_pk_test", "title"));
        }

        @Test
        void testFieldNull() {
            schema.setFields(null);
            Assertions.assertFalse(NoPrimaryKeyVirtualField.hasPrimaryOrUniqueOrKeys(schema));
        }

        @Test
        void testFalse() {
            Assertions.assertFalse(NoPrimaryKeyVirtualField.hasPrimaryOrUniqueOrKeys(schema));
        }

        @Test
        void testExistUk() {
            Field filed = createNormalField("no_pk_test", "uk");
            filed.setUnique(true);
            schema.getFields().add(filed);
            Assertions.assertTrue(NoPrimaryKeyVirtualField.hasPrimaryOrUniqueOrKeys(schema));

            schema.setHasUnionIndex(true);
            Assertions.assertTrue(NoPrimaryKeyVirtualField.hasPrimaryOrUniqueOrKeys(schema));
        }

        @Test
        void testExistPk() {
            Field filed = createNormalField("no_pk_test", "id", 1);
            schema.getFields().add(filed);
            Assertions.assertTrue(NoPrimaryKeyVirtualField.hasPrimaryOrUniqueOrKeys(schema));

            schema.setHasPrimaryKey(true);
            Assertions.assertTrue(NoPrimaryKeyVirtualField.hasPrimaryOrUniqueOrKeys(schema));
        }
    }

    @Nested
    class getVirtualHashFieldNames {
        String schemaName;
        MetadataInstancesDto metadata;

        @BeforeEach
        void setUp() {
            schemaName = "no_pk_test";

            metadata = new MetadataInstancesDto();
            metadata.setOriginalName(schemaName);
            metadata.setFields(new ArrayList<>());
        }

        @Test
        void testFieldExistsWithMetadata() {
            metadata.getFields().add(createVirualField(metadata.getName()));

            List<String> fieldNames = NoPrimaryKeyVirtualField.getVirtualHashFieldNames(metadata);
            Assertions.assertNotNull(fieldNames);
            Assertions.assertEquals(1, fieldNames.size());
            Assertions.assertTrue(fieldNames.contains(NoPrimaryKeyVirtualField.FIELD_NAME));
        }

        @Test
        void testFieldNotExistsWithMetadata() {
            String fieldName = NoPrimaryKeyVirtualField.FIELD_NAME + "_no";
            metadata.getFields().add(createNormalField(metadata.getName(), fieldName));

            List<String> fieldNames = NoPrimaryKeyVirtualField.getVirtualHashFieldNames(metadata);
            Assertions.assertNotNull(fieldNames);
            Assertions.assertTrue(fieldNames.isEmpty());
        }

        @Test
        void testFieldExistsWithTapTable() {
            metadata.getFields().add(createVirualField(metadata.getName()));

            TapTable tapTable = PdkSchemaConvert.toPdk(metadata);
            Collection<String> fieldNames = NoPrimaryKeyVirtualField.getVirtualHashFieldNames(tapTable);
            Assertions.assertNotNull(fieldNames);
            Assertions.assertEquals(1, fieldNames.size());
            Assertions.assertTrue(fieldNames.contains(NoPrimaryKeyVirtualField.FIELD_NAME));
        }

        @Test
        void testFieldNotExistsWithTapTable() {
            String fieldName = NoPrimaryKeyVirtualField.FIELD_NAME + "_no";
            metadata.getFields().add(createNormalField(metadata.getName(), fieldName));

            TapTable tapTable = PdkSchemaConvert.toPdk(metadata);
            Collection<String> fieldNames = NoPrimaryKeyVirtualField.getVirtualHashFieldNames(tapTable);
            Assertions.assertNotNull(fieldNames);
            Assertions.assertTrue(fieldNames.isEmpty());
        }
    }

    @Nested
    class instance {
        String tableNameHashPk = "has_pk_table";
        String tableNameHashPi = "has_pi_table";
        String tableNameHashUk = "has_uk_table";
        String tableNameNoPk = "no_pk_table";
        NoPrimaryKeyVirtualField instance;

        @BeforeEach
        void setUp() {
            instance = new NoPrimaryKeyVirtualField();

            // add primary key table
            instance.add(Optional.of(new Schema()).map(schema -> {
                schema.setOriginalName(tableNameHashPk);
                schema.setFields(new ArrayList<>());
                schema.getFields().add(createNormalField(schema.getName(), "id", 1));
                schema.getFields().add(createNormalField(schema.getName(), "title"));
                return PdkSchemaConvert.toPdk(schema);
            }).get());

            // add primary index table
            instance.add(Optional.of(new Schema()).map(schema -> {
                schema.setOriginalName(tableNameHashPi);
                schema.setIndices(new ArrayList<>());
                schema.getIndices().add(Optional.of(new TableIndex()).map(index -> {
                    TableIndexColumn tableIndexColumn = new TableIndexColumn();
                    tableIndexColumn.setColumnName("title");

                    index.setIndexName("hash_title");
                    index.setColumns(new ArrayList<>());
                    index.getColumns().add(tableIndexColumn);
                    return index;
                }).get());
                schema.getIndices().add(Optional.of(new TableIndex()).map(index -> {
                    TableIndexColumn tableIndexColumn = new TableIndexColumn();
                    tableIndexColumn.setColumnName("id");

                    index.setIndexName("pk_id");
                    index.setColumns(new ArrayList<>());
                    index.getColumns().add(tableIndexColumn);
                    index.setPrimaryKey("true");
                    return index;
                }).get());
                schema.setFields(new ArrayList<>());
                schema.getFields().add(Optional.of(createNormalField(schema.getName(), "id")).map(field -> {
                    field.setUnique(true);
                    return field;
                }).get());
                schema.getFields().add(createNormalField(schema.getName(), "title"));
                return PdkSchemaConvert.toPdk(schema);
            }).get());

            // add union key table
            instance.add(Optional.of(new Schema()).map(schema -> {
                schema.setOriginalName(tableNameHashUk);
                schema.setHasUnionIndex(true);
                schema.setIndices(new ArrayList<>());
                schema.getIndices().add(Optional.of(new TableIndex()).map(index -> {
                    TableIndexColumn tableIndexColumn = new TableIndexColumn();
                    tableIndexColumn.setColumnName("title");

                    index.setIndexName("hash_title");
                    index.setColumns(new ArrayList<>());
                    index.getColumns().add(tableIndexColumn);
                    return index;
                }).get());
                schema.getIndices().add(Optional.of(new TableIndex()).map(index -> {
                    TableIndexColumn tableIndexColumn = new TableIndexColumn();
                    tableIndexColumn.setColumnName("id");

                    index.setIndexName("uk_id");
                    index.setColumns(new ArrayList<>());
                    index.getColumns().add(tableIndexColumn);
                    index.setUnique(true);
                    return index;
                }).get());
                schema.setFields(new ArrayList<>());
                schema.getFields().add(Optional.of(createNormalField(schema.getName(), "id")).map(field -> {
                    field.setUnique(true);
                    return field;
                }).get());
                schema.getFields().add(createNormalField(schema.getName(), "title"));
                return PdkSchemaConvert.toPdk(schema);
            }).get());

            // add no pk table
            instance.add(Optional.of(new Schema()).map(schema -> {
                schema.setOriginalName(tableNameNoPk);
                schema.setFields(new ArrayList<>());
                schema.getFields().add(createNormalField(schema.getName(), "title"));
                schema.setIndices(new ArrayList<>());
                schema.getIndices().add(Optional.of(new TableIndex()).map(index -> {
                    TableIndexColumn tableIndexColumn = new TableIndexColumn();
                    tableIndexColumn.setColumnName("title");

                    index.setIndexName("hash_title");
                    index.setColumns(new ArrayList<>());
                    index.getColumns().add(tableIndexColumn);
                    return index;
                }).get());
                return PdkSchemaConvert.toPdk(schema);
            }).get());
        }

        @Test
        void testAdd() {
            Assertions.assertFalse(instance.noPkTables.containsKey(tableNameHashPk));
            Assertions.assertFalse(instance.noPkTables.containsKey(tableNameHashPi));
            Assertions.assertFalse(instance.noPkTables.containsKey(tableNameHashUk));
            Assertions.assertTrue(instance.noPkTables.containsKey(tableNameNoPk));
        }

        @Test
        void testErrorIncompleteFields() {
            TapUpdateRecordEvent recordEvent = TapUpdateRecordEvent.create()
                .table(tableNameNoPk)
                .before(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("created", System.currentTimeMillis());
                    return data;
                }).get())
                .after(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("created", System.currentTimeMillis());
                    return data;
                }).get());
            Assertions.assertThrows(NoPrimaryKeyException.class, () -> instance.addHashValue(recordEvent));
            Assertions.assertFalse(instance.addHashValue(recordEvent));
            Assertions.assertFalse(instance.addHashValue(TapDeleteRecordEvent.create()
                .table(tableNameNoPk)
                .before(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("created", System.currentTimeMillis());
                    return data;
                }).get())
            ));
        }

        @Test
        void testErrorNotfoundHashAlgorithm() {
            TapUpdateRecordEvent recordEvent = TapUpdateRecordEvent.create()
                .table(tableNameNoPk)
                .before(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("created", System.currentTimeMillis());
                    return data;
                }).get())
                .after(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("created", System.currentTimeMillis());
                    return data;
                }).get());

            try (MockedStatic<MessageDigest> mockedStatic = Mockito.mockStatic(MessageDigest.class)) {
                mockedStatic.when(() -> MessageDigest.getInstance(Mockito.anyString())).thenThrow(new NoSuchAlgorithmException("XXX not found"));
                Assertions.assertThrows(NoPrimaryKeyException.class, () -> instance.addHashValue(recordEvent));
                Assertions.assertFalse(instance.addHashValue(recordEvent));
                Assertions.assertFalse(instance.addHashValue(TapInsertRecordEvent.create()
                    .table(tableNameNoPk)
                    .after(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                        data.put("created", System.currentTimeMillis());
                        return data;
                    }).get())
                ));
                Assertions.assertFalse(instance.addHashValue(TapDeleteRecordEvent.create()
                    .table(tableNameNoPk)
                    .before(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                        data.put("created", System.currentTimeMillis());
                        return data;
                    }).get())
                ));
            }
        }

        @Test
        void testOtherError() {
            TapUpdateRecordEvent recordEvent = TapUpdateRecordEvent.create()
                .table(tableNameNoPk)
                .before(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("created", System.currentTimeMillis());
                    return data;
                }).get())
                .after(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("created", System.currentTimeMillis());
                    return data;
                }).get());

            try (MockedStatic<MessageDigest> mockedStatic = Mockito.mockStatic(MessageDigest.class)) {
                mockedStatic.when(() -> MessageDigest.getInstance(Mockito.anyString())).thenThrow(new RuntimeException("others"));
                Assertions.assertThrows(NoPrimaryKeyException.class, () -> instance.addHashValue(recordEvent));
                Assertions.assertFalse(instance.addHashValue(recordEvent));
                Assertions.assertFalse(instance.addHashValue(TapInsertRecordEvent.create()
                    .table(tableNameNoPk)
                    .after(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                        data.put("created", System.currentTimeMillis());
                        return data;
                    }).get())
                ));
                Assertions.assertFalse(instance.addHashValue(TapDeleteRecordEvent.create()
                    .table(tableNameNoPk)
                    .before(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                        data.put("created", System.currentTimeMillis());
                        return data;
                    }).get())
                ));
            }
        }

        @Nested
        class addHashValue {

            @Test
            void testSkipTable() {
                TapInsertRecordEvent recordEvent = TapInsertRecordEvent.create();
                recordEvent.table(tableNameHashPk);
                Assertions.assertTrue(instance.addHashValue(recordEvent));
            }

            @Test
            void testInsertRecordEvent() {
                TapInsertRecordEvent recordEvent = TapInsertRecordEvent.create();
                recordEvent.table(tableNameNoPk);
                recordEvent.setAfter(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("title", "1");
                    return data;
                }).get());
                Assertions.assertTrue(instance.addHashValue(recordEvent));
            }

            @Test
            void testUpdateRecordEvent() {
                TapUpdateRecordEvent recordEvent = TapUpdateRecordEvent.create();
                recordEvent.table(tableNameNoPk);
                recordEvent.setBefore(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("title", "2");
                    return data;
                }).get());
                recordEvent.setAfter(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("title", "1");
                    return data;
                }).get());
                Assertions.assertTrue(instance.addHashValue(recordEvent));
            }

            @Test
            void testDeleteRecordEvent() {
                TapDeleteRecordEvent recordEvent = TapDeleteRecordEvent.create();
                recordEvent.table(tableNameNoPk);
                recordEvent.setBefore(Optional.of(new LinkedHashMap<String, Object>()).map(data -> {
                    data.put("title", "2");
                    return data;
                }).get());
                Assertions.assertTrue(instance.addHashValue(recordEvent));
            }

            @Test
            void testUnknownRecordEvent() {
                TapUnknownRecordEvent recordEvent = TapUnknownRecordEvent.create();
                recordEvent.setTableId(tableNameNoPk);
                Assertions.assertTrue(instance.addHashValue(recordEvent));
            }
        }
    }

    @Nested
    class allTypes {
        String tableName = "no_pk_table";
        NoPrimaryKeyVirtualField instance;
        TapInsertRecordEvent recordEvent;

        @BeforeEach
        void setUp() {
            instance = new NoPrimaryKeyVirtualField();
            instance.add(Optional.of(new Schema()).map(schema -> {
                schema.setOriginalName(tableName);
                schema.setFields(new ArrayList<>());
                schema.getFields().add(createNormalField(schema.getName(), "key"));
                schema.getFields().add(createNormalField(schema.getName(), "val"));
                return PdkSchemaConvert.toPdk(schema);
            }).get());
            recordEvent = TapInsertRecordEvent.create().table(tableName).after(new LinkedHashMap<>());
        }

        void assertType(String key, Object val) {
            recordEvent.getAfter().put("key", key);
            recordEvent.getAfter().put("val", val);
            Assertions.assertTrue(instance.addHashValue(recordEvent));
            Assertions.assertNotNull(recordEvent.getAfter().get(NoPrimaryKeyVirtualField.FIELD_NAME));
        }

        @Test
        void testTypes() {
            assertType("non", null);
        }

        @Test
        void testBytes() {
            assertType("bytes", "hello".getBytes());
        }

        @Test
        void testString() {
            assertType("string", "hello");
        }

        @Test
        void testInteger() {
            assertType("integer", 1);
        }

        @Test
        void testFloat() {
            assertType("float", 1.1f);
        }

        @Test
        void testDouble() {
            assertType("double", 1.1d);
        }

        @Test
        void testArray() {
            assertType("array", new String[]{"hello", "world"});
        }

        @Test
        void testList() {
            assertType("list", Arrays.asList("hello", "world"));
        }

        @Test
        void testMap() {
            assertType("map", new HashMap<String, Object>() {{
                put("hello", "world");
            }});
        }
    }
}
