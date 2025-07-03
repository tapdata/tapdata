package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.Element;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.exception.NoPrimaryKeyException;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import io.github.openlg.graphlib.Graph;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 无主键虚拟列
 *
 * <ul>
 *     <h3>已知问题：</h3>
 *     <li>虚拟列名为固定值，可能会与用户数据模型产生冲突</li>
 *     <li>产品现阶段，还无法在目标模型获取到来自哪个节点的哪个表，源节点会对无主键表统一添加 hash 列</li>
 *     <li>存在多条字段及值完全一致的数据，删除和更新会将目标的所有数据进行变更，目标可能存在多余或丢失数据情况</li>
 *     <li>不支持字段 DDL</li>
 * </ul>
 * <ul>
 *     <h3>关键需求：</h3>
 *     <li>在源节点生成 hash 列数据，可以避免处理节点修改数据的场景下，对同个数据多次修改导致 before 中 hash 列错误问题</li>
 *     <li>在目标节点补充 hash 列模型，是因为需要根据目标节点配置来判断是否添加</li>
 * </ul>
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/10/15 17:19 Create
 */
public class NoPrimaryKeyVirtualField {

    public static final String HASH_ALGORITHM = "MD5";
    public static final String FIELD_NAME = "_no_pk_hash";
    public static final int FIELD_LENGTH = 32;
    public static final TapType FIELD_TAP_TYPE = new TapString((long) FIELD_LENGTH, false);
    public static final byte SPLIT_CHAR = ',';

    protected Consumer<TapTable> addTable;
    protected Predicate<TapRecordEvent> addHashValue;
    protected final Map<String, HashValueAppender> noPkTables = new HashMap<>();

    public void init(Graph<? extends Element, ? extends Element> graph) {
        if (isEnable(graph)) {
            this.noPkTables.clear();
            this.addTable = table -> {
                // 过滤：有主键、有唯一索引的表
                if (Optional.ofNullable(table.primaryKeys()).map(keys -> !keys.isEmpty()).orElse(false)) return;
                if (Optional.ofNullable(table.getIndexList()).map(list -> {
                    for (TapIndex index : list) {
                        if (Boolean.TRUE.equals(index.getPrimary())) return true;
                        if (Boolean.TRUE.equals(index.isUnique())) return true;
                    }
                    return false;
                }).orElse(false)) return;

                // 添加 hash 列到数据上
                String tableName = table.getName();
                List<String> keys = new ArrayList<>(table.getNameFieldMap().keySet());
                noPkTables.put(tableName, new HashValueAppender(tableName, keys, getTargetNodePKVirtualFieldName(graph)));
            };
            this.addHashValue = event -> {
                HashValueAppender handle = noPkTables.get(event.getTableId());
                if (null != handle) {
                    return handle.apply(event);
                }
                return true;
            };
        } else {
            this.noPkTables.clear();
            this.addTable = table -> {
            };
            this.addHashValue = table -> true;
        }
    }

    public void add(TapTable table) {
        this.addTable.accept(table);
    }

    public boolean addHashValue(TapRecordEvent event) {
        return this.addHashValue.test(event);
    }

    // ---------- 以下为静态函数 ----------

    public static void addVirtualField(Object schemaObj, Node<?> node) {
        if (isEnable(node.getGraph())) {
            // 没有前置节点，跳过
            if (node.predecessors().isEmpty()) return;
            checkAndAddVirtualField(schemaObj, node);
        }
    }

    public static Collection<String> getVirtualHashFieldNames(TapTable table) {
        Collection<String> virtualHashFields = new HashSet<>();
        Optional.ofNullable(table.childItems()).ifPresent(fields -> {
            for (TapField field : fields) {
                if (Field.SOURCE_VIRTUAL_HASH.equals(field.getCreateSource())) {
                    virtualHashFields.add(field.getName());
                }
            }
        });
        return virtualHashFields;
    }

    public static List<String> getVirtualHashFieldNames(MetadataInstancesDto metadata) {
        List<String> virtualHashFields = new ArrayList<>();
        Optional.ofNullable(metadata.getFields()).ifPresent(fields -> {
            for (Field field : fields) {
                if (Field.SOURCE_VIRTUAL_HASH.equals(field.getCreateSource())) {
                    virtualHashFields.add(field.getFieldName());
                }
            }
        });
        return virtualHashFields;
    }

    public static boolean isEnable(Graph<? extends Element, ? extends Element> graph) {
        for (String id : graph.getNodes()) {
            // 存在表合并，不启用
            Element graphNode = graph.getNode(id);
            if (graphNode instanceof MergeTableNode) return false;
            // 存在表关联，不启用
            if (graphNode instanceof JoinProcessorNode) return false;
            // 过滤：追加写入模式
            if (graphNode instanceof DataParentNode) {
                // 没有前置节点，跳过
                DataParentNode<?> dataParentNode = (DataParentNode<?>) graphNode;
                if (!dataParentNode.predecessors().isEmpty() && NoPrimaryKeySyncMode.ADD_HASH == dataParentNode.getNoPrimaryKeySyncMode()) {
                    return true;
                }
            }
        }
        return false;
    }
    public static String getTargetNodePKVirtualFieldName(Graph<? extends Element, ? extends Element> graph){
        for (String id : graph.getNodes()) {
            // 存在表合并，不启用
            Element graphNode = graph.getNode(id);
            if (graphNode instanceof MergeTableNode) return null;
            // 存在表关联，不启用
            if (graphNode instanceof JoinProcessorNode) return null;
            // 过滤：追加写入模式
            if (graphNode instanceof DataParentNode) {
                // 没有前置节点，跳过
                DataParentNode<?> dataParentNode = (DataParentNode<?>) graphNode;
                if (!dataParentNode.predecessors().isEmpty() && NoPrimaryKeySyncMode.ADD_HASH == dataParentNode.getNoPrimaryKeySyncMode()) {
                    return getNoPKVirtualFieldName(((DataParentNode<?>) graphNode).getNoPKVirtualFieldName());
                }
            }
        }
        return null;
    }

    // ---------- 以下为内部函数 ----------

    protected static void checkAndAddVirtualField(Object schemaObj, Node<?> node) {
        if (schemaObj instanceof Schema) {
            Schema schema = (Schema) schemaObj;
            if (isNeed2AddField(schema, node)) {
                addVirtualField2Schema(schema, node);
            }
        } else if (schemaObj instanceof List) {
            for (Object schema : (List<?>) schemaObj) {
                checkAndAddVirtualField(schema, node);
            }
        }
    }

    protected static boolean isNeed2AddField(Schema schema, Node<?> node) {
        // 过滤：主键，唯一索引
        if (hasPrimaryOrUniqueOrKeys(schema)) return false;
        List<String> schemaField = schema.getFields().stream().map(Field::getFieldName).toList();
        // 过滤：指定关联字段
        if (node instanceof TableNode) {
            TableNode tableNode = (TableNode) node;
            List<String> conditionFields = tableNode.getUpdateConditionFields();
            String noPKVirtualFieldName = getNoPKVirtualFieldName(tableNode.getNoPKVirtualFieldName());
            filterConditionFieldNotInSchemaAndNoPKField(conditionFields, schemaField, noPKVirtualFieldName);
            return hasVirtualField(conditionFields, noPKVirtualFieldName);
        } else if (node instanceof DatabaseNode) {
            DatabaseNode databaseNode = (DatabaseNode) node;
            Map<String, List<String>> conditionFieldMap = databaseNode.getUpdateConditionFieldMap();
            String noPKVirtualFieldName = getNoPKVirtualFieldName(databaseNode.getNoPKVirtualFieldName());
            List<String> conditionFields = null == conditionFieldMap ? null : conditionFieldMap.get(schema.getName());
            filterConditionFieldNotInSchemaAndNoPKField(conditionFields, schemaField, noPKVirtualFieldName);
            return hasVirtualField(conditionFields,noPKVirtualFieldName);
        }
        return false;
    }

    private static void filterConditionFieldNotInSchemaAndNoPKField(List<String> conditionFields, List<String> schemaField, String noPKVirtualFieldName) {
        if (null != conditionFields && !conditionFields.isEmpty()) {
            Iterator<String> conditionFieldsIterator = conditionFields.iterator();
            while (conditionFieldsIterator.hasNext()) {
                String conditionField = conditionFieldsIterator.next();
                if (!schemaField.contains(conditionField)&&!noPKVirtualFieldName.equalsIgnoreCase(conditionField)) {
                    conditionFieldsIterator.remove();
                }
            }
        }
    }

    protected static boolean hasVirtualField(List<String> conditionFields,String noPKVirtualFieldName) {
        if (null != conditionFields && !conditionFields.isEmpty()) {
            for (String field : conditionFields) {
                if (noPKVirtualFieldName.equalsIgnoreCase(field)) return true;
            }
            return false;
        }
        return true;
    }

    protected static void addVirtualField2Schema(Schema schema, Node<?> node) {
        List<Field> fields = schema.getFields();
        String noPKVirtualFieldName = getNoPKVirtualFieldName(node);
        // 字段已存在，不添加
        for (Field field : fields) {
            if (noPKVirtualFieldName.equalsIgnoreCase(field.getFieldName())) return;
        }
        // 字段不存在，添加
        Field field = createVirtualField(schema, node);
        fields.add(field);
    }

    public static String getNoPKVirtualFieldName(Node node) {
        if (node instanceof DatabaseNode) {
            DatabaseNode databaseNode = (DatabaseNode) node;
            if (StringUtils.isBlank(databaseNode.getNoPKVirtualFieldName())) {
                return FIELD_NAME;
            } else {
                return databaseNode.getNoPKVirtualFieldName();
            }
        } else if (node instanceof TableNode) {
            TableNode tableNode = (TableNode) node;
            if (StringUtils.isBlank(tableNode.getNoPKVirtualFieldName())) {
                return FIELD_NAME;
            } else {
                return tableNode.getNoPKVirtualFieldName();
            }
        } else {
            return null;
        }
    }

    public static String getNoPKVirtualFieldName(String noPKVirtualFieldName) {
        if (StringUtils.isBlank(noPKVirtualFieldName)) {
            return FIELD_NAME;
        } else {
            return noPKVirtualFieldName;
        }
    }

    protected static Field createVirtualField(Schema schema, Node<?> node) {
        Field field = new Field();
        field.setTableName(schema.getName());
        String noPKVirtualFieldName = getNoPKVirtualFieldName(node);
        field.setFieldName(noPKVirtualFieldName);
        field.setOriginalFieldName(noPKVirtualFieldName);
        field.setSource(Field.SOURCE_VIRTUAL_HASH);
        field.setCreateSource(Field.SOURCE_VIRTUAL_HASH);
        field.setColumnSize(FIELD_LENGTH);
        field.setOriPrecision(FIELD_LENGTH);
        field.setJavaType("String");
        field.setJavaType1("String");
        field.setTapType(InstanceFactory.instance(JsonParser.class).toJson(FIELD_TAP_TYPE));
        field.setColumnPosition(schema.getFields().size() + 1);
        field.setPrimaryKey(false);
        field.setIsNullable(true);
        field.setId(MetaDataBuilderUtils.generateFieldId(node.getId(), field.getTableName(), field.getFieldName()));
        return field;
    }

    protected static boolean hasPrimaryOrUniqueOrKeys(Schema schema) {
        if (schema.isHasPrimaryKey() || schema.isHasUnionIndex()) return true;

        List<Field> fields = schema.getFields();
        if (null != fields) {
            for (Field field : fields) {
                if (Boolean.TRUE.equals(field.getPrimaryKey())) return true;
                if (field.isUnique()) return true;
            }
        }
        return false;
    }

    protected static class HashValueAppender {

        final String table;
        final List<String> keys;
        private String noPKVirtualFieldName;
        private Predicate<TapInsertRecordEvent> insertRecordEventPredicate;
        private Predicate<TapUpdateRecordEvent> updateRecordEventPredicate;
        private Predicate<TapDeleteRecordEvent> deleteRecordEventPredicate;

        HashValueAppender(String table, List<String> keys,String noPKVirtualFieldName) {
            this.table = table;
            this.keys = keys;
            this.noPKVirtualFieldName = noPKVirtualFieldName;
            keys.removeIf(key -> key.equalsIgnoreCase(this.noPKVirtualFieldName));
            insertRecordEventPredicate = event -> {
                Optional.ofNullable(event.getAfter())
                    .ifPresent(data -> data.put(this.noPKVirtualFieldName, toHash(keys, data, false)));
                return true;
            };
            updateRecordEventPredicate = event -> {
                Map<String, Object> data = event.getBefore();
                data.put(this.noPKVirtualFieldName, toHash(keys, data, true));
                data = event.getAfter();
                data.put(this.noPKVirtualFieldName, toHash(keys, data, true));
                return true;
            };
            deleteRecordEventPredicate = event -> {
                Map<String, Object> data = event.getBefore();
                data.put(this.noPKVirtualFieldName, toHash(keys, data, true));
                return true;
            };
        }

        boolean apply(TapRecordEvent event) {
            if (event instanceof TapInsertRecordEvent) {
                return insertRecordEventPredicate.test((TapInsertRecordEvent) event);
            } else if (event instanceof TapUpdateRecordEvent) {
                return updateRecordEventPredicate.test((TapUpdateRecordEvent) event);
            } else if (event instanceof TapDeleteRecordEvent) {
                return deleteRecordEventPredicate.test((TapDeleteRecordEvent) event);
            }
            return true;
        }

        protected String toHash(List<String> keys, Map<String, Object> data, boolean isStrictMode) {
            try {
                MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    boolean isFirst = true;
                    baos.write('[');
                    for (String key : keys) {
                        if (isStrictMode && !data.containsKey(key)) {
                            throw NoPrimaryKeyException.incompleteFields(key);
                        }
                        if (isFirst) {
                            isFirst = false;
                        } else {
                            baos.write(SPLIT_CHAR);
                        }

                        Object val = data.get(key);
                        byte[] bytes = toBytes(val);
                        baos.write(bytes);
                    }
                    baos.write(']');

                    byte[] hashBytes = md.digest(baos.toByteArray());
                    StringBuilder hashHex = new StringBuilder();
                    for (byte b : hashBytes) {
                        hashHex.append(String.format("%02x", b));
                    }
                    return hashHex.toString(); // 返回 128 位（32 个字符）的哈希值
                }
            } catch (NoPrimaryKeyException e) {
                updateRecordEventPredicate = event -> false;
                deleteRecordEventPredicate = event -> false;
                throw e;
            } catch (NoSuchAlgorithmException e) {
                updateRecordEventPredicate = event -> false;
                deleteRecordEventPredicate = event -> false;
                insertRecordEventPredicate = event -> false;
                throw NoPrimaryKeyException.notfoundHashAlgorithm(HASH_ALGORITHM, e);
            } catch (Exception e) {
                updateRecordEventPredicate = event -> false;
                deleteRecordEventPredicate = event -> false;
                insertRecordEventPredicate = event -> false;
                throw NoPrimaryKeyException.otherFailed(e);
            }
        }

        protected byte[] toBytes(Object data) throws IOException {
            if (null == data) return new byte[0];
            if (data instanceof byte[]) return (byte[]) data;
            if (data.getClass().isArray()) return arrayToBytes(Arrays.asList((Object[]) data));
            if (data instanceof Collection) return arrayToBytes((Collection<?>) data);
            if (data instanceof Map) return mapToBytes((Map<?, ?>) data);
            return data.toString().getBytes();
        }

        protected byte[] arrayToBytes(Collection<?> collection) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                boolean isFirst = true;
                baos.write('[');
                for (Object o : collection) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        baos.write(SPLIT_CHAR);
                    }
                    baos.write(toBytes(o));
                }
                baos.write(']');
                return baos.toByteArray();
            }
        }

        protected byte[] mapToBytes(Map<?, ?> map) throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                baos.write('{');
                for (Map.Entry<?, ?> en : map.entrySet()) {
                    baos.write(toBytes(en.getKey()));
                    baos.write(':');
                    baos.write(toBytes(en.getValue()));
                }
                baos.write('}');
                return baos.toByteArray();
            }
        }

    }
}
