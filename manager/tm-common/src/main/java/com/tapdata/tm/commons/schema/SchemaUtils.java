package com.tapdata.tm.commons.schema;

import cn.hutool.core.collection.CollUtil;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import io.tapdata.entity.schema.TapConstraint;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/10 下午10:37
 */
public class SchemaUtils {
    public static final String SCHEMA_FREE = "schema-free";
    public static final String TAP_MAP = TapSimplify.toJson(new TapMap());
    public static final String TAP_ARRAY = TapSimplify.toJson(new TapArray());

    private static Logger log = LoggerFactory.getLogger(SchemaUtils.class);

    private static final String _ID = "_id";

    public static final String OBJECT_ID = "OBJECT_ID";

    private enum PriorityEnum {
        manual(3),
        auto(2),
        job_analyze(1),
        ;
        @Getter
        private final int v;

        PriorityEnum(int v) {
            this.v = v;
        }
    }

    /**
     * 多个模型合并为一个
     * @param inputSchemas 输入模型
     * @param schema 目标模型
     * @return 合并后的结果
     */
    public static Schema mergeSchema(List<Schema> inputSchemas, Schema schema) {
        return mergeSchema(inputSchemas, schema, true);
    }

    /**
     *
     * @param inputSchemas 输入
     * @param schema 原有表
     * @param logicInput 输入为逻辑表：true  输入为物理表:  false
     * @return 合并后的模型
     */
    public static Schema mergeSchema(List<Schema> inputSchemas, Schema schema, boolean logicInput) {
        List<Schema> _inputSchemas = inputSchemas.stream().filter(Objects::nonNull).collect(Collectors.toList());
        Schema targetSchema = cloneSchema(schema);

        if (targetSchema == null) {
            if (_inputSchemas.size() > 0) {
                targetSchema = _inputSchemas.remove(0);
            } else {
                log.warn("Can't merge non schema.");
                return null;
            }
        } else {
            if (!_inputSchemas.isEmpty()) {
                cloneSchemaInfo(_inputSchemas.get(0), targetSchema);
            }
            if (logicInput) {
                List<Field> fields = targetSchema.getFields();
                if (CollectionUtils.isNotEmpty(fields)) {
                    List<Field> removeList = new ArrayList<>();
                    for (Field field : fields) {
                        if (!"manual".equals(field.getSource())) {
                            removeList.add(field);
                        }
                    }
                    fields.removeAll(removeList);
                }
            }
        }

        if (targetSchema == null) {
            log.warn("Can't merge non schema.");
            return null;
        } else {
            if (CollectionUtils.isNotEmpty(inputSchemas)) {
                List<TableIndex> inputIndices = inputSchemas.stream()
                        .filter(Objects::nonNull)
                        .filter(s -> null != s.getIndices())
                        .flatMap(s -> s.getIndices().stream())
                        .filter(Objects::nonNull).collect(Collectors.toList());
                targetSchema.setIndices(inputIndices);
                List<TapConstraint> inputConstraints = inputSchemas.stream()
                        .filter(Objects::nonNull)
                        .filter(s -> null != s.getConstraints())
                        .flatMap(s -> s.getConstraints().stream())
                        .filter(Objects::nonNull).collect(Collectors.toList());
                targetSchema.setConstraints(inputConstraints);
            }
        }

        Map<String, Field> fields = Stream
                .concat(_inputSchemas.stream().flatMap(m ->
                                        (m != null && m.getFields() != null)
                                        ? m.getFields().stream() :
                                        Stream.empty()),
                targetSchema.getFields() != null ? targetSchema.getFields().stream(): Stream.empty())
                .collect(Collectors.toMap(Field::getFieldName, f -> f, (v, m) -> {
                    int vPriority = getPriority(v.getSource());
                    int mPriority = getPriority(m.getSource());
                    Field field;
                    //都为手动修改的时候，取后者更合理
                    if (mPriority == PriorityEnum.manual.v && vPriority == PriorityEnum.manual.v) {
                        field = m;
                    } else {
                        field = getPriority(v.getSource()) >= getPriority(m.getSource()) ? v : m;
                    }
                    return field;
                }));

        targetSchema.setFields(new ArrayList<>(fields.values()));
        List<String> sourceNodeDatabaseTypes = _inputSchemas.stream().map(Schema::getSourceNodeDatabaseType).distinct().collect(Collectors.toList());
        if (sourceNodeDatabaseTypes.size() > 0){
            targetSchema.setSourceNodeDatabaseType(sourceNodeDatabaseTypes.get(0));
        }

        return targetSchema;
    }

    protected static void cloneSchemaInfo(Schema inputSchema, Schema targetSchema) {
        targetSchema.setAncestorsName(inputSchema.getAncestorsName());
        targetSchema.setPartitionInfo(inputSchema.getPartitionInfo());
        targetSchema.setPartitionMasterTableId(inputSchema.getPartitionMasterTableId());
    }

    private static int getPriority(String source) {
        if (StringUtils.isBlank(source)) {
            return 0;
        }
        for (PriorityEnum value : PriorityEnum.values()) {
            if (value.name().equals(source)) {
                return value.v;
            }
        }

        return 0;
    }

    /**
     * 复制模型对象
     * @param source
     * @return
     */
    public static Schema cloneSchema(Schema source) {
        return InstanceFactory.instance(JsonParser.class).fromJson(InstanceFactory.instance(JsonParser.class).toJson(source), new TypeHolder<Schema>() {
        });
    }

    public static List<Schema> cloneSchema(List<Schema> schemas) {
        return InstanceFactory.instance(JsonParser.class)
                .fromJson(InstanceFactory.instance(JsonParser.class)
                        .toJson(schemas), new TypeHolder<List<Schema>>() {});
        //return JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(schemas), new TypeReference<List<Schema>>() {});
    }

    public static Field createField(String nodeId, String tableName, FieldProcessorNode.Operation operation) {
        Field field = new Field();
        field.setFieldName(operation.getField());
        field.setSource("manual");
        field.setDataType(operation.getType());
        field.setColumnSize(100);
        field.setOriPrecision(100);
        if (StringUtils.isNotBlank(operation.getId())) {
            field.setId(operation.getId());
        } else {
            field.setId(MetaDataBuilderUtils.generateFieldId(nodeId, tableName, operation.getField()));
        }
        field.setTableName(operation.getTableName());
        field.setOriginalFieldName(operation.getField());
        if (StringUtils.isNotBlank(operation.getJava_type())) {
            field.setJavaType(operation.getJava_type());
            field.setJavaType1(operation.getJava_type());
        }
				field.setColumnPosition(0);
        field.setIsNullable(true);
        return field;
    }


    public static void removeDeleteFields(List<Schema> inputSchema) {
        for (Schema schema : inputSchema) {
            List<Field> fields = schema.getFields();
            if (CollectionUtils.isNotEmpty(fields)) {
                fields = fields.stream().filter(f -> !f.isDeleted()).collect(Collectors.toList());
                schema.setFields(fields);
            }
        }
    }

    public static List<Schema> removeSubFieldsWhichFromFreeSchema(DataSourceConnectionDto dataSource, List<Schema> schemas) {
        if (!SchemaUtils.dataBaseIsFreeSchema(dataSource)) {
            return schemas;
        }
        schemas.stream().filter(Objects::nonNull).forEach(SchemaUtils::removeSubFieldsWhichFromFreeSchema);
        return schemas;
    }
    public static Schema removeSubFieldsWhichFromFreeSchema(DataSourceConnectionDto dataSource, Schema schema) {
        if (SchemaUtils.dataBaseIsFreeSchema(dataSource)) {
            return schema;
        }
        SchemaUtils.removeSubFieldsWhichFromFreeSchema(schema);
        return schema;
    }

    public static boolean dataBaseIsFreeSchema(DataSourceConnectionDto dataSource) {
        List<String> definitionTags = dataSource.getDefinitionTags();
        return CollUtil.isNotEmpty(definitionTags) && definitionTags.contains(SCHEMA_FREE);
    }

    public static void removeSubFieldsWhichFromFreeSchema(Schema schema) {
        List<Field> fields = schema.getFields();
        if (CollUtil.isEmpty(fields)) {
            return;
        }
        Map<String, Field> fieldMap = fields.stream()
                .collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));

        List<Field> afterFilterField = fieldMap.entrySet()
                .stream()
                .filter(entry -> SchemaUtils.isFreeSchemaFields(fieldMap, entry))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
        //4. Otherwise, delete the current attribute
        fields.removeAll(afterFilterField);
    }

    public static boolean isFreeSchemaFields(Map<String, Field> allFieldMap, Map.Entry<String, Field> fieldEntry) {
        String fieldName = fieldEntry.getKey();
        int index = fieldName.lastIndexOf(".");
        if (index <= 0) {
            return false;
        }
        //2. Traverse attribute names with a length greater than 2
        String fatherFieldName = fieldName.substring(0, index);
        //3. If the parent attribute does not exist, skip it
        if (!allFieldMap.containsKey(fatherFieldName)) {
            return false;
        }
        Field field = allFieldMap.get(fatherFieldName);
        String tapType = field.getTapType();
        //3. If the type of the parent property is not TapMap and TapArray, it will jump out
        return TAP_MAP.equals(tapType) || TAP_ARRAY.equals(tapType);
    }

    /**
     * 添加mongo数据库的_id字段
     * @param schema
     */
    public static void addFieldObjectIdIfMongoDatabase(Schema schema, String databaseType){
        List<Field> fields = schema.getFields();
        if (CollUtil.isEmpty(fields)) {
            return ;
        }
        AtomicBoolean primaryKey = new AtomicBoolean(false);
        Map<String, Field> fieldMap = fields.stream()
                .collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));
        if(fieldMap.containsKey(_ID)) return ;
        fieldMap.values().forEach(field -> {
            if(null != field.getPrimaryKey() && field.getPrimaryKey()){
                primaryKey.set(true);
            }
        });
        FieldProcessorNode.Operation fieldOperation = new FieldProcessorNode.Operation();
        fieldOperation.setType(OBJECT_ID);
        fieldOperation.setField(_ID);
        fieldOperation.setOp("CREATE");
        fieldOperation.setTableName(schema.getName());
        Field field = createField(schema.getNodeId(), schema.getOriginalName(), fieldOperation);
        field.setSource("job_analyze");
        field.setTapType( JsonUtil.toJsonUseJackson(new TapString(24L, false)));
        field.setDataType(OBJECT_ID);
        field.setDataTypeTemp(OBJECT_ID);
        field.setSourceDbType(databaseType);
        if(!primaryKey.get()){
            field.setPrimaryKey(true);
            field.setPrimaryKeyPosition(1);
        };
        schema.getFields().add(field);
    }
}
