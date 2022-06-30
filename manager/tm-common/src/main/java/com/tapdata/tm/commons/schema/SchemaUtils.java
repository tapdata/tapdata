package com.tapdata.tm.commons.schema;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.commons.dag.process.FieldProcessorNode;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.pdk.core.utils.TapConstants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/10 下午10:37
 */
public class SchemaUtils {

    private static Logger log = LoggerFactory.getLogger(SchemaUtils.class);
    private static Map<String, Integer> createSourcePriority = new HashMap<String, Integer>(){{
        put("manual", 3);
        put("auto", 2);
        put("job_analyze", 1);
    }};

    /**
     * 多个模型合并为一个
     * @param inputSchemas 输入模型
     * @param schema 目标模型
     * @return 合并后的结果
     */
    public static Schema mergeSchema(List<Schema> inputSchemas, Schema schema) {

        List<Schema> _inputSchemas = inputSchemas.stream().filter(Objects::nonNull).collect(Collectors.toList());
        Schema targetSchema = cloneSchema(schema);

        if (targetSchema == null) {
            if (_inputSchemas.size() > 0) {
                targetSchema = _inputSchemas.remove(0);
            } else {
                log.warn("Can't merge non schema.");
                return null;
            }
        }
        if (targetSchema == null) {
            log.warn("Can't merge non schema.");
            return null;
        }

        Schema finalTargetSchema = targetSchema;
        List<String> inputSchemaFieldIds = _inputSchemas.stream().flatMap(s -> s.getFields().stream())
                .map(Field::getId).filter(Objects::nonNull).collect(Collectors.toList());

        Map<String, Field> inputSchemaFieldMap = _inputSchemas.stream().flatMap(s -> s.getFields().stream())
                .filter(Objects::nonNull).collect(
                        Collectors.toMap(Field::getId, f->f, (v, m) -> getPriority(v.getSource()) > getPriority(m.getSource()) ? v : m));

            List<Field> targetFields = targetSchema.getFields();
        List<String> targetSchemaFieldIds = targetFields != null ? targetFields.stream().filter(Objects::nonNull)
                .map(Field::getId).collect(Collectors.toList()) : new ArrayList<>();
        Map<String, Field> targetSchemaFieldMap = targetFields != null ?
                targetFields.stream().filter(Objects::nonNull).collect(
                        Collectors.toMap(Field::getId, f->f, (v, m) -> getPriority(v.getSource()) > getPriority(m.getSource()) ? v : m)) :
                new HashMap<>();
        Map<String, Field> fields = Stream.concat(
                _inputSchemas.stream().flatMap(m ->
                        (m != null && m.getFields() != null) ?
                                m.getFields().stream().filter(f -> {
                                    if (!targetSchemaFieldIds.contains(f.getId())) {
                                        return true;
                                    }
                                    //都为手动修改的时候，取后者更合理
                                    int tPriority = getPriority(targetSchemaFieldMap.get(f.getId()).getSource());
                                    int sPriority = getPriority(f.getSource());
                                    if (tPriority == sPriority && tPriority == 3) {
                                        return false;
                                    }
                                    return getPriority(targetSchemaFieldMap.get(f.getId()).getSource()) <= getPriority(f.getSource());
                                }
                                ) :
                                Stream.empty()),
                targetSchema.getFields() != null ?
                        targetSchema.getFields().stream().filter(f -> {
                            if (!inputSchemaFieldIds.contains(f.getId())) {
                                return true;
                            }
                            //都为手动修改的时候，取后者更合理
                            int sPriority = getPriority(inputSchemaFieldMap.get(f.getId()).getSource());
                            int tPriority = getPriority(f.getSource());
                            if (tPriority == sPriority && tPriority == 3) {
                                return true;
                            }
                            return getPriority(inputSchemaFieldMap.get(f.getId()).getSource()) < getPriority(f.getSource());
                        })
                                :
                        Stream.empty())
                .collect(Collectors.toMap(Field::getFieldName, f -> f, (v, m) -> {

                    // 根据 模型来源优先级(create_source) 控制是否覆盖策略
                    // is_auto_allowed(字段配置) > 人工修改 > 加载模型 > 模型推演
                    //                            manual / auto / job_analyze
//                    if (v.getIsAutoAllowed())
//                        return v;
//                    if (m.getIsAutoAllowed())
//                        return m;
                    int vPriority = getPriority(v.getSource());
                    int mPriority = getPriority(m.getSource());
                    Field field;
                    //都为手动修改的时候，取后者更合理
                    if (mPriority == 3 && vPriority == 3) {
                        field = m;
                    } else {
                        field = getPriority(v.getSource()) >= getPriority(m.getSource()) ? v : m;
                    }
                    List<String> oldIdList = field.getOldIdList();

                    if (oldIdList == null) {
                        oldIdList = new ArrayList<>();
                    }

                    oldIdList.add(v.getId());
                    oldIdList.add(m.getId());
                    return field;

                }));

        targetSchema.setFields(new ArrayList<>(fields.values()));
        List<String> sourceNodeDatabaseTypes = _inputSchemas.stream().map(Schema::getSourceNodeDatabaseType).distinct().collect(Collectors.toList());
        if (sourceNodeDatabaseTypes.size() > 0){
            targetSchema.setSourceNodeDatabaseType(sourceNodeDatabaseTypes.get(0));
        }

        return targetSchema;
    }

    private static int getPriority(String source) {
        return source != null ? createSourcePriority.get(source) : 0;
    }

    /**
     * 复制模型对象
     * @param source
     * @return
     */
    public static Schema cloneSchema(Schema source) {

        return InstanceFactory.instance(JsonParser.class).fromJson(InstanceFactory.instance(JsonParser.class).toJson(source), new TypeHolder<Schema>() {
        }, TapConstants.abstractClassDetectors);
        //return JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(source), Schema.class);
    }

    public static Field createField(FieldProcessorNode.Operation operation) {
        Field field = new Field();
        field.setFieldName(operation.getField());
        field.setSource("manual");
        field.setDataType(operation.getType());
        field.setColumnSize(100);
        field.setOriPrecision(100);
        if (StringUtils.isNotBlank(operation.getId())) {
            field.setId(operation.getId());
        } else {
            field.setId(new ObjectId().toHexString());
        }
        field.setTableName(operation.getTableName());
        field.setOriginalFieldName(operation.getField());
        if (StringUtils.isNotBlank(operation.getJava_type())) {
            field.setJavaType(operation.getJava_type());
            field.setJavaType1(operation.getJava_type());
        }
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

}
