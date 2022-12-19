package com.tapdata.tm.commons.dag.process;

import cn.hutool.core.lang.Assert;
import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@NodeType("union_processor")
@Getter
@Setter
public class UnionProcessorNode extends ProcessorNode{

    public UnionProcessorNode() {
        super(NodeEnum.union_processor.name());
    }

    @Override
    public Schema mergeSchema(List<Schema> inputSchemas, Schema schema) {
        if (schema != null) {
            return schema;
        }

        Assert.notEmpty(inputSchemas);

        for (int i = 0; i < inputSchemas.size(); i++) {
            Schema inputSchema = inputSchemas.get(i);

            Optional.ofNullable(inputSchema.getIndices()).ifPresent(indices -> indices.removeIf(TableIndex::isUnique));

            if (i == 0) {
                schema = inputSchema;
                continue;
            }
            // field -- start
            if (CollectionUtils.isEmpty(schema.getFields())) {
                schema.setFields(inputSchema.getFields());
            } else if (CollectionUtils.isNotEmpty(inputSchema.getFields())) {
                Map<String, Field> inputFieldMap = inputSchema.getFields().stream()
                        .collect(Collectors.toMap(field -> StringUtils.upperCase(field.getFieldName()),
                                Function.identity(), (e1, e2) -> e1));

                // compare tapType
                schema.getFields().forEach(field -> {
                    String fieldName = StringUtils.upperCase(field.getFieldName());
                    if (inputFieldMap.containsKey(fieldName)) {
                        Field inputField = inputFieldMap.get(fieldName);

                        Object[] inputBytes = getBytes(inputField.getTapType());
                        Object[] bytes = getBytes(field.getTapType());

                        if (inputBytes[0] != bytes[0]) {
                            TapString tapString = new TapString().bytes(Long.MAX_VALUE);
                            field.setTapType(JsonUtil.toJson(tapString));
                        } else if (Long.parseLong(inputBytes[1].toString()) > Long.parseLong(bytes[1].toString())) {
                            field.setTapType(inputField.getTapType());
                            field.setDataType(inputField.getDataType());
                            field.setDataTypeTemp(inputField.getDataTypeTemp());
                        }

                        if (field.getIsNullable() != inputFieldMap.get(field.getFieldName()).getIsNullable()) {
                            field.setIsNullable(false);
                        }

                        if (field.getPrimaryKey() != inputFieldMap.get(field.getFieldName()).getPrimaryKey()) {
                            field.setPrimaryKey(false);
                            field.setPrimaryKeyPosition(null);
                        }
                    } else {
                        field.setIsNullable(false);
                    }
                });

                // compare need add field
                Map<String, Field> fieldMap = schema.getFields().stream()
                        .collect(Collectors.toMap(field -> StringUtils.upperCase(field.getFieldName()),
                                Function.identity(), (e1, e2) -> e1));

                Schema finalSchema = schema;
                Consumer<Field> addFieldConsumer = (field) -> {
                   if (!fieldMap.containsKey(StringUtils.upperCase(field.getFieldName()))) {
                       finalSchema.getFields().add(field);
                   }
                };
                Optional.ofNullable(inputSchema.getFields()).ifPresent(fields -> fields.iterator().forEachRemaining(addFieldConsumer));
            }
            // field -- end

            // index -- start
            if (CollectionUtils.isEmpty(schema.getIndices())) {
                schema.setIndices(inputSchema.getIndices());
            } else {
                Map<String, TableIndex> indexMap = schema.getIndices().stream()
                        .collect(Collectors.toMap(TableIndex::getIndexName, Function.identity(), (e1, e2) -> e2));

                Schema finalSchema = schema;
                Consumer<TableIndex> indexConsumer = (index) -> {
                    if (!indexMap.containsKey(index.getIndexName())) {
                        finalSchema.getIndices().add(index);
                    }
                };

                Optional.ofNullable(inputSchema.getIndices()).ifPresent(indices -> indices.iterator().forEachRemaining(indexConsumer));
            }
            // index -- end
        }

        return schema;
    }

    private Object[] getBytes(String tapTypeJson) {
        TapType tapType = JsonUtil.parseJsonUseJackson(tapTypeJson, new TypeReference<TapType>() {});
        Class<? extends TapType> tapTypeClass = TapType.getTapTypeClass(tapType.getType());
        switch (Objects.requireNonNull(tapTypeClass).getSimpleName()) {
            case "TapBinary":
                TapBinary tapBinary = InstanceFactory.instance(JsonParser.class).fromJson(tapTypeJson, TapBinary.class);
                return new Object[]{tapBinary.getType(), tapBinary.getBytes()};
            case "TapDate":
                TapDate tapDate = InstanceFactory.instance(JsonParser.class).fromJson(tapTypeJson, TapDate.class);
                return new Object[]{tapDate.getType(), tapDate.getBytes()};
            case "TapDateTime":
                TapDateTime tapDateTime = InstanceFactory.instance(JsonParser.class).fromJson(tapTypeJson, TapDateTime.class);
                return new Object[]{tapDateTime.getType(), tapDateTime.getBytes()};
            case "TapNumber":
                TapNumber tapNumber = InstanceFactory.instance(JsonParser.class).fromJson(tapTypeJson, TapNumber.class);
                return new Object[]{tapNumber.getType(), Objects.nonNull(tapNumber.getBit()) ? tapNumber.getBit().longValue() : tapNumber.getPrecision().longValue()};
            case "TapString":
                TapString tapString = InstanceFactory.instance(JsonParser.class).fromJson(tapTypeJson, TapString.class);
                return new Object[]{tapString.getType(), tapString.getBytes()};
            case "TapTime":
                TapTime tapTime = InstanceFactory.instance(JsonParser.class).fromJson(tapTypeJson, TapTime.class);
                return new Object[]{tapTime.getType(), tapTime.getBytes()};
            case "TapYear":
                TapYear tapYear = InstanceFactory.instance(JsonParser.class).fromJson(tapTypeJson, TapYear.class);
                return new Object[]{tapYear.getType(), tapYear.getMax().longValue()};
        }
        return new Object[]{tapType.getType(), 0L};
    }
}
