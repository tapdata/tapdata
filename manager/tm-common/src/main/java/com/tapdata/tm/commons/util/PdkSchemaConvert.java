package com.tapdata.tm.commons.util;

import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.bean.TapFieldEx;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import io.tapdata.entity.schema.TapField;

import io.tapdata.entity.schema.TapTable;
import org.springframework.beans.BeanUtils;

@Slf4j
public class PdkSchemaConvert {

    //存在类加载顺序导致这几个为空，所以下面加了静态方法补救措施
    public static TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
    public static TargetTypesGenerator targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);

    private static  JsonParser instance = InstanceFactory.instance(JsonParser.class);


    public static TableFieldTypesGenerator getTableFieldTypesGenerator() {
        //存在类加载顺序导致这几个为空，所以下面加了静态方法补救措施
        if (tableFieldTypesGenerator == null) {
            tableFieldTypesGenerator =  InstanceFactory.instance(TableFieldTypesGenerator.class);
        }
        return tableFieldTypesGenerator;
    }

    public static TargetTypesGenerator getTargetTypesGenerator() {
        //存在类加载顺序导致这几个为空，所以下面加了静态方法补救措施
        if (targetTypesGenerator == null) {
            targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
        }
        return targetTypesGenerator;
    }

    public static JsonParser getJsonParser() {
        //存在类加载顺序导致这几个为空，所以下面加了静态方法补救措施
        if (instance == null) {
            instance = InstanceFactory.instance(JsonParser.class);
        }
        return instance;
    }

    public static TapTable toPdk(MetadataInstancesDto schema) {
        TapTable tapTable = new TapTable(schema.getOriginalName() == null ? null : schema.getOriginalName());
        //通过databaseId查询到pdk到相关熟悉
        tapTable.pdkId(schema.getPdkId());
        tapTable.pdkGroup(schema.getPdkGroup());
        tapTable.pdkVersion(schema.getPdkVersion());
        tapTable.setStorageEngine(schema.getStorageEngine());
        tapTable.setCharset(schema.getCharset());
        if (CollectionUtils.isNotEmpty(schema.getIndices())) {
            List<TapIndex> tapIndexList = schema.getIndices().stream().map(in -> {
                TapIndex tapIndex = new TapIndex();
                List<TapIndexField> tapIndexFields = in.getColumns().stream().map(i -> {
                    TapIndexField tapIndexField = new TapIndexField();
                    tapIndexField.setFieldAsc(i.getColumnIsAsc());
                    tapIndexField.setName(i.getColumnName());
                    return tapIndexField;
                }).collect(Collectors.toList());

                tapIndex.setIndexFields(tapIndexFields);
                tapIndex.setUnique(in.isUnique());
                tapIndex.setPrimary("true".equals(in.getPrimaryKey()));
                tapIndex.setName(in.getIndexName());

                return tapIndex;
            }).collect(Collectors.toList());
            tapTable.setIndexList(tapIndexList);
        }

        List<Field> fields = schema.getFields();

        Set<Integer> partitionSet = schema.getPartitionSet();
        if (partitionSet == null) {
            partitionSet = new HashSet<>();
        }


        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();

        if (CollectionUtils.isNotEmpty(fields)) {
            for (Field field : fields) {
                TapFieldEx tapField = new TapFieldEx();
                BeanUtils.copyProperties(field, tapField);

                tapField.setId(field.getId());
                tapField.setDefaultValue(field.getDefaultValue());
                tapField.setNullable((Boolean) field.getIsNullable());
                tapField.setName(field.getFieldName());
                //tapField.setPartitionKeyPos(field.get);
                tapField.setPos(field.getColumnPosition());
                tapField.setPrimaryKeyPos(field.getPrimaryKeyPosition());
                tapField.setForeignKeyTable(field.getForeignKeyTable());
                tapField.setForeignKeyField(field.getForeignKeyColumn());
                tapField.setAutoInc("YES".equals(field.getAutoincrement()));
                //tapField.setAutoIncStartValue(dd);
                //tapField.setCheck(field.);
                tapField.setComment(field.getComment());
                tapField.setConstraint(field.getPkConstraintName());
                tapField.setPrimaryKey(field.getPrimaryKey());
                tapField.setPartitionKey(partitionSet.contains(field.getColumnPosition()));
                tapField.setDataType(field.getDataType());
                if (StringUtils.isNotBlank(field.getTapType())) {
                    TapType tapType = null;
                    try {

                        Class<? extends TapType> classByJson = getClassByJson(field.getTapType());
                        tapType = getJsonParser().fromJson(field.getTapType(), classByJson);
                    } catch (Exception e) {
                        tapType = JsonUtil.parseJsonUseJackson(field.getTapType(), new TypeReference<TapType>() {
                        });
                    }

                    tapField.setTapType(tapType);

                }
                nameFieldMap.put(field.getFieldName(), tapField);
            }
        }
        tapTable.setNameFieldMap(nameFieldMap);
        tapTable.setLastUpdate(schema.getLastUpdate());
        return tapTable;
    }


//    public static TapTable toPdk(Schema schema) {
//        MetadataInstancesDto metadataInstances = InstanceFactory.instance(JsonParser.class).fromJson(InstanceFactory.instance(JsonParser.class).toJson(schema), new TypeHolder<MetadataInstancesDto>() {
//        }, TapConstants.abstractClassDetectors);
//        return toPdk(metadataInstances);
//    }

    public static TapTable toPdk(Schema schema) {
        long start = System.currentTimeMillis();
        TapTable tapTable = new TapTable(schema.getOriginalName() == null ? null : schema.getOriginalName());
        //通过databaseId查询到pdk到相关熟悉
        tapTable.pdkId(schema.getPdkId());
        tapTable.pdkGroup(schema.getPdkGroup());
        tapTable.pdkVersion(schema.getPdkVersion());
        tapTable.setStorageEngine(schema.getStorageEngine());
        tapTable.setCharset(schema.getCharset());
        if (CollectionUtils.isNotEmpty(schema.getIndices())) {
            List<TapIndex> tapIndexList = schema.getIndices().stream().map(in -> {
                TapIndex tapIndex = new TapIndex();
                List<TapIndexField> tapIndexFields = in.getColumns().stream().map(i -> {
                    TapIndexField tapIndexField = new TapIndexField();
                    tapIndexField.setFieldAsc(i.getColumnIsAsc());
                    tapIndexField.setName(i.getColumnName());
                    return tapIndexField;
                }).collect(Collectors.toList());

                tapIndex.setIndexFields(tapIndexFields);
                tapIndex.setUnique(in.isUnique());
                tapIndex.setPrimary("true".equals(in.getPrimaryKey()));
                tapIndex.setName(in.getIndexName());

                return tapIndex;
            }).collect(Collectors.toList());
            tapTable.setIndexList(tapIndexList);
        } else if (schema.getIndices() != null) {
            tapTable.setIndexList(Lists.newArrayList());
        }

        List<Field> fields = schema.getFields();

        Set<Integer> partitionSet = schema.getPartitionSet();
        if (partitionSet == null) {
            partitionSet = new HashSet<>();
        }


        LinkedHashMap<String, TapField> nameFieldMap = new LinkedHashMap<>();
        if (CollectionUtils.isNotEmpty(fields)) {

            for (Field field : fields) {

                TapFieldEx tapField = new TapFieldEx();
                tapField.setId(field.getId());
                tapField.setAliasName(field.getAliasName());
                tapField.setVisible(field.getVisible());
                tapField.setOriginalDataType(field.getOriginalDataType());
                tapField.setField_type(field.getField_type());
                tapField.setRequired(field.getRequired());
                tapField.setExample(field.getExample());
                tapField.setDictionary(field.getDictionary());
                tapField.setOldIdList(field.getOldIdList());
                tapField.setNodeDataType(field.getNodeDataType());
                tapField.setOriPrecision(field.getOriPrecision());
                tapField.setOriScale(field.getOriScale());
                tapField.setOriginalFieldName(field.getOriginalFieldName());
                tapField.setOriginalJavaType(field.getOriginalJavaType());
                tapField.setParent(field.getParent());
                tapField.setPrecision(field.getPrecision());
                tapField.setPrecisionEdit(field.getIsPrecisionEdit());
                tapField.setScaleEdit(field.getIsScaleEdit());
                tapField.setJavaType(field.getJavaType());
                tapField.setJavaType1(field.getJavaType1());
                tapField.setDataCode(field.getDataCode());
                tapField.setDataType1(field.getDataType1());
                tapField.setCreateSource(field.getCreateSource());
                tapField.setSource(field.getSource());
                tapField.setSourceDbType(field.getSourceDbType());
                tapField.setAutoincrement(field.getAutoincrement());
                tapField.setColumnSize(field.getColumnSize());
                tapField.setDataTypeTemp(field.getDataTypeTemp());
                tapField.setOriginalDefaultValue(field.getOriginalDefaultValue());
                tapField.setFieldName(field.getFieldName());
                tapField.setForeignKeyPosition(field.getForeignKeyPosition());
                tapField.setAnalyze(field.getIsAnalyze());
                tapField.setAutoAllowed(field.getIsAutoAllowed());
                tapField.setIsNullable(field.getIsNullable());
                tapField.setOriginalPrecision(field.getOriginalPrecision());
                tapField.setPrimaryKey(field.getPrimaryKey());
                tapField.setPrimaryKeyPosition(field.getPrimaryKeyPosition());
                tapField.setScale(field.getScale());
                tapField.setOriginalScale(field.getOriginalScale());
                tapField.setKey(field.getKey());
                tapField.setPkConstraintName(field.getPkConstraintName());
                tapField.setPkConstraintName1(field.getPkConstraintName1());
                tapField.setForeignKey(field.getForeignKey());
                tapField.setForeignKeyColumn(field.getForeignKeyColumn());
                tapField.setTableName(field.getTableName());
                tapField.setColumnPosition(field.getColumnPosition());
                tapField.setDefaultValue(field.getDefaultValue());
                tapField.setForeignKeyTable(field.getForeignKeyTable());
                tapField.setAutoInc(field.getIsAutoAllowed());
                tapField.setComment(field.getComment());
                tapField.setPrimaryKey(field.getPrimaryKey());
                tapField.setDataType(field.getDataType());
                tapField.setDeleted(field.isDeleted());

                tapField.setId(field.getId());
                tapField.setDefaultValue(field.getDefaultValue());
                tapField.setNullable((Boolean) field.getIsNullable());
                tapField.setName(field.getFieldName());
                //tapField.setPartitionKeyPos(field.get);
                tapField.setPos(field.getColumnPosition());
                tapField.setPrimaryKeyPos(field.getPrimaryKeyPosition());
                tapField.setForeignKeyTable(field.getForeignKeyTable());
                tapField.setForeignKeyField(field.getForeignKeyColumn());
                tapField.setAutoInc("YES".equals(field.getAutoincrement()));
                //tapField.setAutoIncStartValue(dd);
                //tapField.setCheck(field.);
                tapField.setComment(field.getComment());
                tapField.setConstraint(field.getPkConstraintName());
                tapField.setPrimaryKey(field.getPrimaryKey());
                tapField.setPartitionKey(partitionSet.contains(field.getColumnPosition()));

                tapField.setDataType(field.getDataType());
                if (StringUtils.isNotBlank(field.getTapType())) {
                    TapType tapType = null;
                    try {

                        Class<? extends TapType> classByJson = getClassByJson(field.getTapType());
                        tapType = getJsonParser().fromJson(field.getTapType(), classByJson);
                    } catch (Exception e) {
                        tapType = JsonUtil.parseJsonUseJackson(field.getTapType(), new TypeReference<TapType>() {
                        });
                    }

                    tapField.setTapType(tapType);

                }
                nameFieldMap.put(field.getFieldName(), tapField);
            }

        }
        tapTable.setNameFieldMap(nameFieldMap);
        tapTable.setLastUpdate(schema.getLastUpdate());

        return tapTable;
    }


    public static MetadataInstancesDto fromPdk(TapTable tapTable) {
        MetadataInstancesDto schema = new MetadataInstancesDto();
        schema.setName(tapTable.getId());
        schema.setOriginalName(tapTable.getName());
        schema.setMetaType("table");

        schema.setPdkId(tapTable.getPdkId());
        schema.setPdkGroup(tapTable.getPdkGroup());
        schema.setPdkVersion(tapTable.getPdkVersion());
        schema.setCharset(tapTable.getCharset());
        schema.setStorageEngine(tapTable.getStorageEngine());


        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        List<Field> fields = new ArrayList<>();
        Set<Integer> partitionSet = new HashSet<>();



        if (nameFieldMap != null && !nameFieldMap.isEmpty()) {
            for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
                Field field = new Field();
                TapField tapField = entry.getValue();
                if (tapField instanceof TapFieldEx) {
                    TapFieldEx tapField1 = (TapFieldEx) tapField;
                    field.setId(tapField1.getId());
                    field.setAliasName(tapField1.getAliasName());
                    field.setVisible(tapField1.getVisible());
                    field.setOriginalDataType(tapField1.getOriginalDataType());
                    field.setField_type(tapField1.getField_type());
                    field.setRequired(tapField1.getRequired());
                    field.setExample(tapField1.getExample());
                    field.setDictionary(tapField1.getDictionary());
                    field.setOldIdList(tapField1.getOldIdList());
                    field.setNodeDataType(tapField1.getNodeDataType());
                    field.setOriPrecision(tapField1.getOriPrecision());
                    field.setOriScale(tapField1.getOriScale());
                    field.setOriginalFieldName(tapField1.getOriginalFieldName());
                    field.setOriginalJavaType(tapField1.getOriginalJavaType());
                    field.setParent(tapField1.getParent());
                    field.setPrecision(tapField1.getPrecision());
                    field.setIsPrecisionEdit(tapField1.getPrecisionEdit());
                    field.setIsScaleEdit(tapField1.getScaleEdit());
                    field.setJavaType(tapField1.getJavaType());
                    field.setJavaType1(tapField1.getJavaType1());
                    field.setDataCode(tapField1.getDataCode());
                    field.setDataType1(tapField1.getDataType1());
                    field.setCreateSource(tapField1.getCreateSource());
                    field.setSource(tapField1.getSource());
                    field.setSourceDbType(tapField1.getSourceDbType());
                    field.setAutoincrement(tapField1.getAutoincrement());
                    field.setColumnSize(tapField1.getColumnSize());
                    field.setDataTypeTemp(tapField1.getDataTypeTemp());
                    field.setOriginalDefaultValue(tapField1.getOriginalDefaultValue());
                    field.setFieldName(tapField1.getFieldName());
                    field.setForeignKeyPosition(tapField1.getForeignKeyPosition());
                    field.setIsAnalyze(tapField1.getAnalyze());
                    field.setIsAutoAllowed(tapField1.getAutoAllowed());
                    field.setIsNullable(tapField1.getIsNullable());
                    field.setOriginalPrecision(tapField1.getOriginalPrecision());
                    field.setPrimaryKey(tapField1.getPrimaryKey());
                    field.setPrimaryKeyPosition(tapField1.getPrimaryKeyPosition());
                    field.setScale(tapField1.getScale());
                    field.setOriginalScale(tapField1.getOriginalScale());
                    field.setKey(tapField1.getKey());
                    field.setPkConstraintName(tapField1.getPkConstraintName());
                    field.setPkConstraintName1(tapField1.getPkConstraintName1());
                    field.setForeignKey(tapField1.getForeignKey());
                    field.setForeignKeyColumn(tapField1.getForeignKeyColumn());
                    field.setTableName(tapField1.getTableName());
                    field.setColumnPosition(tapField1.getColumnPosition());
                    field.setDefaultValue(tapField1.getDefaultValue());
                    field.setForeignKeyTable(tapField1.getForeignKeyTable());
                    field.setIsAutoAllowed(tapField1.getAutoAllowed());
                    field.setComment(tapField1.getComment());
                    field.setPrimaryKey(tapField1.getPrimaryKey());
                    field.setDataType(tapField1.getDataType());
                    field.setDeleted(tapField1.isDeleted());
                }
                field.setDefaultValue(tapField.getDefaultValue());
                field.setIsNullable(tapField.getNullable());
                field.setFieldName(tapField.getName());
                if (StringUtils.isBlank(field.getOriginalFieldName())) {
                    field.setOriginalFieldName(tapField.getName());
                }
                field.setColumnPosition(tapField.getPos());
                field.setPrimaryKeyPosition(tapField.getPrimaryKeyPos());
                field.setForeignKeyTable(tapField.getForeignKeyTable());
                field.setForeignKeyColumn(tapField.getForeignKeyField());
                field.setAutoincrement(tapField.getAutoInc() ? "YES" : "NO");
                field.setComment(tapField.getComment());
                field.setPkConstraintName(tapField.getConstraint());
                field.setPrimaryKey(tapField.getPrimaryKey());
                field.setTapType(tapField.getTapType() == null ? null : getJsonParser().toJson(tapField.getTapType()));

                if (tapField.getPartitionKey()) {
                    partitionSet.add(tapField.getPos());
                }
                field.setDataType(tapField.getDataType());
                fields.add(field);
            }
        }


        schema.setFields(fields);
        schema.setPartitionSet(partitionSet);
        schema.setLastUpdate(tapTable.getLastUpdate());


        List<TapIndex> indexList = tapTable.getIndexList();

        if (CollectionUtils.isNotEmpty(indexList)) {
            List<TableIndex> tableIndexList = indexList.stream().map(in -> {
                TableIndex tableIndex = new TableIndex();
                tableIndex.setIndexName(in.getName());
                tableIndex.setPrimaryKey(String.valueOf(in.isPrimary()));
                //tableIndex.setIndexType();
                //tableIndex.setIndexSourceType();
                tableIndex.setUnique(in.isUnique());
                List<TapIndexField> indexFields = in.getIndexFields();


                List<TableIndexColumn> tableIndexColumns = new ArrayList<>();
                for (TapIndexField indexField : indexFields) {
                    TableIndexColumn tableIndexColumn = new TableIndexColumn();
                    tableIndexColumn.setColumnName(indexField.getName());
                    tableIndexColumn.setColumnIsAsc(indexField.getFieldAsc());
                    tableIndexColumns.add(tableIndexColumn);
                }

                tableIndex.setColumns(tableIndexColumns);
                //tableIndex.setDbIndexDescriptionJson();
                //tableIndex.setPrimaryKey();
                //tableIndex.setClustered();

                return tableIndex;
            }).collect(Collectors.toList());
            schema.setIndices(tableIndexList);
        } else if (indexList != null) {
            schema.setIndices(Lists.newArrayList());
        }

        return schema;


    }

//    public static Schema fromPdkSchema(TapTable tapTable) {
//        MetadataInstancesDto metadataInstances = fromPdk(tapTable);
//        return InstanceFactory.instance(JsonParser.class).fromJson(InstanceFactory.instance(JsonParser.class).toJson(metadataInstances), new TypeHolder<Schema>() {
//        }, TapConstants.abstractClassDetectors);
//    }
    public static Schema fromPdkSchema(TapTable tapTable) {
        Schema schema = new Schema();
        schema.setName(tapTable.getId());
        schema.setOriginalName(tapTable.getName());
        schema.setMetaType("table");

        schema.setPdkId(tapTable.getPdkId());
        schema.setPdkGroup(tapTable.getPdkGroup());
        schema.setPdkVersion(tapTable.getPdkVersion());
        schema.setCharset(tapTable.getCharset());
        schema.setStorageEngine(tapTable.getStorageEngine());


        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        List<Field> fields = new ArrayList<>();
        Set<Integer> partitionSet = new HashSet<>();



        if (nameFieldMap != null && !nameFieldMap.isEmpty()) {
            for (Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
                Field field = new Field();
                TapField tapField = entry.getValue();
                if (tapField instanceof TapFieldEx) {
                    TapFieldEx tapField1 = (TapFieldEx) tapField;
                    field.setId(tapField1.getId());
                    field.setAliasName(tapField1.getAliasName());
                    field.setVisible(tapField1.getVisible());
                    field.setOriginalDataType(tapField1.getOriginalDataType());
                    field.setField_type(tapField1.getField_type());
                    field.setRequired(tapField1.getRequired());
                    field.setExample(tapField1.getExample());
                    field.setDictionary(tapField1.getDictionary());
                    field.setOldIdList(tapField1.getOldIdList());
                    field.setNodeDataType(tapField1.getNodeDataType());
                    field.setOriPrecision(tapField1.getOriPrecision());
                    field.setOriScale(tapField1.getOriScale());
                    field.setOriginalFieldName(tapField1.getOriginalFieldName());
                    field.setOriginalJavaType(tapField1.getOriginalJavaType());
                    field.setParent(tapField1.getParent());
                    field.setPrecision(tapField1.getPrecision());
                    field.setIsPrecisionEdit(tapField1.getPrecisionEdit());
                    field.setIsScaleEdit(tapField1.getScaleEdit());
                    field.setJavaType(tapField1.getJavaType());
                    field.setJavaType1(tapField1.getJavaType1());
                    field.setDataCode(tapField1.getDataCode());
                    field.setDataType1(tapField1.getDataType1());
                    field.setCreateSource(tapField1.getCreateSource());
                    field.setSource(tapField1.getSource());
                    field.setSourceDbType(tapField1.getSourceDbType());
                    field.setAutoincrement(tapField1.getAutoincrement());
                    field.setColumnSize(tapField1.getColumnSize());
                    field.setDataTypeTemp(tapField1.getDataTypeTemp());
                    field.setOriginalDefaultValue(tapField1.getOriginalDefaultValue());
                    field.setFieldName(tapField1.getFieldName());
                    field.setForeignKeyPosition(tapField1.getForeignKeyPosition());
                    field.setIsAnalyze(tapField1.getAnalyze());
                    field.setIsAutoAllowed(tapField1.getAutoAllowed());
                    field.setIsNullable(tapField1.getIsNullable());
                    field.setOriginalPrecision(tapField1.getOriginalPrecision());
                    field.setPrimaryKey(tapField1.getPrimaryKey());
                    field.setPrimaryKeyPosition(tapField1.getPrimaryKeyPosition());
                    field.setScale(tapField1.getScale());
                    field.setOriginalScale(tapField1.getOriginalScale());
                    field.setKey(tapField1.getKey());
                    field.setPkConstraintName(tapField1.getPkConstraintName());
                    field.setPkConstraintName1(tapField1.getPkConstraintName1());
                    field.setForeignKey(tapField1.getForeignKey());
                    field.setForeignKeyColumn(tapField1.getForeignKeyColumn());
                    field.setTableName(tapField1.getTableName());
                    field.setColumnPosition(tapField1.getColumnPosition());
                    field.setDefaultValue(tapField1.getDefaultValue());
                    field.setForeignKeyTable(tapField1.getForeignKeyTable());
                    field.setIsAutoAllowed(tapField1.getAutoAllowed());
                    field.setComment(tapField1.getComment());
                    field.setPrimaryKey(tapField1.getPrimaryKey());
                    field.setDataType(tapField1.getDataType());
                    field.setDeleted(tapField1.isDeleted());
                }
                field.setDefaultValue(tapField.getDefaultValue());
                field.setIsNullable(tapField.getNullable());
                field.setFieldName(tapField.getName());
                if (StringUtils.isBlank(field.getOriginalFieldName())) {
                    field.setOriginalFieldName(tapField.getName());
                }
                field.setColumnPosition(tapField.getPos());
                field.setPrimaryKeyPosition(tapField.getPrimaryKeyPos());
                field.setForeignKeyTable(tapField.getForeignKeyTable());
                field.setForeignKeyColumn(tapField.getForeignKeyField());
                field.setAutoincrement(tapField.getAutoInc() ? "YES" : "NO");
                field.setComment(tapField.getComment());
                field.setPkConstraintName(tapField.getConstraint());
                field.setPrimaryKey(tapField.getPrimaryKey());
                field.setTapType(tapField.getTapType() == null ? null : getJsonParser().toJson(tapField.getTapType()));

                if (tapField.getPartitionKey()) {
                    partitionSet.add(tapField.getPos());
                }
                field.setDataType(tapField.getDataType());
                fields.add(field);
            }
        }

        schema.setFields(fields);
        schema.setPartitionSet(partitionSet);
        schema.setLastUpdate(tapTable.getLastUpdate());


        List<TapIndex> indexList = tapTable.getIndexList();
        if (CollectionUtils.isNotEmpty(indexList)) {
            List<TableIndex> tableIndexList = indexList.stream().map(in -> {
                TableIndex tableIndex = new TableIndex();
                tableIndex.setIndexName(in.getName());
                tableIndex.setPrimaryKey(String.valueOf(in.isPrimary()));
                //tableIndex.setIndexType();
                //tableIndex.setIndexSourceType();
                tableIndex.setUnique(in.isUnique());
                List<TapIndexField> indexFields = in.getIndexFields();


                List<TableIndexColumn> tableIndexColumns = new ArrayList<>();
                for (TapIndexField indexField : indexFields) {
                    TableIndexColumn tableIndexColumn = new TableIndexColumn();
                    tableIndexColumn.setColumnName(indexField.getName());
                    tableIndexColumn.setColumnIsAsc(indexField.getFieldAsc());
                    tableIndexColumns.add(tableIndexColumn);
                }

                tableIndex.setColumns(tableIndexColumns);
                //tableIndex.setDbIndexDescriptionJson();
                //tableIndex.setPrimaryKey();
                //tableIndex.setClustered();

                return tableIndex;
            }).collect(Collectors.toList());
            schema.setIndices(tableIndexList);
        }

        return schema;
    }


    public static Class<? extends TapType> getClassByJson(String json) {
        TypeV typeV = JsonUtil.parseJson(json, TypeV.class);
        byte type = typeV.getType();
        return TapType.getTapTypeClass(type);
    }

    @Setter
    @Getter
    private static class TypeV{
        private byte type;
    }

}
