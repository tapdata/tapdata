package com.tapdata.tm.commons.util;

import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
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
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.pdk.core.utils.TapConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import io.tapdata.entity.schema.TapField;

import io.tapdata.entity.schema.TapTable;
import org.springframework.beans.BeanUtils;

@Slf4j
public class PdkSchemaConvert {

    public static final TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
    public static final TargetTypesGenerator targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);

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

                        tapType = InstanceFactory.instance(JsonParser.class).fromJson(field.getTapType(), new TypeHolder<TapType>() {
                        }, TapConstants.abstractClassDetectors);
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

                        tapType = InstanceFactory.instance(JsonParser.class).fromJson(field.getTapType(), new TypeHolder<TapType>() {
                        }, TapConstants.abstractClassDetectors);
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
                    BeanUtils.copyProperties(tapField, field);
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
                field.setTapType(tapField.getTapType() == null ? null : InstanceFactory.instance(JsonParser.class).toJson(tapField.getTapType()));

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
                    BeanUtils.copyProperties(tapField, field);
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
                field.setTapType(tapField.getTapType() == null ? null : InstanceFactory.instance(JsonParser.class).toJson(tapField.getTapType()));

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

}
