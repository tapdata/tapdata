package com.tapdata.tm.utils;

import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.bean.ForeignKeyTable;
import com.tapdata.tm.commons.schema.bean.Relation;
import com.tapdata.tm.commons.schema.bean.RelationField;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.Table;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2021/9/18
 * @Description:
 */
public class SchemaTransformUtils {


    //老模型转换成新模型
    public static List<MetadataInstancesDto> oldSchema2newSchema(Schema schema) {
        List<MetadataInstancesDto> newSchema = new ArrayList<>();
        if (schema == null || CollectionUtils.isEmpty(schema.getTables())) {
            return newSchema;
        }

        List<Table> tables = schema.getTables();

        for (Table table : tables) {
            MetadataInstancesDto newObj = new MetadataInstancesDto();
            newObj.setOriginalName(table.getTableName());
            newObj.setMetaType(StringUtils.isNotBlank(table.getMetaType()) ? table.getMetaType() : MetaType.collection.name());
            List<Field> fields = new ArrayList<>();
            Relation relation = new Relation();
            List<Field> tableFields = table.getFields();
            List<Relation> relaArr = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(tableFields)) {
                for (Field oldField : tableFields) {
                    Pair<Integer, Relation> relaObj = getRelation(relation, oldField, tables);

                    relation = relaObj.getValue();
                    relaArr.add(relation);
                    Field newField = buildField(oldField);
                    newField.setDataCode(oldField.getDataType1());
                    newField.setForeignKeyPosition(relaObj.getKey());
                    newField.setUnique(newField.getPrimaryKeyPosition() > 0 || oldField.isUnique());
                    newField.setJavaType(oldField.getJavaType1());
                    newField.setPkConstraintName(oldField.getPkConstraintName1());
                    newField.setOriginalJavaType(StringUtils.isNotBlank(oldField.getOriginalJavaType()) ? oldField.getOriginalJavaType() : StringUtils.isNotBlank(oldField.getJavaType1()) ? oldField.getJavaType1() : "");
                    fields.add(newField);
                }
            }



            //TODO 看不懂下面这段
            //let relaArr = [];
            //for (let x in relation) {
            //    relaArr.push(relation[x]);
            //}

            newObj.setRelation(relaArr);
            newObj.setFields(fields);
            newObj.setIndices(CollectionUtils.isNotEmpty(table.getIndices()) ? table.getIndices() : new ArrayList<>());
            newObj.setSchemaVersion(table.getSchemaVersion());
            newObj.setPartitionSet(table.getPartitionSet());
            //TODO  fileMeta
            newObj.setFileProperty(table.getFileProperty());//TODO JAVA TYPE
            newSchema.add(newObj);
        }

        return newSchema;
    }

    private static Field buildField(Field oldField) {
        Field newField = new Field();
        BeanUtils.copyProperties(oldField, newField);

        if (oldField.getIsNullable() instanceof  String) {
            newField.setIsNullable("YES".equals(oldField.getIsNullable()));// 非空
        } else {
            newField.setIsNullable(oldField.getIsNullable());// 非空
        }
        newField.setKey(StringUtils.isNotBlank(oldField.getKey()) ? oldField.getKey() : "");
        newField.setPkConstraintName(StringUtils.isNotBlank(oldField.getPkConstraintName()) ? oldField.getPkConstraintName() : "");
        newField.setId(StringUtils.isNotBlank(oldField.getId()) ? oldField.getId() : ObjectId.get().toString());
        newField.setOriginalFieldName(StringUtils.isNotBlank(oldField.getOriginalFieldName()) ? oldField.getOriginalFieldName() : StringUtils.isNotBlank(oldField.getFieldName()) ? oldField.getFieldName() : "");
        newField.setAutoincrement(oldField.getAutoincrement() != null ? oldField.getAutoincrement() : "NO");
        return newField;
    }

    public static Schema newSchema2oldSchema(List<MetadataInstancesDto> tables) {
        List<Table> oldTables = new ArrayList<>();
        for (MetadataInstancesDto newTable : tables) {
            Table oldTable = new Table();
            oldTable.setTableName(newTable.getOriginalName());
            oldTable.setCdcEnabled(true);
            oldTable.setMetaType(newTable.getMetaType());
            oldTable.setTableId(newTable.getId().toHexString());
            oldTable.setPartitionSet(newTable.getPartitionSet());

            List<Field> fields = newTable.getFields();
            List<Field> oldFields = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(fields)) {
                for (Field field : fields) {
                    if (field.isDeleted()) {
                        continue;
                    }
                    Field oldField = buildField(field);
                    oldField.setDataType1(field.getDataCode());
                    oldField.setTableName(newTable.getOriginalName());
                    oldField.setNodeDataType("");
                    oldField.setOriginalJavaType(StringUtils.isNotBlank(oldField.getOriginalJavaType()) ? oldField.getOriginalJavaType() : StringUtils.isNotBlank(oldField.getJavaType()) ? oldField.getJavaType() : "");
                    oldField.setPkConstraintName1(field.getPkConstraintName());
                    Map<String, String> foreign = getForeign(newTable, oldField.getFieldName());
                    oldField.setForeignKeyTable(foreign.get("foreign_key_table"));
                    oldField.setForeignKeyColumn(foreign.get("foreign_key_column"));
                    oldFields.add(oldField);
                }
            }

            oldTable.setFields(oldFields);
            oldTable.setIndices(CollectionUtils.isNotEmpty(newTable.getIndices()) ? newTable.getIndices() : new ArrayList<>());
            //TODO  fileMeta 这个字段暂时还没有处理
            oldTable.setFileProperty(newTable.getFileProperty());//TODO 暂时还不知道这个类型是什么
            oldTables.add(oldTable);
        }
        if (CollectionUtils.isNotEmpty(tables)) {
            Schema schema = new Schema();
            schema.setTables(oldTables);
            return schema;
        }
        return null;
    }


    public static Pair<Integer, Relation> getRelation(Relation relation, Field oldField, List<Table> tables) {
        if (relation == null) {
            return ImmutablePair.of(null, null);
        }
        String foreignKeyTable = oldField.getForeignKeyTable();
        String foreignKeyColumn = oldField.getForeignKeyColumn();

        if (StringUtils.isBlank(foreignKeyTable) || StringUtils.isBlank(foreignKeyColumn)) {
            return ImmutablePair.of(0, relation);
        }

        Integer position = null;
        Relation returnRelation = null;

        if (CollectionUtils.isNotEmpty(tables)) {
            for (Table table : tables) {
                if (relation.getForeignKeyTable() == null) {
                    ForeignKeyTable keyTable = new ForeignKeyTable();
                    keyTable.setId(table.getTableName());
                    keyTable.setRel(table.getOneone());
                    keyTable.setFields(new ArrayList<>());
                    relation.setForeignKeyTable(keyTable);
                }

                RelationField relationField = new RelationField();
                relationField.setForeign(foreignKeyColumn);
                relationField.setLocal(oldField.getFieldName());
                relation.getForeignKeyTable().getFields().add(relationField);

                List<Field> fields = table.getFields();
                if (CollectionUtils.isNotEmpty(fields)) {
                    for (int i = 0; i < fields.size(); i++) {
                        Field field = fields.get(i);
                        if (field.getFieldName().equals(foreignKeyColumn)) {
                            position = i;
                            break;
                        }
                    }
                }

                returnRelation = relation;
                break;

            }
        }
        return ImmutablePair.of(position, returnRelation);
    }

    private static Map<String, String> getForeign(MetadataInstancesDto newTable, String fieldName) {
        List<Relation> relations = newTable.getRelation();
        Map<String, String> map = new HashMap<>();
        if (CollectionUtils.isEmpty(relations)) {
            map.put("foreign_key_table", "");
            map.put("foreign_key_column", "");
            return map;
        }

        for (Relation relation : relations) {
            List<RelationField> relationFields = relation.getRelationFields();
            if (CollectionUtils.isNotEmpty(relationFields)) {
                for (RelationField relationField : relationFields) {
                    if (relationField.getLocal().equals(fieldName)) {
                        map.put("foreign_key_table", relation.getId());
                        map.put("foreign_key_column", relationField.getForeign());
                    }
                }

            }

        }
        map.put("foreign_key_table", "");
        map.put("foreign_key_column", "");
        return map;
    }

}
