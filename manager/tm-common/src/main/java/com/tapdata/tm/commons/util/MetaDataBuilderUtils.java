package com.tapdata.tm.commons.util;

import com.google.common.base.Joiner;
import com.mongodb.ConnectionString;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.*;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.lang.NonNull;

import java.util.*;


/**
 * @Author: Zed
 * @Date: 2021/9/10
 * @Description:
 */
public class MetaDataBuilderUtils {

    private static final String QUALIFIED_NAME_SEPARATOR = "_";
    private static final String QUALIFIED_NAME_SPECIAL_CHARACTERS = "[/.@&:?=%\\s]+";

    public static final Map<String, MetaTypeProperty> metaTypePropertyMap;

    static {
        metaTypePropertyMap = new HashMap<>();
        metaTypePropertyMap.put(MetaType.database.name(), new MetaTypeProperty("CONN_", false));
        metaTypePropertyMap.put(MetaType.job.name(), new MetaTypeProperty("JOB_", false));
        metaTypePropertyMap.put(MetaType.dataflow.name(), new MetaTypeProperty("FLOW_", false));
        metaTypePropertyMap.put(MetaType.api.name(), new MetaTypeProperty("API_", false));
        metaTypePropertyMap.put(MetaType.collection.name(), new MetaTypeProperty("MC_", true));
        metaTypePropertyMap.put(MetaType.mongo_view.name(), new MetaTypeProperty("MV_", true));
        metaTypePropertyMap.put(MetaType.table.name(), new MetaTypeProperty("T_", true));
        metaTypePropertyMap.put(MetaType.view.name(), new MetaTypeProperty("V_", true));
        metaTypePropertyMap.put(MetaType.file.name(), new MetaTypeProperty("F_", true));
        metaTypePropertyMap.put(MetaType.directory.name(), new MetaTypeProperty("CONN_", false));
        metaTypePropertyMap.put(MetaType.ftp.name(), new MetaTypeProperty("CONN_", false));
        metaTypePropertyMap.put(MetaType.apiendpoint.name(), new MetaTypeProperty("CONN_", false));
        metaTypePropertyMap.put(MetaType.processor_node.name(), new MetaTypeProperty("PN_", true));
    }

    public static String generateQualifiedName(String metaType, String nodeId, String tableName) {
        return generateQualifiedName(metaType, nodeId, tableName, null);
    }

    /** stay task id*/
    public static String generateQualifiedName(String metaType, String nodeId, String tableName, String taskId) {
        if (StringUtils.isBlank(tableName)) {
            return metaTypePropertyMap.get(metaType).prefix + nodeId;
        } else {
            return metaTypePropertyMap.get(metaType).prefix + nodeId + QUALIFIED_NAME_SEPARATOR + tableName;
        }
    }

    public static String generatePdkQualifiedName(@NonNull String metaType, @NonNull String connId, @NonNull String tableName
            , @NonNull String definitionPdkId, @NonNull String definitionGroup, @NonNull String definitionVersion, String taskId) {

        String qualifiedName = metaTypePropertyMap.get(metaType).getPrefix();

        Joiner joiner = Joiner.on(QUALIFIED_NAME_SEPARATOR).skipNulls();
        if (StringUtils.isBlank(tableName)) {
            tableName = null;
        }

        if (StringUtils.isBlank(taskId)) {
            taskId = null;
        }
        qualifiedName += joiner.join(definitionPdkId, definitionGroup, definitionVersion, tableName, connId, taskId);
        qualifiedName = qualifiedName.replaceAll(QUALIFIED_NAME_SPECIAL_CHARACTERS, QUALIFIED_NAME_SEPARATOR);
        return qualifiedName;
    }

    public static String generateQualifiedName(String metaType, DataSourceConnectionDto connectionDto, String tableName) {
        return generateQualifiedName(metaType, connectionDto, tableName, null);
    }
    public static String generateQualifiedName(String metaType, DataSourceConnectionDto connectionDto, String tableName, String taskId) {

        String id = connectionDto.getId().toHexString();
        if (DataSourceDefinitionDto.PDK_TYPE.equals(connectionDto.getPdkType())) {
            return generatePdkQualifiedName(metaType, id, tableName, connectionDto.getDefinitionPdkId(), connectionDto.getDefinitionGroup(), connectionDto.getDefinitionVersion(), taskId);
        }

        String qualifiedName = metaTypePropertyMap.get(metaType).getPrefix();
        if (metaTypePropertyMap.get(metaType).isModel()) {
            String databaseType = connectionDto.getDatabase_type();
            String databaseName;
            String databaseOwner = "";

            if (DataSourceEnum.mongodb.name().equals(databaseType)) {
                ConnectionString connectionString = new ConnectionString(connectionDto.getDatabase_uri());
                databaseName = connectionString.getDatabase();
            } else {
                databaseName = connectionDto.getDatabase_name();
                databaseOwner = connectionDto.getDatabase_owner();
            }

            qualifiedName += databaseType + QUALIFIED_NAME_SEPARATOR;

            if (StringUtils.isNotBlank(databaseName)) {
                qualifiedName += databaseName + QUALIFIED_NAME_SEPARATOR;
            }
            if (StringUtils.isNotBlank(databaseOwner)) {
                qualifiedName += databaseOwner + QUALIFIED_NAME_SEPARATOR;
            }
            qualifiedName += tableName + QUALIFIED_NAME_SEPARATOR + id;
            if (StringUtils.isNotBlank(taskId)) {
                qualifiedName += "_" + taskId;
            }

        } else {
            if ("api".equals(metaType)) {
                String basePath = StringUtils.isNotBlank(connectionDto.getBasePath()) ? connectionDto.getBasePath() : connectionDto.getPath();
                String apiVersion = connectionDto.getApiVersion();
                qualifiedName += basePath + QUALIFIED_NAME_SEPARATOR + apiVersion + QUALIFIED_NAME_SEPARATOR;
            }
            qualifiedName += id;
        }
        qualifiedName = qualifiedName.replaceAll(QUALIFIED_NAME_SPECIAL_CHARACTERS, QUALIFIED_NAME_SEPARATOR);

        return qualifiedName;
    }

    @Data
    public static class MetaTypeProperty {
        private String prefix;
        private boolean model;

        public MetaTypeProperty(String prefix, boolean model) {
            this.prefix = prefix;
            this.model = model;
        }
    }


    public static MetadataInstancesDto build(String metaType, DataSourceConnectionDto source, String userId, String userName) {
        return build(metaType, source, userId, userName, null, null, null, null, null, null, null);
    }
    public static MetadataInstancesDto build(String metaType, DataSourceConnectionDto source, String userId, String userName, String taskId) {
       return build(metaType, source, userId, userName, null, null, null, null, null, null, taskId);
    }


    public static MetadataInstancesDto build(String metaType, DataSourceConnectionDto source, String userId, String userName, String tableName
            , MetadataInstancesDto newModel, MetadataInstancesDto oldModel, String databaseId) {
        return build(metaType, source, userId, userName, tableName, newModel, oldModel, databaseId, null);

    }

    public static MetadataInstancesDto build(String metaType, DataSourceConnectionDto source, String userId, String userName, String tableName
            , MetadataInstancesDto newModel, MetadataInstancesDto oldModel, String databaseId, String taskId) {
        return build(metaType, source, userId, userName, tableName, newModel, oldModel, databaseId, null, null, taskId);
    }

    public static MetadataInstancesDto build(String metaType, DataSourceConnectionDto source, String userId, String userName, String tableName
            , MetadataInstancesDto newModel, MetadataInstancesDto oldModel, String databaseId, String createSource, Map<String, String> qualifiedNameMap) {
        return build(metaType, source, userId, userName, tableName, newModel, oldModel, databaseId, createSource, qualifiedNameMap, null);
    }
    public static MetadataInstancesDto build(String metaType, DataSourceConnectionDto source, String userId, String userName, String tableName
            , MetadataInstancesDto newModel, MetadataInstancesDto oldModel, String databaseId, String createSource, Map<String, String> qualifiedNameMap, String taskId) {

        if (qualifiedNameMap == null) {
            qualifiedNameMap = new HashMap<>();
        }

        MetadataInstancesDto metadataObj = new MetadataInstancesDto();
        createSource = StringUtils.isNotBlank(createSource) ? createSource : "auto";

        SourceDto sourceDto = new SourceDto();
        BeanUtils.copyProperties(source, sourceDto);
        sourceDto.set_id(source.getId().toHexString());

        // General properties
        handleSource(sourceDto);
        metadataObj.setMetaType(metaType);
        metadataObj.setOriginalName(sourceDto.getName());
        String qualifiedName = qualifiedNameMap.get(tableName);
        metadataObj.setQualifiedName(StringUtils.isBlank(qualifiedName) ? generateQualifiedName(metaType, source, tableName, taskId) : qualifiedName);
        metadataObj.setSource(sourceDto);
        metadataObj.setDevVersion(1);

        if (CollectionUtils.isNotEmpty(sourceDto.getListtags()) && !metaTypePropertyMap.get(metaType).model) {
            metadataObj.setListtags(sourceDto.getListtags());
        }

        metadataObj.setLastUpdBy(userId);
        metadataObj.setLastUserName(userName);

        if (metaTypePropertyMap.get(metaType).model) {

            if (newModel != null) {
                if (newModel.getMetaType() == null) {
                    newModel.setMetaType(metadataObj.getMetaType());
                }
                if (newModel.getQualifiedName() == null) {
                    newModel.setQualifiedName(metadataObj.getQualifiedName());
                }
                if (newModel.getSource() == null) {
                    newModel.setSource(metadataObj.getSource());
                }
                if (newModel.getDevVersion() == null) {
                    newModel.setDevVersion(metadataObj.getDevVersion());
                }
                if (newModel.getListtags() == null) {
                    newModel.setListtags(metadataObj.getListtags());
                }
                if (newModel.getLastUpdBy() == null) {
                    newModel.setLastUpdBy(metadataObj.getLastUpdBy());
                }
                if (newModel.getLastUserName() == null) {
                    newModel.setLastUserName(metadataObj.getLastUserName());
                }
                BeanUtils.copyProperties(newModel, metadataObj);
            }

            metadataObj.setDatabaseId(databaseId);
            metadataObj.setSource(sourceDto);
            metadataObj.setTaskId(taskId);

            if (sourceDto.getLoadSchemaField() != null && sourceDto.getLoadSchemaField()) {

                metadataObj.setCreateSource(createSource);
                if (newModel != null && CollectionUtils.isNotEmpty(newModel.getFields())) {
                    for (Field field : newModel.getFields()) {
                        field.setId(StringUtils.isBlank(field.getId()) ? ObjectId.get().toString() : field.getId());
                            /*field.setIsAutoAllowed(createSource.equals("auto"));
                            field.setSource(createSource);*/
                        field.setIsAutoAllowed(createSource.equals("auto") || createSource.equals("job_analyze"));
                        if (StringUtils.isBlank(field.getSource())) {
                            field.setSource(createSource);
                        }
                    }
                } else {
                    if (oldModel != null && CollectionUtils.isNotEmpty(oldModel.getFields())) {
                        metadataObj.setFields(oldModel.getFields());
                    }
                    for (Field field : metadataObj.getFields()) {
                        field.setDeleted(true);
                    }
                }

            } else {
                if (oldModel != null && CollectionUtils.isNotEmpty(oldModel.getFields())) {
                    metadataObj.setFields(oldModel.getFields());
                }
            }

            if (metadataObj.getVirtual() != null && metadataObj.getVirtual()) {
                metadataObj.setDeleted(true);
            }

        } else {
            MetaType switchValue = MetaType.valueOf(metaType);
            switch (switchValue) {
                case database:
                    String databaseType = sourceDto.getDatabase_type();
                    if ("file".equals(databaseType)) {
                        String protocal = sourceDto.getFile_source_protocol();
                        switch (protocal) {
                            case "localFile":
                            case "smb":
                                metadataObj.setMetaType(MetaType.directory.name());
                                break;
                            case "ftp":
                                metadataObj.setMetaType(MetaType.ftp.name());
                                break;
                        }
                    } else if ("rest api".equals(databaseType)) {
                        metadataObj.setMetaType(MetaType.apiendpoint.name());
                    }
                    break;
                case api:
                    String basePath = StringUtils.isNotBlank(sourceDto.getBasePath()) ? sourceDto.getBasePath() : sourceDto.getPath();
                    String apiVersion = sourceDto.getApiVersion();
                    String s = StringUtils.isNotBlank(sourceDto.getName()) ? sourceDto.getName() : StringUtils.isNotBlank(sourceDto.getDescription()) ? sourceDto.getDescription() : basePath;
                    metadataObj.setOriginalName(basePath + "_" + apiVersion);
                    break;
                default:
                    break;

            }
        }

        // set originalDefaultValue originalPrecision originalScale
        if (CollectionUtils.isNotEmpty(metadataObj.getFields())) {
            metadataObj.getFields().forEach(field -> {
                if (Objects.isNull(field.getOriginalDefaultValue())) {
                    field.setOriginalDefaultValue(field.getDefaultValue());
                }
                if (Objects.isNull(field.getOriginalPrecision())) {
                    field.setOriginalPrecision(field.getPrecision());
                }
                if (Objects.isNull(field.getOriginalScale())) {
                    field.setOriginalScale(field.getScale());
                }
            });
        }

        if (StringUtils.isBlank(metadataObj.getSourceType())) {
            metadataObj.setSourceType(SourceTypeEnum.SOURCE.name());
        }

        return metadataObj;
    }


    private static void handleSource(SourceDto source) {
        if (source == null) {
            return;
        }

        // 清理一些不需要的属性
        source.setSchema(null);
        source.setEditorData(null);
        source.setResponse_body(null);
        source.setListtags(null);
        //TODO objectUtil.deepFilterBlank(source);
    }


    private static void mergeModel(MetadataInstancesDto newModel, MetadataInstancesDto oldModel, String metaType, SourceDto source) {
        List<Field> newFields = newModel.getFields() == null ? new ArrayList<>() : newModel.getFields();
        for (Field newField : newFields) {
            newField.setDeleted(false);
        }
        List<Field> oldFields = oldModel.getFields() == null ? new ArrayList<>() : oldModel.getFields();
        boolean collectionPkIsNotOid = false;
        if (MetaType.collection.name().equals(metaType)) {
            for (Field oldField : oldFields) {
                if (!"_id".equals(oldField.getFieldName()) && oldField.getPrimaryKeyPosition() != null && oldField.getPrimaryKeyPosition() > 0) {
                    collectionPkIsNotOid = true;
                    break;
                }
            }

            if (collectionPkIsNotOid) {
                for (Field newField : newFields) {
                    if ("_id".equals(newField.getFieldName())) {
                        newField.setPrimaryKeyPosition(0);
                        break;
                    }
                }
            }
        }

        for (Field oldField : oldFields) {
            boolean fieldIsFound = false;

            List<Integer> removeIndexs = new ArrayList<>();
            for (int i = 0; i < newFields.size(); i++) {
                Field newField = newFields.get(i);
                if (oldField.getFieldName().equals(newField.getFieldName())) {
                    if (oldField.getIsAutoAllowed()) {
                        String oldId = oldField.getId();
                        BeanUtils.copyProperties(newField, oldField);
                        oldField.setDeleted(false);
                        if (StringUtils.isNotBlank(oldId)) {
                            oldField.setId(oldId);
                        }
                    }

                    fieldIsFound = true;
                    removeIndexs.add(i);
                    break;
                }
            }
            for (Integer removeIndex : removeIndexs) {
                newFields.remove(removeIndex.intValue());
            }


            if (!fieldIsFound && (oldField.getIsAutoAllowed() || !DataSourceEnum.mongodb.name().equals(source.getDatabase_type()))) {
                oldField.setDeleted(true);
            }
        }

        for (Field newField : newFields) {
            newField.setIsAutoAllowed(true);
            newField.setSource("auto");
        }

        oldFields.addAll(newFields);

        List<Relation> oldRelations = CollectionUtils.isNotEmpty(oldModel.getRelation()) ? oldModel.getRelation() : new ArrayList<>();
        List<Relation> newRelations = CollectionUtils.isNotEmpty(newModel.getRelation()) ? newModel.getRelation() : new ArrayList<>();


        List<Integer> oldRelationIndexs = new ArrayList<>();
        for (Relation oldRelation : oldRelations) {
            for (int j = 0; j < newRelations.size(); j++) {
                Relation newRelation = newRelations.get(j);

                if (StringUtils.isNotBlank(oldRelation.getTableName()) && oldRelation.getTableName().equals(newRelation.getTableName())) {

                    List<RelationField> oldRelFields = oldRelation.getRelationFields();
                    List<RelationField> newRelFields = newRelation.getRelationFields();


                    for (RelationField oldRelField : oldRelFields) {
                        List<Integer> newRelFieldsIndexs = new ArrayList<>();
                        for (int l = 0; l < newRelFields.size(); l++) {
                            RelationField newRelField = newRelFields.get(l);

                            if (oldRelField.getLocal().equals(newRelField.getLocal())
                                    && oldRelField.getForeign().equals(newRelField.getForeign())) {
                                newRelFieldsIndexs.add(l);
                                break;
                            }
                        }
                        for (Integer newRelFieldsIndex : newRelFieldsIndexs) {
                            newRelFields.remove(newRelFieldsIndex.intValue());
                        }
                    }

                    for (int l = 0; l < newRelFields.size(); l++) {
                        List<Integer> newRelFieldsIndexs = new ArrayList<>();
                        for (Field oldField : oldFields) {
                            if (oldField.getFieldName().equals(newRelFields.get(l).getForeign())) {
                                if (!oldField.getIsAutoAllowed() || !DataSourceEnum.mongodb.name().equals(source.getDatabase_type())) {
                                    newRelFieldsIndexs.add(l);
                                    //TODO l--;
                                }
                                break;
                            }
                        }
                        for (Integer newRelFieldsIndex : newRelFieldsIndexs) {
                            newRelFields.remove(newRelFieldsIndex.intValue());
                        }
                    }

                    oldRelFields.addAll(newRelFields);
                    oldRelationIndexs.add(j);
                    break;
                }
            }
        }
        for (Integer oldRelationIndex : oldRelationIndexs) {
            oldRelations.remove(oldRelationIndex.intValue());
        }
        newModel.setFields(oldFields);
        oldRelations.addAll(newRelations);
        newModel.setRelation(oldRelations);
    }

}

