package com.tapdata.tm.commons.dag;

import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/10 下午2:15
 */
@Slf4j
public class DAGDataServiceImpl implements DAGDataService {

    private final Map<String, MetadataInstancesDto> metadataMap;
    private final Map<String, DataSourceConnectionDto> dataSourceMap;
    private final Map<String, DataSourceDefinitionDto> definitionDtoMap;

    private final Map<String, MetadataTransformerDto> transformerDtoMap;
    private final Map<String, TaskDto> taskMap;

    private final String userId;
    private final String userName;

    private final Map<String, Update> batchMetadataUpdateMap = new LinkedHashMap<>();

    private final List<MetadataInstancesDto> batchInsertMetaDataList = new ArrayList<>();


    private final List<MetadataTransformerItemDto> upsertItems = new ArrayList<>();
    private final List<MetadataTransformerDto> upsertTransformer = new ArrayList<>();



    public DAGDataServiceImpl(List<MetadataInstancesDto> metadataInstancesDtos, Map<String, DataSourceConnectionDto> dataSourceMap
            , Map<String, DataSourceDefinitionDto> definitionDtoMap, String userId, String userName, Map<String, TaskDto> taskMap
            , Map<String, MetadataTransformerDto> transformerDtoMap) {
        this.metadataMap = new HashMap<>();
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            setMetaDataMap(metadataInstancesDto);
        }
        this.dataSourceMap = dataSourceMap;
        this.definitionDtoMap = definitionDtoMap;
        this.taskMap = taskMap;
        this.userId = userId;
        this.userName = userName;
        this.transformerDtoMap = transformerDtoMap;
    }

    private void setMetaDataMap(MetadataInstancesDto metadataInstancesDto) {
        if (metadataInstancesDto.getId() != null) {
            metadataMap.put(metadataInstancesDto.getId().toHexString(), metadataInstancesDto);
        }
        if (StringUtils.isNotBlank(metadataInstancesDto.getQualifiedName())) {
            metadataMap.put(metadataInstancesDto.getQualifiedName(), metadataInstancesDto);
        }
        if (!"database".equals(metadataInstancesDto.getMetaType()) && metadataInstancesDto.getSource() != null && !metadataInstancesDto.getQualifiedName().startsWith("PN")) {
            metadataMap.put(metadataInstancesDto.getSource().get_id() + metadataInstancesDto.getName(), metadataInstancesDto);
        }
    }

    private Schema convertToSchema(MetadataInstancesDto metadataInstances) {
        if (metadataInstances == null)
            return null;
        Schema schema = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(metadataInstances), Schema.class);
        schema.setFields(schema.getFields()/*.stream().filter(field -> field.getParent() == null).collect(Collectors.toList())*/);

        // 这里需要 执行字段映射，将 data_type 转换为 通用字段类型，设置到 data_type 字段（不要回写原模型）
        // 修改后的字段类型保存在schema里面
        processFieldFromDB(metadataInstances, schema);
        return schema;
    }

    @Override
    public Schema loadSchema(String ownerId, ObjectId dataSourceId, String tableName) {

        if (ownerId == null || dataSourceId == null || tableName == null) {
            log.error("Can't load schema by params: {}, {}, {}", ownerId, dataSourceId, tableName);
            return null;
        }
//
//        //UserDetail userDetail = userService.loadUserById(toObjectId(ownerId));
//
////        if (userDetail == null) {
////            log.error("Load schema failed, not found user by id {}", ownerId);
////            return null;
////        }
//
//        Criteria criteria = Criteria
//                .where("meta_type").in("table", "collection", "view")
//                .and("original_name").is(tableName)
//                .and("is_deleted").ne(true)
//                .and("source._id").is(dataSourceId.toHexString());
        String key = dataSourceId + tableName;

        MetadataInstancesDto metadataInstances = metadataMap.get(key);

        return convertToSchema(metadataInstances);
    }

    @Override
    public List<Schema> loadSchema(String ownerId, ObjectId dataSourceId, List<String> includes, List<String> excludes) {

        if (ownerId == null || dataSourceId == null) {
            log.error("Can't load schema by params: {}, {}, {}, {}", ownerId, dataSourceId, includes, excludes);
            return null;
        }

        //UserDetail userDetail = userService.loadUserById(toObjectId(ownerId));
//
//        if (userDetail == null) {
//            log.error("Load schema failed, not found user by id {}", ownerId);
//            return null;
//        }

        DataSourceConnectionDto dataSource = dataSourceMap.get(dataSourceId);

        if (dataSource == null) {
            log.error("Load schema failed, not found connection by id {}", dataSourceId.toHexString());
            return null;
        }

        Criteria criteria = Criteria
                .where("meta_type").in("table", "collection", "view")
                .and("is_deleted").ne(true)
                .orOperator(Criteria.where("source.id").is(dataSourceId.toHexString()),
                        Criteria.where("source.id").is(dataSourceId),
                        Criteria.where("source._id").is(dataSourceId),
                        Criteria.where("source._id").is(dataSourceId.toHexString())
                ).and("source.database_name").is(dataSource.getDatabase_name());

        boolean includeNotEmpty = includes != null && includes.size() > 0;
        boolean excludeNotEmpty = excludes != null && excludes.size() > 0;
        if (includeNotEmpty && excludeNotEmpty) {
            criteria.andOperator(Criteria.where("original_name").in(includes),
                    Criteria.where("original_name").nin(excludes));
        } else if (includeNotEmpty) {
            criteria.and("original_name").in(includes);
        } else if (excludeNotEmpty) {
            criteria.and("original_name").nin(excludes);
        }

        List<MetadataInstancesDto> metadataInstances = new ArrayList<>();
        for (String key : metadataMap.keySet()) {
            if (key.contains(dataSourceId.toHexString())) {
                MetadataInstancesDto metadataInstancesDto = metadataMap.get(key);
                if (metadataInstancesDto != null) {
                    metadataInstances.add(metadataInstancesDto);
                }
            }
        }

        long start = System.currentTimeMillis();
        List<Schema> result = metadataInstances.stream().map(this::convertToSchema).collect(Collectors.toList());
        log.debug("Convert metadata instances record to Schema cost {} millisecond", System.currentTimeMillis() - start);
        return result;
    }

    @Override
    public List<Schema> createOrUpdateSchema(String ownerId, ObjectId dataSourceId, List<Schema> schemas, DAG.Options options, Node node) {

        log.info("Create or update schema: ownerId={}, dataSourceId={}, schemaCount={}, options={}", ownerId,
                dataSourceId, schemas != null ? schemas.size() : null,options);

        if (schemas == null || ownerId == null) {
            log.error("Save schema failed, param can not be empty: {}, {}, {}", ownerId, dataSourceId, schemas);
            return Collections.emptyList();
        }

        //UserDetail userDetail = userService.loadUserById(toObjectId(ownerId));

//        if (userDetail == null) {
//            log.error("Save schema failed, not found user by id {}", ownerId);
//            return Collections.emptyList();
//        }

        //为了获取原表的id才讲schema这个实体加上了id这个属性，但是原来是没有这个属性的，可能导致保存的时候id重复，因为是由原来的模型复制过来的
        for (Schema schema : schemas) {
            schema.setId(null);


            //去掉重复的字段id
            List<Field> fields = schema.getFields();
            Set<String> set = new HashSet<>();
            for (Field field : fields) {
                if (set.contains(field.getId())) {
                    List<String> oldIdList = field.getOldIdList();
                    if (oldIdList == null) {
                        field.setOldIdList(new ArrayList<>());
                    }
                    field.getOldIdList().add(field.getId());
                    field.setId(new ObjectId().toHexString());
                }
                set.add(field.getId());
            }
        }


        if (node.isDataNode()) {
            return createOrUpdateSchemaForDataNode(dataSourceId, schemas, options);
        } else {
            return createOrUpdateSchemaForProcessNode(schemas, options, node.getId());
        }
    }

    /**
     * 保存处理器节点模型
     * @param schemas 模型
     * @param options 配置项
     * @return
     */
    private List<Schema> createOrUpdateSchemaForProcessNode(List<Schema> schemas, DAG.Options options, String nodeId) {

        List<ObjectId> databaseMetadataInstancesIds = schemas.stream().map(Schema::getDatabaseId).distinct()
                .filter(Objects::nonNull).map(ObjectId::new).collect(Collectors.toList());

        List<MetadataInstancesDto> databaseMetadataInstances = new ArrayList<>();
        for (ObjectId databaseMetadataInstancesId : databaseMetadataInstancesIds) {
            databaseMetadataInstances.add(metadataMap.get(databaseMetadataInstancesId.toHexString()));
        }

        MetadataInstancesDto dataSourceMetadataInstance =
                databaseMetadataInstances.size() > 0 ? databaseMetadataInstances.get(0) : null;

        if (dataSourceMetadataInstance == null) {
            log.error("Save schema failed, can't not found data source by id {}", databaseMetadataInstancesIds);
            return Collections.emptyList();
        }

        SourceDto sourceDto = dataSourceMetadataInstance.getSource();
        DataSourceConnectionDto dataSource = dataSourceMap.get(sourceDto.get_id());

        // 其他类型的 meta type 暂时不做模型推演处理
        final String _metaType = MetaType.processor_node.name();
        List<MetadataInstancesDto> metadataInstancesDtos = schemas.stream().map(schema -> {
            MetadataInstancesDto metadataInstancesDto =
                    JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(schema), MetadataInstancesDto.class);

            // 这里需要将 data_type 字段根据字段类型映射规则转换为 数据库类型
            //   需要 根据 所有可匹配条件，尽量缩小匹配结果，选择最优字段类型
            metadataInstancesDto = processFieldToDB(schema, metadataInstancesDto, dataSource);

            metadataInstancesDto.setMetaType(_metaType);
            metadataInstancesDto.setDeleted(false);
            metadataInstancesDto.setSource(sourceDto);

            // 需要将 Connections 的id 转换为 metadata instance 中 meta_type=database 的 id
            //   使用 qualified name 作为条件查询
            metadataInstancesDto.setDatabaseId(dataSourceMetadataInstance.getId().toHexString());

            metadataInstancesDto.setCreateSource("job_analyze");
            metadataInstancesDto.setLastUserName(userName);
            metadataInstancesDto.setLastUpdBy(userId);

            metadataInstancesDto.setQualifiedName(
                    MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId));

            MetaDataBuilderUtils.build(_metaType, dataSource, userId, userName, metadataInstancesDto.getOriginalName(),
                    metadataInstancesDto, null, dataSource.getId().toHexString());

            metadataInstancesDto.setSourceType(SourceTypeEnum.VIRTUAL.name());

            /*MetadataInstancesDto result = metadataInstancesService.upsertByWhere(
                    Where.where("qualified_name", metadataInstancesDto.getQualifiedName()), metadataInstancesDto, userDetail);*/
            return metadataInstancesDto;
        }).collect(Collectors.toList());

        String rollback = options != null ? options.getRollback() : null;
        String rollbackTable = options != null ? options.getRollbackTable() : null;
        long start = System.currentTimeMillis();

        Map<String, MetadataInstancesDto> existsMetadataInstances = rollbackOperation(metadataInstancesDtos, rollback, rollbackTable);
        int modifyCount = bulkSave(metadataInstancesDtos, dataSource, existsMetadataInstances);
        log.info("Bulk save metadataInstance {}, cost {}ms", modifyCount, System.currentTimeMillis() - start);

        return metadataInstancesDtos.stream()
                .map(dto -> JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dto), Schema.class)).collect(Collectors.toList());
    }

    /**
     * 保存数据节点模型
     * @param dataSourceId 数据源id
     * @param schemas 模型
     * @param options 配置项
     * @return
     */
    private List<Schema> createOrUpdateSchemaForDataNode(ObjectId dataSourceId, List<Schema> schemas, DAG.Options options) {
        DataSourceConnectionDto dataSource = dataSourceMap.get(dataSourceId.toHexString());

        if (dataSource == null) {
            log.error("Save schema failed, can't not found data source by id {}", dataSourceId);
            return Collections.emptyList();
        }

        if (DataSourceDefinitionDto.PDK_TYPE.equals(dataSource.getPdkType())) {
            DataSourceDefinitionDto definitionDto = definitionDtoMap.get(dataSource.getDatabase_type());
            if (definitionDto != null) {
                dataSource.setDefinitionScope(definitionDto.getScope());
                dataSource.setDefinitionGroup(definitionDto.getGroup());
                dataSource.setDefinitionVersion(definitionDto.getVersion());
                dataSource.setDefinitionPdkId(definitionDto.getPdkId());
                dataSource.setDefinitionBuildNumber(String.valueOf(definitionDto.getBuildNumber()));
            }
        }

        String databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", dataSource, null);
        MetadataInstancesDto dataSourceMetadataInstance = metadataMap.get(databaseQualifiedName);

        if (dataSourceMetadataInstance == null) {
            log.error("Save schema failed, can't not found metadata for data source {}({})", databaseQualifiedName, dataSource.getId());
            return Collections.emptyList();
        }

        SourceDto sourceDto = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dataSource), SourceDto.class);

        // 其他类型的 meta type 暂时不做模型推演处理
        String metaType = "table";
        if ("mongodb".equals(dataSource.getDatabase_type())) {
            metaType = "collection";
        }

        final String _metaType = metaType;
        List<MetadataInstancesDto> metadataInstancesDtos = schemas.stream().map(schema -> {
            MetadataInstancesDto metadataInstancesDto =
                    JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(schema), MetadataInstancesDto.class);

            // 这里需要将 data_type 字段根据字段类型映射规则转换为 数据库类型
            //   需要 根据 所有可匹配条件，尽量缩小匹配结果，选择最优字段类型
            metadataInstancesDto = processFieldToDB(schema, metadataInstancesDto, dataSource);

            metadataInstancesDto.getFields().forEach(field -> {
                field.setSourceDbType(dataSource.getDatabase_type());
                field.setDataTypeTemp(field.getDataType());
            });

            metadataInstancesDto.setMetaType(_metaType);
            metadataInstancesDto.setDeleted(false);
            metadataInstancesDto.setSource(sourceDto);

            // 需要将 Connections 的id 转换为 metadata instance 中 meta_type=database 的 id
            //   使用 qualified name 作为条件查询
            metadataInstancesDto.setDatabaseId(dataSourceMetadataInstance.getId().toHexString());

            metadataInstancesDto.setCreateSource("job_analyze");
            metadataInstancesDto.setLastUserName(userName);
            metadataInstancesDto.setLastUpdBy(userId);

            metadataInstancesDto.setQualifiedName(
                    MetaDataBuilderUtils.generateQualifiedName(metadataInstancesDto.getMetaType(), dataSource, schema.getOriginalName()));

            MetaDataBuilderUtils.build(_metaType, dataSource, userId, userName, metadataInstancesDto.getOriginalName(),
                    metadataInstancesDto, null, dataSourceId.toHexString());

            metadataInstancesDto.setSourceType(SourceTypeEnum.VIRTUAL.name());

            /*MetadataInstancesDto result = metadataInstancesService.upsertByWhere(
                    Where.where("qualified_name", metadataInstancesDto.getQualifiedName()), metadataInstancesDto, userDetail);*/
            return metadataInstancesDto;
        }).collect(Collectors.toList());

        String rollback = options != null ? options.getRollback() : null;
        String rollbackTable = options != null ? options.getRollbackTable() : null;
        long start = System.currentTimeMillis();

        Map<String, MetadataInstancesDto> existsMetadataInstances = rollbackOperation(metadataInstancesDtos, rollback, rollbackTable);
        int modifyCount = bulkSave(metadataInstancesDtos, dataSource, existsMetadataInstances);
        log.info("Bulk save metadataInstance {}, cost {}ms", modifyCount, System.currentTimeMillis() - start);



        return metadataInstancesDtos.stream()
                .map(dto -> JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dto), Schema.class)).collect(Collectors.toList());
    }

    private Map<String, MetadataInstancesDto> rollbackOperation(List<MetadataInstancesDto> metadataInstancesDtos, String rollback, String rollbackTable) {

        List<String> qualifiedNames = metadataInstancesDtos.stream().map(MetadataInstancesDto::getQualifiedName)
                .filter(Objects::nonNull).collect(Collectors.toList());

        Map<String, MetadataInstancesDto> existsMetadataInstances = new HashMap<>();

        for (String qualifiedName : qualifiedNames) {
            MetadataInstancesDto metadataInstancesDto = metadataMap.get(qualifiedName);
            if (metadataInstancesDto != null) {
                existsMetadataInstances.put(qualifiedName, metadataInstancesDto);
            }
        }
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            MetadataInstancesDto existsMetadataInstance = existsMetadataInstances.get(metadataInstancesDto.getQualifiedName());
            if (existsMetadataInstance == null) {
                continue;
            }
            Map<String, Field> existsFieldMap = existsMetadataInstance.getFields().stream()
                    .collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));

            if ("all".equalsIgnoreCase(rollback) ||
                    ("table".equalsIgnoreCase(rollback) &&
                            metadataInstancesDto.getOriginalName().equalsIgnoreCase(rollbackTable))) {

                metadataInstancesDto.getFields().forEach(field -> {
                    if (existsFieldMap.containsKey(field.getOriginalFieldName())) {
                        Field existsField = existsFieldMap.get(field.getOriginalFieldName());
                        if ("manual".equalsIgnoreCase(existsField.getSource())
                            /* && existsField.getIsAutoAllowed() != null&& !existsField.getIsAutoAllowed()*/) {
                            boolean deleted = field.isDeleted();
                            BeanUtils.copyProperties(existsField, field);
                            field.setFieldName(field.getOriginalFieldName());
                            field.setJavaType(field.getOriginalJavaType());
                            //field.setPrecision(((Double) field.getOriPrecision()).intValue());
                            field.setDataType(field.getOriginalDataType());
                            field.setDeleted(deleted);
                            // set originalDefaultValue originalPrecision originalScale
                            field.setDefaultValue(field.getOriginalDefaultValue());
                            field.setPrecision(field.getOriginalPrecision());
                            field.setScale(field.getOriginalScale());
                        }
                        field.setId(existsField.getId());
                    }
                });
            }
            metadataInstancesDto.getFields().forEach(field -> {
                if (existsFieldMap.containsKey(field.getOriginalFieldName())) {
                    Field existsField = existsFieldMap.get(field.getOriginalFieldName());
                    field.setId(existsField.getId());
                }
                if (StringUtils.isBlank(field.getId())) {
                    field.setId(new ObjectId().toHexString());
                }
            });
        }

        return existsMetadataInstances;
    }

    /**
     * 根据字段类型映射规则，将模型中的数据库类型转换为通用类型
     * @param metadataInstances 模型元数据记录
     * @param schema 映射后的字段类型将保存在这个对象上
     */
    private Schema processFieldFromDB(MetadataInstancesDto metadataInstances, Schema schema) {
        String dbVersion = metadataInstances.getSource().getDb_version();
        if (StringUtils.isBlank(dbVersion)) {
            dbVersion = "*";
        }


        String databaseType = metadataInstances.getSource().getDatabase_type();
        //Map<String, List<TypeMappingsEntity>> typeMapping = typeMappingsService.getTypeMapping(databaseType, TypeMappingDirection.TO_TAPTYPE);

        if (schema.getFields() == null) {
            schema.setFields(new ArrayList<>());
        }

        DataSourceDefinitionDto definitionDto = definitionDtoMap.get(databaseType);
        String expression = definitionDto.getExpression();

        TapTable tapTable = PdkSchemaConvert.toPdk(schema);

        PdkSchemaConvert.tableFieldTypesGenerator.autoFill(tapTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));

        //这里最好是将那些旧参数也带过来
        Schema schema1 = PdkSchemaConvert.fromPdkSchema(tapTable);

        schema.setFields(schema1.getFields());

        schema.setInvalidFields(new ArrayList<>());
        //String finalDbVersion = dbVersion;
        schema.getFields().forEach(field -> {
            String originalDataType = field.getDataType();
            if (StringUtils.isBlank(field.getOriginalDataType())) {
                field.setOriginalDataType(originalDataType);
            }
            field.setDataTypeTemp(originalDataType);

//            String cacheKey = originalDataType + "-" + finalDbVersion;
//            List<TypeMappingsEntity> typeMappings = null;
//            if (typeMapping.containsKey(cacheKey)) {
//                typeMappings = typeMapping.get(cacheKey);
//            } else if (typeMapping.containsKey(originalDataType + "-*")) {
//                cacheKey = originalDataType + "-*";
//                typeMappings = typeMapping.get(cacheKey);
//            }
//            if (typeMappings == null || typeMappings.size() == 0) {
//                schema.getInvalidFields().add(field.getFieldName());
//                log.error("Not found tap type mapping rule for databaseType={}, dbVersion={}, dbFieldType={}", databaseType, finalDbVersion, originalDataType);
//                return;
//            }
//
//            field.setDataType(typeMappings.get(0).getTapType());
//            field.setFixed(typeMappings.get(0).getFixed());
        });

        return schema;
    }

    /**
     * 根据字段类型映射规则，将模型 schema中的通用字段类型转换为指定数据库字段类型
     * @param schema 包含通用字段类型的模型
     * @param metadataInstancesDto 将映射后的字段类型保存到这里
     * @param dataSourceConnectionDto 数据库类型
     */
    private MetadataInstancesDto processFieldToDB(Schema schema, MetadataInstancesDto metadataInstancesDto, DataSourceConnectionDto dataSourceConnectionDto) {

        if (metadataInstancesDto == null || schema == null ||
                metadataInstancesDto.getFields() == null || dataSourceConnectionDto == null){
            log.error("Process field type mapping to db type failed, invalid params: schema={}, metadataInstanceDto={}, dataSourceConnectionsDto={}",
                    schema, metadataInstancesDto, dataSourceConnectionDto);
            return metadataInstancesDto;
        }

        final String sourceNodeDatabaseType = schema.getSourceNodeDatabaseType();
        final String databaseType = dataSourceConnectionDto.getDatabase_type();
        String dbVersion = dataSourceConnectionDto.getDb_version();
        if (StringUtils.isBlank(dbVersion)) {
            dbVersion = "*";
        }
        //Map<String, List<TypeMappingsEntity>> typeMapping = typeMappingsService.getTypeMapping(databaseType, TypeMappingDirection.TO_DATATYPE);

        schema.setInvalidFields(new ArrayList<>());
        String finalDbVersion = dbVersion;
        Map<String, Field> fields = schema.getFields().stream().collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));


        DataSourceDefinitionDto definitionDto = definitionDtoMap.get(databaseType);
        String expression = definitionDto.getExpression();
        Map<Class<?>, String> tapMap = definitionDto.getTapMap();

        TapTable tapTable = PdkSchemaConvert.toPdk(schema);


        if (tapTable.getNameFieldMap() != null && tapTable.getNameFieldMap().size() != 0) {
            LinkedHashMap<String, TapField> updateFieldMap = new LinkedHashMap<>();
            tapTable.getNameFieldMap().forEach((k, v) -> {
                if (v.getTapType() == null) {
                    updateFieldMap.put(k, v);
                }
            });

            if (updateFieldMap.size() != 0) {
                PdkSchemaConvert.tableFieldTypesGenerator.autoFill(updateFieldMap, DefaultExpressionMatchingMap.map(expression));

                updateFieldMap.forEach((k, v) -> {
                    tapTable.getNameFieldMap().replace(k, v);
                });
            }
        }

        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create().withTapTypeDataTypeMap(tapMap));
        TapResult<LinkedHashMap<String, TapField>> convert = PdkSchemaConvert.targetTypesGenerator.convert(nameFieldMap
                , DefaultExpressionMatchingMap.map(expression), codecsFilterManager);
        LinkedHashMap<String, TapField> data = convert.getData();

        data.forEach((k, v) -> {
            TapField tapField = nameFieldMap.get(k);
            BeanUtils.copyProperties(v, tapField);
        });
        tapTable.setNameFieldMap(nameFieldMap);



        metadataInstancesDto = PdkSchemaConvert.fromPdk(tapTable);


        metadataInstancesDto.getFields().forEach(field -> {
            if (field.getId() == null) {
                field.setId(new ObjectId().toHexString());
            }
            Field originalField = fields.get(field.getFieldName());
            if (databaseType.equalsIgnoreCase(field.getSourceDbType())) {
                if (originalField != null && originalField.getDataTypeTemp() != null) {
                    field.setDataType(originalField.getDataTypeTemp());
                }
            }
        });

        Map<String, Field> result = metadataInstancesDto.getFields()
                .stream().collect(Collectors.toMap(Field::getFieldName, m -> m, (m1, m2) -> m2));
        if (result.size() != metadataInstancesDto.getFields().size()) {
            metadataInstancesDto.setFields(new ArrayList<>(result.values()));
        }

        return metadataInstancesDto;
    }

    private Long ensureLong(Long value) {
        if (value == null) return 0L;
        return value;
    }

    public DataSourceConnectionDto getDataSource(String connectionId) {
        DataSourceConnectionDto dto = dataSourceMap.get(connectionId);

        DataSourceDefinitionDto definitionDto = definitionDtoMap.get(dto.getDatabase_type());
        dto.setDefinitionPdkId(definitionDto.getPdkId());
        dto.setDefinitionVersion(definitionDto.getVersion());
        dto.setDefinitionGroup(definitionDto.getGroup());
        dto.setDefinitionBuildNumber(String.valueOf(definitionDto.getBuildNumber()));
        return dto;
    }

    @Override
    public void updateTransformRate(String taskId, int total, int finished) {
//        //需要做一下频率控制 下面算法是每十分之一发一次
//        if (((finished -1) * 10 / total) == (finished *10 / total)) {
//            return;
//        }
//
//        Map<String, Object> data = new HashMap<>();
//        data.put("totalTable", total);
//        data.put("finishTable", finished);
//        data.put("transformStatus", total == finished ? "finished" : "loading");
//
//
//        Map<String, Object> map = new HashMap<>();
//        map.put("type", MessageType.EDIT_FLUSH.getType());
//        map.put("opType", "transformRate");
//        map.put("taskId", taskId);
//        map.put("data", data);
//        MessageQueueDto queueDto = new MessageQueueDto();
//        queueDto.setData(map);
//        queueDto.setType("pipe");
//
//        log.info("build transform rate websocket context, queueDto = {}", queueDto);
//        messageQueueService.sendMessage(queueDto);
    }

    @Override
    public boolean checkNewestVersion(String taskId, String nodeId, String version) {
//        Criteria criteria = Criteria.where("dataFlowId").is(taskId).and("stageId").is(nodeId);
//        MetadataTransformerDto transformer = metadataTransformerService.findOne(new Query(criteria));
//
//        // meta transformer version not equals return;
//        if (Objects.nonNull(transformer) && !org.apache.commons.lang3.StringUtils.equals(version, transformer.getVersion())) {
//            log.warn("meta transformer version not equals, transformer = {}, uuid = {}", transformer, version);
//            return false;
//        } else {
//            return true;
//        }
        //todo
        return false;
    }

    /**
     * @param schemaTransformerResults 单批执行的表结果集
     * @param taskId 任务id
     * @param nodeId 节点id
     * @param total 所有的表
     */
    @Override
    public void upsertTransformTemp(List<SchemaTransformerResult> schemaTransformerResults, String taskId, String nodeId, int total, List<String> sourceQualifiedNames, String uuid) {
        log.debug("upsert transform record, size = {}", schemaTransformerResults == null ? 0 : schemaTransformerResults.size());
        if (CollectionUtils.isEmpty(schemaTransformerResults)) {
            return;
        }
        SchemaTransformerResult result = schemaTransformerResults.get(0);


        for (SchemaTransformerResult schemaTransformerResult : schemaTransformerResults) {
            sourceQualifiedNames.add(schemaTransformerResult.getSourceQualifiedName());
            sourceQualifiedNames.add(schemaTransformerResult.getSinkQulifiedName());
        }

        Criteria criteria1 = Criteria.where("qualified_name").in(sourceQualifiedNames);
        Query query = new Query(criteria1);
        query.fields().include("_id", "qualified_name", "originalName", "name", "fields");
        List<MetadataInstancesDto> all = new ArrayList<>();

        for (String sourceQualifiedName : sourceQualifiedNames) {
            all.add(metadataMap.get(sourceQualifiedName));
        }
        Map<String, MetadataInstancesDto> metaMaps = all.stream().collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m, (m1, m2) -> m1));

        //更新两张中间状态表
        for (SchemaTransformerResult schemaTransformerResult : schemaTransformerResults) {
            MetadataTransformerItemDto metadataTransformerItemDto = new MetadataTransformerItemDto();
            BeanUtils.copyProperties(schemaTransformerResult, metadataTransformerItemDto);
            metadataTransformerItemDto.setDataFlowId(taskId);
            metadataTransformerItemDto.setSinkNodeId(nodeId);
            metadataTransformerItemDto.setUuid(uuid);
            MetadataInstancesDto metadataInstancesDto = metaMaps.get(schemaTransformerResult.getSinkQulifiedName());
            if (metadataInstancesDto != null) {
                if (metadataInstancesDto.getId() != null) {
                    metadataTransformerItemDto.setSinkTableId(metadataInstancesDto.getId().toHexString());
                }
                metadataTransformerItemDto.setSinkObjectName(metadataInstancesDto.getOriginalName());
            }

            List<FieldsMapping> fieldsMappings = getFieldsMapping(metaMaps.get(schemaTransformerResult.getSourceQualifiedName()), metaMaps.get(schemaTransformerResult.getSinkQulifiedName()));
            metadataTransformerItemDto.setFieldsMapping(fieldsMappings);

            upsertItems.add(metadataTransformerItemDto);
        }



        MetadataTransformerDto transformer = transformerDtoMap.get(taskId + nodeId);
        String status = MetadataTransformerDto.StatusEnum.running.name();
        if (transformer == null) {
            transformer = MetadataTransformerDto.builder()
                    .dataFlowId(taskId)
                    .stageId(nodeId)
                    .pingTime(System.currentTimeMillis())
                    .finished(schemaTransformerResults.size())
                    .sinkDbType(result.getSinkDbType())
                    .status(status)
                    .total(total)
                    .beginTimestamp(System.currentTimeMillis())
                    //.usedTimestamp()
                    .version(uuid)
                    //.hashCode()
                    .build();

            if (transformer.getFinished() == total) {
                transformer.setStatus(MetadataTransformerDto.StatusEnum.done.name());
            }
            Map<String, Boolean> tableNameMap = schemaTransformerResults.stream().collect(Collectors.toMap(SchemaTransformerResult::getSinkObjectName, s -> true, (v1, v2) -> v1));
            transformer.setTableName(tableNameMap);
        } else {
            transformer.setTotal(total);
            if (uuid.equals(transformer.getVersion())) {
                int f = transformer.getFinished() + schemaTransformerResults.size();
                f = Math.min(f, total);
                transformer.setFinished(f);
            } else {
                transformer.setFinished(schemaTransformerResults.size());
                transformer.setBeginTimestamp(System.currentTimeMillis());
            }
            transformer.setPingTime(System.currentTimeMillis());
            transformer.setVersion(uuid);
            if (transformer.getFinished() == total) {
                status = MetadataTransformerDto.StatusEnum.done.name();
            }
            transformer.setStatus(status);
        }

        if (transformer.getBeginTimestamp() == 0) {
            transformer.setBeginTimestamp(transformer.getPingTime());
        }

        upsertTransformer.add(transformer);
    }

//    /**
//     * @param schemaTransformerResults 单批执行的表结果集
//     * @param taskId 任务id
//     * @param nodeId 节点id
//     * @param tableName 表名
//     */
//    @Override
//    public void upsertTransformTemp(List<SchemaTransformerResult> schemaTransformerResults, String taskId, String nodeId, String tableName) {
//        List<String> tableNames = Lists.newArrayList(tableName);
//        upsertTransformTemp(schemaTransformerResults, taskId, nodeId, tableNames);
//    }


    public List<FieldsMapping> getFieldsMapping(MetadataInstancesDto source, MetadataInstancesDto target) {
        if (source == null || target == null) {
            return null;
        }
        if (source.getFields() == null || target.getFields() == null) {
            return null;
        }

        Map<String, Field> idMap = new HashMap<>();
        Map<String, Field> idOldMap = new HashMap<>();
        for (Field field : source.getFields()) {
            idMap.put(field.getId(), field);

            if (CollectionUtils.isNotEmpty(field.getOldIdList())) {
                for (String id : field.getOldIdList()) {
                    idOldMap.put(id, field);
                }
            }
        }

        List<FieldsMapping> fieldsMappings = new ArrayList<>();

        //页面显示排序问题处理
        MetadataInstancesDto.sortField(target.getFields());

        for (Field field : target.getFields()) {
            FieldsMapping fieldsMapping = new FieldsMapping();
            fieldsMapping.setTargetFieldName(field.getFieldName());
            fieldsMapping.setType(field.getSource());

            Field sourceField = idMap.get(field.getId());

            if (sourceField == null) {
                sourceField = idOldMap.get(field.getId());
            }

            if (sourceField != null) {
                fieldsMapping.setSourceFieldName(sourceField.getFieldName());
                fieldsMapping.setSourceFieldType(sourceField.getDataType());
            }

            fieldsMappings.add(fieldsMapping);
        }
        return fieldsMappings;
    }

    @Override
    public TaskDto getTaskById(String taskId) {
        return taskMap.get(taskId);
    }



    public int bulkSave(List<MetadataInstancesDto> metadataInstancesDtos,
                        DataSourceConnectionDto dataSourceConnectionDto,
                        Map<String, MetadataInstancesDto> existsMetadataInstances) {

        // 这里只保存 original_name, fields;
        //   判定新增模型时，需要设置 create Source 为 推演，执行 MetadataBuilder.build 方法构建新模型
        // 需要比对现有模型，并记录模型历史

        //BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);

        Map<String, Update> metadataUpdateMap = new LinkedHashMap<>();

        List<MetadataInstancesDto> insertMetaDataList = new ArrayList<>();
        metadataInstancesDtos.forEach(metadataInstancesDto -> {

            String qualifiedName = metadataInstancesDto.getQualifiedName();

            // 需要增加版本
            if (existsMetadataInstances.containsKey(qualifiedName)) {
                MetadataInstancesDto existsMetadataInstance = existsMetadataInstances.get(qualifiedName);
                if (existsMetadataInstance == null) {
                    return;
                }

                existsMetadataInstance.setVersion(existsMetadataInstance.getVersion() == null ? 1 : existsMetadataInstance.getVersion());
                int newVersion = existsMetadataInstance.getVersion() + 1;

                MetadataInstancesDto historyModel = new MetadataInstancesDto();
                BeanUtils.copyProperties(existsMetadataInstance, historyModel);
                historyModel.setId(null);
                historyModel.setVersionUserId(userId);
                historyModel.setVersionUserName(userName);
                historyModel.setHistories(null);
                Update update = new Update();
                update.set("version", newVersion);
                ArrayList<MetadataInstancesDto> hisModels = new ArrayList<>();
                hisModels.add(historyModel);
                BasicDBObject basicDBObject = new BasicDBObject("$each", hisModels);
                basicDBObject.append("$slice", -5);
                update.push("histories", basicDBObject);
                update.set("fields", metadataInstancesDto.getFields());
                update.set("indices", metadataInstancesDto.getIndices());
                update.set("is_deleted", false);
                update.set("createSource", metadataInstancesDto.getCreateSource());
                metadataUpdateMap.put(existsMetadataInstance.getId().toHexString(), update);


            } else { // 直接写入

                MetadataInstancesDto _metadataInstancesDto = MetaDataBuilderUtils.build(
                        metadataInstancesDto.getMetaType(), dataSourceConnectionDto, userId, userName,
                        metadataInstancesDto.getOriginalName(),
                        metadataInstancesDto, null, metadataInstancesDto.getDatabaseId(), "job_analyze", null);
                insertMetaDataList.add(_metadataInstancesDto);
            }
        });


        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            setMetaDataMap(metadataInstancesDto);
        }

        batchMetadataUpdateMap.putAll(metadataUpdateMap);
        batchInsertMetaDataList.addAll(insertMetaDataList);
        return metadataUpdateMap.size() + insertMetaDataList.size();
    }


    public Map<String, Update> getBatchMetadataUpdateMap() {
        return batchMetadataUpdateMap;
    }

    public List<MetadataInstancesDto> getBatchInsertMetaDataList() {
        return batchInsertMetaDataList;
    }


    public List<MetadataTransformerItemDto> getUpsertItems() {
        return upsertItems;
    }

    public List<MetadataTransformerDto> getUpsertTransformer() {
        return upsertTransformer;
    }
}
