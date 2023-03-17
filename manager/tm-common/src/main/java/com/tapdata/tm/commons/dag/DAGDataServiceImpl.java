package com.tapdata.tm.commons.dag;

import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.dag.vo.MigrateJsResultVo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


@Slf4j
public class DAGDataServiceImpl implements DAGDataService, Serializable {
    private static Logger logger = LoggerFactory.getLogger(DAGDataServiceImpl.class);

    private final Map<String, MetadataInstancesDto> metadataMap;
    private final Map<String, DataSourceConnectionDto> dataSourceMap;
    private final Map<String, DataSourceDefinitionDto> definitionDtoMap;

    private final Map<String, MetadataTransformerDto> transformerDtoMap;
    private final Map<String, TaskDto> taskMap = new HashMap<>();

    private final String userId;
    private final String userName;

    private  String taskId = null;

    private final Map<String, MetadataInstancesDto> batchMetadataUpdateMap = new LinkedHashMap<>();

    private final List<MetadataInstancesDto> batchInsertMetaDataList = new ArrayList<>();
    private final List<String> batchRemoveMetaDataList = new ArrayList<>();


    private final List<MetadataTransformerItemDto> upsertItems = new ArrayList<>();
    private final List<MetadataTransformerDto> upsertTransformer = new ArrayList<>();



    public DAGDataServiceImpl(TransformerWsMessageDto transformerWsMessageDto) {
        this.metadataMap = new HashMap<>();
        for (MetadataInstancesDto metadataInstancesDto : transformerWsMessageDto.getMetadataInstancesDtoList()) {
            setMetaDataMap(metadataInstancesDto);
        }
        this.dataSourceMap = transformerWsMessageDto.getDataSourceMap();
        this.definitionDtoMap = transformerWsMessageDto.getDefinitionDtoMap();
        if (transformerWsMessageDto.getTaskDto() != null) {
            taskMap.put(transformerWsMessageDto.getTaskDto().getId().toHexString(), transformerWsMessageDto.getTaskDto());
            this.taskId = transformerWsMessageDto.getTaskDto().getId().toHexString();
        }
        this.userId = transformerWsMessageDto.getUserId();
        this.userName = transformerWsMessageDto.getUserName();
        this.transformerDtoMap = transformerWsMessageDto.getTransformerDtoMap();
    }
    public DAGDataServiceImpl(List<MetadataInstancesDto> metadataInstancesDtos, Map<String, DataSourceConnectionDto> dataSourceMap
            , Map<String, DataSourceDefinitionDto> definitionDtoMap, String userId, String userName, TaskDto taskDto
            , Map<String, MetadataTransformerDto> transformerDtoMap) {
        this.metadataMap = new HashMap<>();
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            setMetaDataMap(metadataInstancesDto);
        }
        this.dataSourceMap = dataSourceMap;
        this.definitionDtoMap = definitionDtoMap;
        if (taskDto != null) {
            taskMap.put(taskDto.getId().toHexString(), taskDto);
            this.taskId = taskDto.getId().toHexString();
        }
        this.userId = userId;
        this.userName = userName;
        this.transformerDtoMap = transformerDtoMap;
    }

    public void setMetaDataMap(MetadataInstancesDto metadataInstancesDto) {
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

    private void deleteMetaDataMap(String qualifiedName) {
        MetadataInstancesDto metadataInstancesDto = metadataMap.get(qualifiedName);
        if (metadataInstancesDto.getId() != null) {
            metadataMap.remove(metadataInstancesDto.getId().toHexString());
        }
        if (!"database".equals(metadataInstancesDto.getMetaType()) && metadataInstancesDto.getSource() != null) {
            metadataMap.remove(metadataInstancesDto.getSource().get_id() + metadataInstancesDto.getName());
        }
        metadataMap.remove(qualifiedName);
    }

    private Schema convertToSchema(MetadataInstancesDto metadataInstances) {
        if (metadataInstances == null)
            return null;
        Schema schema = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(metadataInstances), Schema.class);
        //schema.setFields(schema.getFields()/*.stream().filter(field -> field.getParent() == null).collect(Collectors.toList())*/);

        // 这里需要 执行字段映射，将 data_type 转换为 通用字段类型，设置到 data_type 字段（不要回写原模型）
        // 修改后的字段类型保存在schema里面
        processFieldFromDB(metadataInstances, schema);
        return schema;
    }

    @Override
    public Schema loadSchema(String ownerId, ObjectId dataSourceId, String tableName) {

        if (dataSourceId == null || tableName == null) {
            log.error("Can't load schema by params: {}, {}", dataSourceId, tableName);
            return null;
        }
        String key = dataSourceId + tableName;

        MetadataInstancesDto metadataInstances = metadataMap.get(key);

        return convertToSchema(metadataInstances);
    }

    @Override
    public List<Schema> loadSchema(String ownerId, ObjectId dataSourceId, List<String> includes, List<String> excludes) {

        if (dataSourceId == null) {
            log.error("Can't load schema by params: {}, {}, {}, {}", ownerId, dataSourceId, includes, excludes);
            return null;
        }


        DataSourceConnectionDto dataSource = dataSourceMap.get(dataSourceId.toHexString());

        if (dataSource == null) {
            log.error("Load schema failed, not found connection by id {}", dataSourceId.toHexString());
            return null;
        }

        List<MetadataInstancesDto> metadataInstances = new ArrayList<>();
        for (String include : includes) {
            MetadataInstancesDto metadataInstancesDto = metadataMap.get(dataSourceId + include);
            if (metadataInstancesDto != null) {
                metadataInstances.add(metadataInstancesDto);
            }
        }

        long start = System.currentTimeMillis();
        List<Schema> result = metadataInstances.parallelStream().map(this::convertToSchema).collect(Collectors.toList());
        log.debug("Convert metadata instances record to Schema cost {} millisecond", System.currentTimeMillis() - start);
        return result;
    }

    @Override
    public TapTable loadTapTable(String nodeId, String virtualId, TaskDto taskDto) {
        //TODO 引擎用于js跑虚拟数据产生模型  js节点存在源节点schema跟 script， 自定义节点存在 自定义节点id, 跟 form表单
        //具体参数可以另行再定，反正这里得到的会成为最终的节点入库模型
//        String json = "{\"defaultPrimaryKeys\":[\"_id\"],\"id\":\"XXX\",\"lastUpdate\":1656129059016,\"name\":\"XXX\",\"nameFieldMap\":{\"_id\":{\"autoInc\":false,\"dataType\":\"OBJECT_ID\",\"name\":\"_id\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":true,\"primaryKeyPos\":1},\"CUSTOMER_ID\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"CUSTOMER_ID\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"CITY\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"CITY\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"COUNTRY_CODE\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"COUNTRY_CODE\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"DATE_OF_BIRTH\":{\"autoInc\":false,\"dataType\":\"DATE_TIME\",\"name\":\"DATE_OF_BIRTH\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"EMAIL\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"EMAIL\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"FIRST_NAME\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"FIRST_NAME\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"GENDER\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"GENDER\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"JOB\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"JOB\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"LAST_CHANGE\":{\"autoInc\":false,\"dataType\":\"DATE_TIME\",\"name\":\"LAST_CHANGE\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"LAST_NAME\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"LAST_NAME\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"MARITAL_STATUS\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"MARITAL_STATUS\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"NATIONALITY\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"NATIONALITY\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"NUMBER_CHILDREN\":{\"autoInc\":false,\"dataType\":\"INT32\",\"name\":\"NUMBER_CHILDREN\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"PHONE\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"PHONE\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"STREET\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"STREET\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"ZIP\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"ZIP\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"POLICY_ID\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"POLICY_ID\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"CAR_MODEL\":{\"autoInc\":false,\"dataType\":\"STRING\",\"name\":\"CAR_MODEL\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"COVER_START\":{\"autoInc\":false,\"dataType\":\"DATE_TIME\",\"name\":\"COVER_START\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"LAST_ANN_PREMIUM_GROSS\":{\"autoInc\":false,\"dataType\":\"DOUBLE\",\"name\":\"LAST_ANN_PREMIUM_GROSS\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false},\"MAX_COVERED\":{\"autoInc\":false,\"dataType\":\"INT32\",\"name\":\"MAX_COVERED\",\"nullable\":true,\"partitionKey\":false,\"pos\":22,\"primaryKey\":false}}}";
//        TapTable tapTable = InstanceFactory.instance(JsonParser.class).fromJson(json, new TypeHolder<TapTable>() {
//        }, TapConstants.abstractClassDetectors);
//        return tapTable;
        return null;
    }

    @Override
    public List<MigrateJsResultVo> getJsResult(String jsNodeId, String virtualTargetId, TaskDto taskDto) {
        return null;
    }

    @Override
    public List<Schema> createOrUpdateSchema(String ownerId, ObjectId dataSourceId, List<Schema> schemas, DAG.Options options, Node node) {

        log.info("Create or update schema: ownerId={}, dataSourceId={}, schemaCount={}, options={}", ownerId,
                dataSourceId, schemas != null ? schemas.size() : null,options);

        if (schemas == null) {
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
            schema.setOldId(schema.getId());
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
                    if (field.getId() != null) {
                        field.getOldIdList().add(field.getId());
                    }
                    field.setId(new ObjectId().toHexString());
                }
                set.add(field.getId());
            }
        }

        boolean appendNodeTableName = node instanceof TableRenameProcessNode || node instanceof MigrateFieldRenameProcessorNode || node instanceof MigrateJsProcessorNode;

        if (node.isDataNode()) {
            return createOrUpdateSchemaForDataNode(dataSourceId, schemas, options, node);
        } else {
            return createOrUpdateSchemaForProcessNode(schemas, options, node.getId(), appendNodeTableName);
        }
    }

    /**
     * 保存处理器节点模型
     * @param schemas 模型
     * @param options 配置项
     * @return
     */
    private List<Schema> createOrUpdateSchemaForProcessNode(List<Schema> schemas, DAG.Options options, String nodeId, boolean appendNodeTableName) {

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
        List<MetadataInstancesDto> metadataInstancesDtos = schemas.parallelStream().map(schema -> {
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

            String nodeTableName = appendNodeTableName ? schema.getOriginalName() : null;
            String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, nodeTableName, taskId);
            metadataInstancesDto.setQualifiedName(qualifiedName);

            MetaDataBuilderUtils.build(_metaType, dataSource, userId, userName, metadataInstancesDto.getOriginalName(),
                    metadataInstancesDto, null, dataSourceMetadataInstance.getId().toHexString(), taskId);

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

        return metadataInstancesDtos.parallelStream()
                .map(dto -> JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dto), Schema.class)).collect(Collectors.toList());
    }

    /**
     * 保存数据节点模型
     * @param dataSourceId 数据源id
     * @param schemas 模型
     * @param options 配置项
     * @return
     */
    private List<Schema> createOrUpdateSchemaForDataNode(ObjectId dataSourceId, List<Schema> schemas, DAG.Options options, Node node) {
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
                dataSource.setDefinitionTags(definitionDto.getTags());
            }
        }

        String databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", dataSource, null);
        MetadataInstancesDto dataSourceMetadataInstance = metadataMap.get(databaseQualifiedName);

        if (dataSourceMetadataInstance == null) {
            log.error("Save schema failed, can't not found metadata for data source {}({})", databaseQualifiedName, dataSource.getId());
            return Collections.emptyList();
        }

        log.info("save schema, found database schema, q_name = {}", databaseQualifiedName);

        SourceDto sourceDto = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dataSource), SourceDto.class);

        // 其他类型的 meta type 暂时不做模型推演处理
        String metaType = "table";
        if ("mongodb".equals(dataSource.getDatabase_type())) {
            metaType = "collection";
        }

        Map<String, List<String>> updateConditionFieldMap;
        if (node instanceof DatabaseNode) {
            updateConditionFieldMap = ((DatabaseNode) node).getUpdateConditionFieldMap();
        } else {
            updateConditionFieldMap = null;
        }

        final String _metaType = metaType;
        List<MetadataInstancesDto> metadataInstancesDtos = schemas.parallelStream().map(schema -> {
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
                    MetaDataBuilderUtils.generateQualifiedName(metadataInstancesDto.getMetaType(), dataSource, schema.getOriginalName(), taskId));

            metadataInstancesDto = MetaDataBuilderUtils.build(_metaType, dataSource, userId, userName, metadataInstancesDto.getOriginalName(),
                    metadataInstancesDto, null, dataSourceMetadataInstance.getId().toHexString(), taskId);

            metadataInstancesDto.setSourceType(SourceTypeEnum.VIRTUAL.name());

            if (node instanceof DatabaseNode && Objects.nonNull(updateConditionFieldMap) && !updateConditionFieldMap.isEmpty()) {
                if (updateConditionFieldMap.containsKey(schema.getOriginalName())) {
                    metadataInstancesDto.setHasUpdateField(true);
                }
            }

            /*MetadataInstancesDto result = metadataInstancesService.upsertByWhere(
                    Where.where("qualified_name", metadataInstancesDto.getQualifiedName()), metadataInstancesDto, userDetail);*/

            options.processRule(metadataInstancesDto);
            return metadataInstancesDto;
        }).collect(Collectors.toList());

        String rollback = options != null ? options.getRollback() : null;
        String rollbackTable = options != null ? options.getRollbackTable() : null;
        long start = System.currentTimeMillis();

        Map<String, MetadataInstancesDto> existsMetadataInstances = rollbackOperation(metadataInstancesDtos, rollback, rollbackTable);
        log.info("bulk save meta data");
        int modifyCount = bulkSave(metadataInstancesDtos, dataSource, existsMetadataInstances);
        log.info("Bulk save metadataInstance {}, cost {}ms", modifyCount, System.currentTimeMillis() - start);

        return metadataInstancesDtos.parallelStream()
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

        if (tapTable.getNameFieldMap() != null && tapTable.getNameFieldMap().size() != 0) {
            PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(tapTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));
        }

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
                PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(updateFieldMap, DefaultExpressionMatchingMap.map(expression));

                updateFieldMap.forEach((k, v) -> {
                    tapTable.getNameFieldMap().replace(k, v);
                });
            }
        }

        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (nameFieldMap != null && nameFieldMap.size() != 0) {

            TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create().withTapTypeDataTypeMap(tapMap));
            TapResult<LinkedHashMap<String, TapField>> convert = PdkSchemaConvert.getTargetTypesGenerator().convert(nameFieldMap
                    , DefaultExpressionMatchingMap.map(expression), codecsFilterManager);
            LinkedHashMap<String, TapField> data = convert.getData();
            metadataInstancesDto.setResultItems(convert.getResultItems());

            data.forEach((k, v) -> {
                TapField tapField = nameFieldMap.get(k);
                tapField.setDataType(v.getDataType());
                tapField.setTapType(v.getTapType());
            });
        }

        tapTable.setNameFieldMap(nameFieldMap);

        ObjectId oldId = metadataInstancesDto.getOldId();
        metadataInstancesDto = PdkSchemaConvert.fromPdk(tapTable);
        metadataInstancesDto.setOldId(oldId);
        metadataInstancesDto.setAncestorsName(schema.getAncestorsName());
        metadataInstancesDto.setNodeId(schema.getNodeId());

        AtomicBoolean hasPrimayKey = new AtomicBoolean(false);
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

            if (Objects.nonNull(field.getPrimaryKey()) && field.getPrimaryKey()) {
                hasPrimayKey.set(true);
            }
        });
        metadataInstancesDto.setHasPrimaryKey(hasPrimayKey.get());

        Map<String, Field> result = metadataInstancesDto.getFields()
                .stream().collect(Collectors.toMap(Field::getFieldName, m -> m, (m1, m2) -> m2));
        if (result.size() != metadataInstancesDto.getFields().size()) {
            metadataInstancesDto.setFields(new ArrayList<>(result.values()));
        }

        AtomicBoolean hasUnionIndex = new AtomicBoolean(false);
        Optional.ofNullable(metadataInstancesDto.getIndices()).ifPresent(indexList -> {
            hasUnionIndex.set(indexList.stream().anyMatch(TableIndex::isUnique));
        });
        metadataInstancesDto.setHasUnionIndex(hasUnionIndex.get());

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
        dto.setDefinitionTags(definitionDto.getTags());
        return dto;
    }

    @Override
    public void updateTransformRate(String taskId, int total, int finished) {
    }

    @Override
    public boolean checkNewestVersion(String taskId, String nodeId, String version) {
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
        if (transformerDtoMap == null) {
            //在这个map为空的时候，说明推演的时候就不需要处理这段逻辑
            return;
        }

        if (taskId == null) {
            taskId = getTaskId().toHexString();
        }
        log.debug("upsert transform record, size = {}", schemaTransformerResults == null ? 0 : schemaTransformerResults.size());
        if (CollectionUtils.isEmpty(schemaTransformerResults)) {
            return;
        }
        SchemaTransformerResult result = schemaTransformerResults.get(0);


        for (SchemaTransformerResult schemaTransformerResult : schemaTransformerResults) {
            sourceQualifiedNames.add(schemaTransformerResult.getSourceQualifiedName());
            sourceQualifiedNames.add(schemaTransformerResult.getSinkQulifiedName());
        }

        List<MetadataInstancesDto> all = new ArrayList<>();

        for (String sourceQualifiedName : sourceQualifiedNames) {
            MetadataInstancesDto metadataInstancesDto = metadataMap.get(sourceQualifiedName);
            if (metadataInstancesDto != null) {
                all.add(metadataInstancesDto);
            }
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
            // TODO: find out if it affects other logic if we skip the set
            Map<String, Boolean> tableNameMap = new HashMap<>();
            for (SchemaTransformerResult schemaTransformerResult : schemaTransformerResults) {
                if (null == schemaTransformerResult.getSinkObjectName()) {
                    logger.error("BUG: the sink object name is null, should find out why");
                    continue;
                }
                tableNameMap.putIfAbsent(schemaTransformerResult.getSinkObjectName(), true);
            }
//            Map<String, Boolean> tableNameMap = schemaTransformerResults.stream().collect(Collectors.toMap(SchemaTransformerResult::getSinkObjectName, s -> true, (v1, v2) -> v1));
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
        TaskDto taskDto = taskMap.get(taskId);

        if (taskDto != null) {
            return taskDto;
        }

        if (taskMap.size() == 1) {
            ArrayList<TaskDto> taskDtos = new ArrayList<>(taskMap.values());
            return taskDtos.get(0);
        }
        return null;
    }

    public ObjectId getTaskId() {

        if (taskMap.size() == 1) {
            ArrayList<TaskDto> taskDtos = new ArrayList<>(taskMap.values());
            return taskDtos.get(0).getId();
        }
        return null;
    }



    public int bulkSave(List<MetadataInstancesDto> metadataInstancesDtos,
                        DataSourceConnectionDto dataSourceConnectionDto,
                        Map<String, MetadataInstancesDto> existsMetadataInstances) {

        // 这里只保存 original_name, fields;
        //   判定新增模型时，需要设置 create Source 为 推演，执行 MetadataBuilder.build 方法构建新模型
        // 需要比对现有模型，并记录模型历史

        //BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
        List<String> removeKey = new ArrayList<>();
        existsMetadataInstances.forEach((k, v) -> {
            if (v.getSourceType().equals(SourceTypeEnum.SOURCE.name())) {
                removeKey.add(k);
            }
        });

        for (String key : removeKey) {
            existsMetadataInstances.remove(key);
        }

        Map<String, MetadataInstancesDto> metadataUpdateMap = new LinkedHashMap<>();

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

                MetadataInstancesDto update2 = new MetadataInstancesDto();

                ArrayList<MetadataInstancesDto> hisModels = new ArrayList<>();
                hisModels.add(historyModel);


                update2.setHistories(hisModels);
                update2.setFields(metadataInstancesDto.getFields());
                update2.setIndexes(metadataInstancesDto.getIndexes());
                update2.setIndices(metadataInstancesDto.getIndices());
                update2.setDeleted(false);
                update2.setCreateSource(metadataInstancesDto.getCreateSource());
                update2.setVersion(newVersion);
                update2.setSourceType(metadataInstancesDto.getSourceType());
                update2.setQualifiedName(metadataInstancesDto.getQualifiedName());
                update2.setHasPrimaryKey(metadataInstancesDto.isHasPrimaryKey());
                update2.setHasUnionIndex(metadataInstancesDto.isHasUnionIndex());
                update2.setResultItems(metadataInstancesDto.getResultItems());
                update2.setHasUpdateField(metadataInstancesDto.isHasUpdateField());
                if (existsMetadataInstance != null && existsMetadataInstance.getId() != null) {
                    metadataInstancesDto.setId(existsMetadataInstance.getId());
                    metadataUpdateMap.put(existsMetadataInstance.getId().toHexString(), update2);
                }


            } else { // 直接写入

                MetadataInstancesDto _metadataInstancesDto = MetaDataBuilderUtils.build(
                        metadataInstancesDto.getMetaType(), dataSourceConnectionDto, userId, userName,
                        metadataInstancesDto.getOriginalName(),
                        metadataInstancesDto, null, metadataInstancesDto.getDatabaseId(), "job_analyze",
                        null, taskId);
                insertMetaDataList.add(_metadataInstancesDto);
                BeanUtils.copyProperties(_metadataInstancesDto, metadataInstancesDto);
            }
        });


        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            setMetaDataMap(metadataInstancesDto);
        }

        //log.info("save schema update map = {}", metadataUpdateMap);
        //log.info("save schema insert metadata = {}", insertMetaDataList);

        batchMetadataUpdateMap.putAll(metadataUpdateMap);
        batchInsertMetaDataList.addAll(insertMetaDataList);
        return metadataUpdateMap.size() + insertMetaDataList.size();
    }


    public Map<String, MetadataInstancesDto> getBatchMetadataUpdateMap() {
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

    public MetadataInstancesDto getMetadata(String qualifiedName) {
        return metadataMap.get(qualifiedName);
    }

    public TapTable getTapTable(String qualifiedName) {
        MetadataInstancesDto metadataInstancesDto = metadataMap.get(qualifiedName);
        return PdkSchemaConvert.toPdk(metadataInstancesDto);
    }


    public void coverMetaDataByTapTable(String qualifiedName, TapTable tapTable) {
        MetadataInstancesDto metadataInstancesDto = metadataMap.get(qualifiedName);
        if (metadataInstancesDto == null) {
            return;
        }
        DataSourceConnectionDto dataSourceConnectionDto = dataSourceMap.get(metadataInstancesDto.getSource().get_id());
        if (dataSourceConnectionDto == null) {
            return;
        }

        DataSourceDefinitionDto definitionDto = definitionDtoMap.get(dataSourceConnectionDto.getDatabase_type());
        if (definitionDto != null) {
            String expression = definitionDto.getExpression();
            Map<Class<?>, String> tapMap = definitionDto.getTapMap();
            PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(tapTable.getNameFieldMap() == null ? new LinkedHashMap<>() : tapTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create().withTapTypeDataTypeMap(tapMap));
            TapResult<LinkedHashMap<String, TapField>> convert = PdkSchemaConvert.getTargetTypesGenerator().convert(nameFieldMap
                    , DefaultExpressionMatchingMap.map(expression), codecsFilterManager);
            LinkedHashMap<String, TapField> data = convert.getData();

            data.forEach((k, v) -> {
                TapField tapField = nameFieldMap.get(k);
                BeanUtils.copyProperties(v, tapField);
            });
            tapTable.setNameFieldMap(nameFieldMap);
        }
        MetadataInstancesDto metadataInstancesDto1 = PdkSchemaConvert.fromPdk(tapTable);

        Map<String, Field> oldFieldMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(metadataInstancesDto.getFields())) {
            oldFieldMap = metadataInstancesDto.getFields().stream().collect(Collectors.toMap(Field::getFieldName, f -> f));
        }
        if (CollectionUtils.isNotEmpty(metadataInstancesDto1.getFields())) {

            for (Field field : metadataInstancesDto1.getFields()) {
                Field oldField = oldFieldMap.get(field.getFieldName());
                if (oldField != null) {
                    oldField.setDefaultValue(field.getDefaultValue());
                    oldField.setIsNullable(field.getIsNullable());
                    oldField.setFieldName(field.getFieldName());
                    oldField.setOriginalFieldName(field.getOriginalFieldName());
                    oldField.setColumnPosition(field.getColumnPosition());
                    oldField.setPrimaryKeyPosition(field.getPrimaryKeyPosition());
                    oldField.setForeignKeyTable(field.getForeignKeyTable());
                    oldField.setForeignKeyColumn(field.getForeignKeyColumn());
                    oldField.setAutoincrement(field.getAutoincrement());
                    oldField.setComment(field.getComment());
                    oldField.setPkConstraintName(field.getPkConstraintName());
                    oldField.setPrimaryKey(field.getPrimaryKey());
                    oldField.setTapType(field.getTapType());
                    oldField.setDataType(field.getDataType());
                    BeanUtils.copyProperties(oldField, field);
                } else {
                    field.setSourceDbType(dataSourceConnectionDto.getDatabase_type());
                    field.setId(ObjectId.get().toHexString());
                }
            }
        }
        metadataInstancesDto.setFields(metadataInstancesDto1.getFields());
        metadataInstancesDto.setIndexes(metadataInstancesDto1.getIndexes());
        metadataInstancesDto.setIndices(metadataInstancesDto1.getIndices());

        setMetaDataMap(metadataInstancesDto);
    }

    public String createNewTable(String connectionId, TapTable tapTable, String taskId) {
        DataSourceConnectionDto connectionDto = dataSourceMap.get(connectionId);
        if (connectionDto == null) {
            return null;
        }

        DataSourceDefinitionDto definitionDto = definitionDtoMap.get(connectionDto.getDatabase_type());
        if (definitionDto != null) {
            String expression = definitionDto.getExpression();
            Map<Class<?>, String> tapMap = definitionDto.getTapMap();
            PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(tapTable.getNameFieldMap() == null ? new LinkedHashMap<>() : tapTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create().withTapTypeDataTypeMap(tapMap));
            TapResult<LinkedHashMap<String, TapField>> convert = PdkSchemaConvert.getTargetTypesGenerator().convert(nameFieldMap
                    , DefaultExpressionMatchingMap.map(expression), codecsFilterManager);
            LinkedHashMap<String, TapField> data = convert.getData();

            data.forEach((k, v) -> {
                TapField tapField = nameFieldMap.get(k);
                BeanUtils.copyProperties(v, tapField);
            });
            tapTable.setNameFieldMap(nameFieldMap);
        }
        MetadataInstancesDto metadataInstancesDto1 = PdkSchemaConvert.fromPdk(tapTable);
        String databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", connectionDto, null);
        MetadataInstancesDto databaseMeta = metadataMap.get(databaseQualifiedName);
        metadataInstancesDto1 = MetaDataBuilderUtils.build(MetaType.table.name(), connectionDto, userId, userName, metadataInstancesDto1.getOriginalName(), metadataInstancesDto1, null, databaseMeta.getId().toString(), taskId);
        metadataInstancesDto1.setCreateSource("job_analyze");
        metadataInstancesDto1.setSourceType(SourceTypeEnum.VIRTUAL.name());
        if (CollectionUtils.isNotEmpty(metadataInstancesDto1.getFields())) {

            for (Field field : metadataInstancesDto1.getFields()) {
                    field.setSourceDbType(connectionDto.getDatabase_type());
                    field.setId(ObjectId.get().toHexString());
                }

        }
        setMetaDataMap(metadataInstancesDto1);
        return metadataInstancesDto1.getQualifiedName();
    }

    public void dropTable(String qualifiedName) {
        batchRemoveMetaDataList.add(qualifiedName);
        deleteMetaDataMap(qualifiedName);
    }



    public String getNameByNodeAndTableName(String nodeId, String tableName) {
        TaskDto taskDto = getTaskById(taskId);
        if (taskDto == null) {
            return null;
        }
        DAG dag = taskDto.getDag();
        if (dag == null) {
            return null;
        }

        Node<?> node = dag.getNode(nodeId);
        if (node instanceof DatabaseNode || node instanceof MigrateProcessorNode) {
            List<Node> sources = node.getDag().getSources();
            Node source = sources.get(0);
            LinkedList<TableRenameProcessNode> linkedList = new LinkedList<>();

            while (!source.getId().equals(node.getId())) {
                if (source instanceof TableRenameProcessNode) {
                    linkedList.add((TableRenameProcessNode) source);
                }
                List successors = source.successors();
                if (CollectionUtils.isEmpty(successors)) {
                    break;
                }
                source = (Node) successors.get(0);
            }
            if (source instanceof TableRenameProcessNode) {
                linkedList.add((TableRenameProcessNode) source);
            }
            TableRenameTableInfo tableInfo = null;
            if (CollectionUtils.isNotEmpty(linkedList)) {
                for (TableRenameProcessNode node1 : linkedList) {
                    Map<String, TableRenameTableInfo> tableRenameTableInfoMap = node1.originalMap();
                    TableRenameTableInfo tableInfo1 = tableRenameTableInfoMap.get(tableName);
                    if (tableInfo1 != null) {
                        tableInfo = tableInfo1;
                    }
                }
            }

            if (tableInfo != null) {
                tableName = tableInfo.getCurrentTableName();
            }
        } else if (node instanceof TableNode) {
            tableName = ((TableNode) node).getTableName();
        }
        return tableName;
    }


    public MetadataInstancesDto getSchemaByNodeAndTableName(String nodeId, String tableName) {
        TaskDto taskDto = getTaskById(taskId);
        if (taskDto == null) {
            return null;
        }
        DAG dag = taskDto.getDag();
        if (dag == null) {
            return null;
        }

        Node<?> node = dag.getNode(nodeId);
        String qualifiedName = null;
        if (node instanceof DatabaseNode) {
//            List<Node> sources = node.getDag().getSources();
//            Node source = sources.get(0);
//            LinkedList<TableRenameProcessNode> linkedList = new LinkedList<>();
//            while (!source.getId().equals(node.getId())) {
//                if (source instanceof TableRenameProcessNode) {
//                    linkedList.add((TableRenameProcessNode) source);
//                }
//                List successors = source.successors();
//                if (CollectionUtils.isEmpty(successors)) {
//                    break;
//                }
//                source = (Node) successors.get(0);
//            }
//            TableRenameTableInfo tableInfo = null;
//            if (CollectionUtils.isNotEmpty(linkedList)) {
//                for (TableRenameProcessNode node1 : linkedList) {
//                    Map<String, TableRenameTableInfo> tableRenameTableInfoMap = node1.originalMap();
//                    TableRenameTableInfo tableInfo1 = tableRenameTableInfoMap.get(tableName);
//                    if (tableInfo1 != null) {
//                        tableInfo = tableInfo1;
//                    }
//                }
//            }
//
//            if (tableInfo != null) {
//                tableName = tableInfo.getCurrentTableName();
//            }
            String connectionId = ((DatabaseNode) node).getConnectionId();
            DataSourceConnectionDto connectionDto = dataSourceMap.get(connectionId);
            qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.table.name(), connectionDto, tableName, taskId);
        } else if (node instanceof ProcessorNode || node instanceof MigrateProcessorNode) {
//            if (node instanceof TableRenameProcessNode) {
//                LinkedHashSet<TableRenameTableInfo> tableNames = ((TableRenameProcessNode) node).getTableNames();
//                if (CollectionUtils.isNotEmpty(tableNames)) {
//                    for (TableRenameTableInfo name : tableNames) {
//                        if (name.getOriginTableName().equals(tableName)) {
//                            tableName = name.getCurrentTableName();
//                            break;
//                        }
//                    }
//                }
//            }
            qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, tableName);
        }

        if (StringUtils.isBlank(qualifiedName)) {
            return null;
        }

        for (MetadataInstancesDto metadataInstancesDto : batchInsertMetaDataList) {
            if (metadataInstancesDto.getQualifiedName().equals(qualifiedName)) {
                return metadataInstancesDto;
            }
        }
        return null;
    }


    public DataSourceDefinitionDto getDefinitionByType(String databaseType) {
        return definitionDtoMap.get(databaseType);
    }
}
