package com.tapdata.tm.dag.service;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.vo.CustomTypeMapping;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.task.constant.DagOutputTemplateEnum;
import com.tapdata.tm.task.service.TaskDagCheckLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.utils.CacheUtils;
import com.tapdata.tm.transform.entity.MetadataTransformerItemEntity;
import com.tapdata.tm.transform.service.MetadataTransformerItemService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.typemappings.constant.TypeMappingDirection;
import com.tapdata.tm.typemappings.entity.TypeMappingsEntity;
import com.tapdata.tm.typemappings.service.DataTypeSupportService;
import com.tapdata.tm.typemappings.service.TypeMappingsService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.ws.enums.MessageType;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/10 下午2:15
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class DAGService implements DAGDataService {
    private final Logger logger = LogManager.getLogger(DAGService.class);

    @Autowired
    private MetadataInstancesService metadataInstancesService;

    @Autowired
    private DataSourceService dataSourceService;

    @Autowired
    private UserService userService;

    @Autowired
    private TypeMappingsService typeMappingsService;

    @Autowired
    private MessageQueueService messageQueueService;

    @Autowired
    private MetadataInstancesRepository repository;
    private MongoTemplate mongoTemplate;
    private DataSourceDefinitionService dataSourceDefinitionService;
    private MetadataTransformerService metadataTransformerService;
    private MetadataTransformerItemService metadataTransformerItemService;
    private TaskService taskService;
    private TaskDagCheckLogService taskDagCheckLogService;

    @Autowired
    private DataTypeSupportService dataTypeSupportService;

    private Schema convertToSchema(MetadataInstancesDto metadataInstances, UserDetail user) {
        if (metadataInstances == null)
            return null;
        Schema schema = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(metadataInstances), Schema.class);
        schema.setFields(schema.getFields()/*.stream().filter(field -> field.getParent() == null).collect(Collectors.toList())*/);

        // 这里需要 执行字段映射，将 data_type 转换为 通用字段类型，设置到 data_type 字段（不要回写原模型）
        // 修改后的字段类型保存在schema里面
        schema = processFieldFromDB(metadataInstances, schema, user);
        return schema;
    }

    @Override
    public Schema loadSchema(String ownerId, ObjectId dataSourceId, String tableName) {

        if (ownerId == null || dataSourceId == null || tableName == null) {
            log.error("Can't load schema by params: {}, {}, {}", ownerId, dataSourceId, tableName);
            return null;
        }

        UserDetail userDetail = userService.loadUserById(toObjectId(ownerId));

        if (userDetail == null) {
            log.error("Load schema failed, not found user by id {}", ownerId);
            return null;
        }

        Criteria criteria = Criteria
                .where("meta_type").in("table", "collection", "view")
                .and("original_name").is(tableName)
                .and("is_deleted").ne(true)
                .and("source._id").is(dataSourceId.toHexString());

        MetadataInstancesDto metadataInstances = metadataInstancesService.findOne(
                Query.query(criteria).with(Sort.by(Sort.Order.desc("dev_version"))), userDetail);

        return convertToSchema(metadataInstances, userDetail);
    }

    @Override
    public List<Schema> loadSchema(String ownerId, ObjectId dataSourceId, List<String> includes, List<String> excludes) {

        if (ownerId == null || dataSourceId == null) {
            log.error("Can't load schema by params: {}, {}, {}, {}", ownerId, dataSourceId, includes, excludes);
            return null;
        }

        UserDetail userDetail = userService.loadUserById(toObjectId(ownerId));

        if (userDetail == null) {
            log.error("Load schema failed, not found user by id {}", ownerId);
            return null;
        }

        DataSourceConnectionDto dataSource = dataSourceService.findById(dataSourceId, userDetail);

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

        List<MetadataInstancesDto> metadataInstances = metadataInstancesService.findAllDto(
                Query.query(criteria).with(Sort.by(Sort.Order.desc("dev_version"))), userDetail);

        if (metadataInstances == null) {
            return Collections.emptyList();
        }
        long start = System.currentTimeMillis();
        List<Schema> result = metadataInstances.stream().map(m->convertToSchema(m, userDetail)).collect(Collectors.toList());
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

        UserDetail userDetail = userService.loadUserById(toObjectId(ownerId));

        if (userDetail == null) {
            log.error("Save schema failed, not found user by id {}", ownerId);
            return Collections.emptyList();
        }

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

        boolean appendNodeTableName = false;
        if (node instanceof TableRenameProcessNode || node instanceof MigrateFieldRenameProcessorNode) {
            appendNodeTableName = true;
        }

        if (node.isDataNode()) {
            return createOrUpdateSchemaForDataNode(userDetail, dataSourceId, schemas, options);
        } else {
            return createOrUpdateSchemaForProcessNode(userDetail, schemas, options, node.getId(), appendNodeTableName);
        }
    }

    /**
     * 保存处理器节点模型
     * @param userDetail 当前用户
     * @param schemas 模型
     * @param options 配置项
     * @return
     */
    private List<Schema> createOrUpdateSchemaForProcessNode(UserDetail userDetail, List<Schema> schemas,
                                                            DAG.Options options, String nodeId,
                                                            boolean appendNodeTableName) {

        List<ObjectId> databaseMetadataInstancesIds = schemas.stream().map(Schema::getDatabaseId).distinct()
                .filter(Objects::nonNull).map(ObjectId::new).collect(Collectors.toList());

        List<MetadataInstancesDto> databaseMetadataInstances = metadataInstancesService.findAllDto(
                Query.query(Criteria.where("_id").in(databaseMetadataInstancesIds)), userDetail);

        MetadataInstancesDto dataSourceMetadataInstance =
                databaseMetadataInstances.size() > 0 ? databaseMetadataInstances.get(0) : null;

        if (dataSourceMetadataInstance == null) {
            log.error("Save schema failed, can't not found data source by id {}", databaseMetadataInstancesIds);
            return Collections.emptyList();
        }

        SourceDto sourceDto = dataSourceMetadataInstance.getSource();
        DataSourceConnectionDto dataSource = dataSourceService.findById(MongoUtils.toObjectId(sourceDto.get_id()));

        // 其他类型的 meta type 暂时不做模型推演处理
        final String _metaType = MetaType.processor_node.name();
        List<MetadataInstancesDto> metadataInstancesDtos = schemas.stream().map(schema -> {
            MetadataInstancesDto metadataInstancesDto =
                    JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(schema), MetadataInstancesDto.class);

            // 这里需要将 data_type 字段根据字段类型映射规则转换为 数据库类型
            //   需要 根据 所有可匹配条件，尽量缩小匹配结果，选择最优字段类型
            metadataInstancesDto = processFieldToDB(schema, metadataInstancesDto, dataSource, userDetail, options);
            metadataInstancesDto.setAncestorsName(schema.getAncestorsName());

            metadataInstancesDto.setMetaType(_metaType);
            metadataInstancesDto.setDeleted(false);
            metadataInstancesDto.setSource(sourceDto);

            // 需要将 Connections 的id 转换为 metadata instance 中 meta_type=database 的 id
            //   使用 qualified name 作为条件查询
            metadataInstancesDto.setDatabaseId(dataSourceMetadataInstance.getId().toHexString());

            metadataInstancesDto.setCreateSource("job_analyze");
            metadataInstancesDto.setLastUserName(userDetail.getUsername());
            metadataInstancesDto.setLastUpdBy(userDetail.getUserId());

            String nodeTableName = appendNodeTableName ? schema.getOriginalName() : null;
            String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, nodeTableName);
            metadataInstancesDto.setQualifiedName(qualifiedName);

            MetaDataBuilderUtils.build(_metaType, dataSource, userDetail.getUserId(), userDetail.getUsername(), metadataInstancesDto.getOriginalName(),
                    metadataInstancesDto, null, dataSourceMetadataInstance.getId().toHexString());

            metadataInstancesDto.setSourceType(SourceTypeEnum.VIRTUAL.name());

            /*MetadataInstancesDto result = metadataInstancesService.upsertByWhere(
                    Where.where("qualified_name", metadataInstancesDto.getQualifiedName()), metadataInstancesDto, userDetail);*/
            return metadataInstancesDto;
        }).collect(Collectors.toList());

        String rollback = options != null ? options.getRollback() : null;
        String rollbackTable = options != null ? options.getRollbackTable() : null;
        long start = System.currentTimeMillis();


        List<String> qualifiedNames = metadataInstancesDtos.stream().map(MetadataInstancesDto::getQualifiedName)
                .filter(Objects::nonNull).collect(Collectors.toList());
        Query query = Query.query(Criteria.where("qualified_name").in(qualifiedNames));
        Map<String, MetadataInstancesEntity> existsMetadataInstances =
                repository.findAll(query, userDetail).stream().collect(Collectors.toMap(MetadataInstancesEntity::getQualifiedName, s -> s, (s1, s2) -> s1));
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            MetadataInstancesEntity existsMetadataInstance = existsMetadataInstances.get(metadataInstancesDto.getQualifiedName());

            Map<String, Field> existsFieldMap = Maps.newConcurrentMap();

            if (!Objects.isNull(existsMetadataInstance)) {
                existsFieldMap = existsMetadataInstance.getFields().stream()
                        .collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));
            }

            if ("all".equalsIgnoreCase(rollback) ||
                    ("table".equalsIgnoreCase(rollback) &&
                            metadataInstancesDto.getOriginalName().equalsIgnoreCase(rollbackTable))) {

                Map<String, Field> finalExistsFieldMap = existsFieldMap;
                metadataInstancesDto.getFields().forEach(field -> {
                    if (finalExistsFieldMap.containsKey(field.getOriginalFieldName())) {
                        Field existsField = finalExistsFieldMap.get(field.getOriginalFieldName());
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

                    if (finalExistsFieldMap.containsKey(field.getOriginalFieldName())) {
                        Field existsField = finalExistsFieldMap.get(field.getOriginalFieldName());
                        field.setId(existsField.getId());
                    }
                    if (StringUtils.isBlank(field.getId())) {
                        field.setId(new ObjectId().toHexString());
                    }
                });
            }

            metadataInstancesDto.setDatabaseId(dataSourceMetadataInstance.getId().toHexString());
        }

        int modifyCount = metadataInstancesService.bulkSave(metadataInstancesDtos, dataSourceMetadataInstance, dataSource,
                options, userDetail, existsMetadataInstances);
        log.info("Bulk save metadataInstance {}, cost {}ms", modifyCount, System.currentTimeMillis() - start);

        return metadataInstancesDtos.stream()
                .map(dto -> JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dto), Schema.class)).collect(Collectors.toList());
    }

    /**
     * 保存数据节点模型
     * @param userDetail 当前用户
     * @param dataSourceId 数据源id
     * @param schemas 模型
     * @param options 配置项
     * @return
     */
    private List<Schema> createOrUpdateSchemaForDataNode(UserDetail userDetail, ObjectId dataSourceId, List<Schema> schemas, DAG.Options options) {
        DataSourceConnectionDto dataSource = dataSourceService.findById(dataSourceId);

        if (dataSource == null) {
            log.error("Save schema failed, can't not found data source by id {}", dataSourceId);
            return Collections.emptyList();
        }

        if (DataSourceDefinitionDto.PDK_TYPE.equals(dataSource.getPdkType())) {
            DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(dataSource.getDatabase_type(), userDetail);
            if (definitionDto != null) {
                dataSource.setDefinitionScope(definitionDto.getScope());
                dataSource.setDefinitionGroup(definitionDto.getGroup());
                dataSource.setDefinitionVersion(definitionDto.getVersion());
                dataSource.setDefinitionPdkId(definitionDto.getPdkId());
                dataSource.setDefinitionBuildNumber(String.valueOf(definitionDto.getBuildNumber()));
            }
        }

        String databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", dataSource, null);
        MetadataInstancesDto dataSourceMetadataInstance = metadataInstancesService.findOne(
                Query.query(Criteria.where("qualified_name").is(databaseQualifiedName).and("is_deleted").ne(true)), userDetail);

        if (dataSourceMetadataInstance == null) {
            log.error("Save schema failed, can't not found metadata for data source {}({})", databaseQualifiedName, dataSource.getId());
            return Collections.emptyList();
        }

        SourceDto sourceDto = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dataSource), SourceDto.class);

        // 其他类型的 meta type 暂时不做模型推演处理
        String metaType = MetaType.table.name();
        if (DataSourceEnum.isMetaTypeCollection(dataSource.getDatabase_type())) {
            metaType = MetaType.collection.name();
        } else if ("vika".equals(dataSource.getDatabase_type())) {
            metaType = MetaType.VikaDatasheet.name();
        } else if ("qingflow".equals(dataSource.getDatabase_type())) {
            metaType = MetaType.qingFlowApp.name();
        }

        final String _metaType = metaType;
        List<MetadataInstancesDto> metadataInstancesDtos = schemas.stream().map(schema -> {
            MetadataInstancesDto metadataInstancesDto =
                    JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(schema), MetadataInstancesDto.class);

            // 这里需要将 data_type 字段根据字段类型映射规则转换为 数据库类型
            //   需要 根据 所有可匹配条件，尽量缩小匹配结果，选择最优字段类型
            metadataInstancesDto = processFieldToDB(schema, metadataInstancesDto, dataSource, userDetail, options);
            metadataInstancesDto.setAncestorsName(schema.getAncestorsName());

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
            metadataInstancesDto.setLastUserName(userDetail.getUsername());
            metadataInstancesDto.setLastUpdBy(userDetail.getUserId());

            metadataInstancesDto.setQualifiedName(
                    MetaDataBuilderUtils.generateQualifiedName(metadataInstancesDto.getMetaType(), dataSource, schema.getOriginalName()));

            MetaDataBuilderUtils.build(_metaType, dataSource, userDetail.getUserId(), userDetail.getUsername(), metadataInstancesDto.getOriginalName(),
                    metadataInstancesDto, null, dataSourceMetadataInstance.getId().toHexString());

            metadataInstancesDto.setSourceType(SourceTypeEnum.VIRTUAL.name());

            /*MetadataInstancesDto result = metadataInstancesService.upsertByWhere(
                    Where.where("qualified_name", metadataInstancesDto.getQualifiedName()), metadataInstancesDto, userDetail);*/
            return metadataInstancesDto;
        }).collect(Collectors.toList());

        String rollback = options != null ? options.getRollback() : null;
        String rollbackTable = options != null ? options.getRollbackTable() : null;
        long start = System.currentTimeMillis();
        List<String> qualifiedNames = metadataInstancesDtos.stream().map(MetadataInstancesDto::getQualifiedName)
                .filter(Objects::nonNull).collect(Collectors.toList());
        Query query = Query.query(Criteria.where("qualified_name").in(qualifiedNames));
        Map<String, MetadataInstancesEntity> existsMetadataInstances =
                repository.findAll(query, userDetail).stream().collect(Collectors.toMap(MetadataInstancesEntity::getQualifiedName, s -> s, (s1, s2) -> s1));
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            MetadataInstancesEntity existsMetadataInstance = existsMetadataInstances.get(metadataInstancesDto.getQualifiedName());
            if(null == existsMetadataInstance) continue;
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
            metadataInstancesDto.setDatabaseId(dataSourceMetadataInstance.getId().toHexString());
        }

        int modifyCount = metadataInstancesService.bulkSave(metadataInstancesDtos, dataSourceMetadataInstance, dataSource,
               options, userDetail, existsMetadataInstances);
        log.info("Bulk save metadataInstance {}, cost {}ms", modifyCount, System.currentTimeMillis() - start);

        return metadataInstancesDtos.stream()
                .map(dto -> JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(dto), Schema.class)).collect(Collectors.toList());
    }

    /**
     * 根据字段类型映射规则，将模型中的数据库类型转换为通用类型
     * @param metadataInstances 模型元数据记录
     * @param schema 映射后的字段类型将保存在这个对象上
     */
    private Schema processFieldFromDB(MetadataInstancesDto metadataInstances, Schema schema, UserDetail user) {
        String databaseType = metadataInstances.getSource().getDatabase_type();
        String dbVersion = metadataInstances.getSource().getDb_version();
        if (StringUtils.isBlank(dbVersion)) {
            dbVersion = "*";
        }

        Map<String, List<TypeMappingsEntity>> typeMapping =
                typeMappingsService.getTypeMapping(databaseType, TypeMappingDirection.TO_TAPTYPE);

        if (schema.getFields() == null) {
            schema.setFields(new ArrayList<>());
        }

        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(databaseType, user);
        String expression = definitionDto.getExpression();

        TapTable tapTable = PdkSchemaConvert.toPdk(schema);

        PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(tapTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(expression));

        //这里最好是将那些旧参数也带过来
        Schema schema1 = PdkSchemaConvert.fromPdkSchema(tapTable);

        schema.setFields(schema1.getFields());

        schema.setInvalidFields(new ArrayList<>());
        String finalDbVersion = dbVersion;
        schema.getFields().forEach(field -> {
            if (field.isDeleted()) {
                return;
            }

            //field.setOriginalDataType(originalDataType);
            if (field.getDataType() == null) {
                field.setDataType(field.getOriginalDataType());
            }
            if (field.getOriginalDataType() == null) {
                field.setOriginalDataType(field.getDataType());
            }
            String originalDataType = field.getDataType();

            String cacheKey = originalDataType + "-" + finalDbVersion;
            List<TypeMappingsEntity> typeMappings = null;
            if (typeMapping.containsKey(cacheKey)) {
                typeMappings = typeMapping.get(cacheKey);
            } else if (typeMapping.containsKey(originalDataType + "-*")) {
                cacheKey = originalDataType + "-*";
                typeMappings = typeMapping.get(cacheKey);
            }
            if (typeMappings == null || typeMappings.size() == 0) {
                //schema.getInvalidFields().add(field.getFieldName());
                log.error("Not found tap type mapping rule for databaseType={}, dbVersion={}, dbFieldType={}", databaseType, finalDbVersion, originalDataType);
            } else if (typeMappings.size() == 1) {
                field.setDataType(typeMappings.get(0).getTapType());
                field.setTapType(typeMappings.get(0).getTapType());
                field.setFixed(typeMappings.get(0).getFixed());
            } else {
                Integer precision = field.getPrecision();
                Integer scale = field.getScale();
                String dataType = field.getDataType();
                TypeMappingsEntity optimalType = null;

                Function<TypeMappingsEntity, Integer> sortFactor = (TypeMappingsEntity tm1) -> {
                    long factorPrecision = 0;
                    long factorScale = 0;
                    if (precision != null) {
                        Long tm1MinPrecision = tm1.getMinPrecision();
                        Long tm1MaxPrecision = tm1.getMaxPrecision();
                        factorPrecision = (tm1MaxPrecision != null ? tm1MaxPrecision : 0L) -
                                (tm1MinPrecision != null ? tm1MinPrecision : 0L);
                    }
                    if (scale != null) {
                        Long tm1MinScale = tm1.getMinScale();
                        Long tm1MaxScale = tm1.getMaxScale();
                        factorScale = (tm1MaxScale != null ? tm1MaxScale : 0L) -
                                (tm1MinScale != null ? tm1MinScale : 0L);
                    }
                    return Long.valueOf(factorPrecision + factorScale).intValue();
                };

                List<TypeMappingsEntity> optimalTypeList = typeMappings.stream().filter(tm -> {
                    if (precision != null) { // 过滤掉 type mapping 中 precision 为 null 或者 min max 范围不包含 字段长度的规则
                        if (tm.getMinPrecision() == null || tm.getMinPrecision() > precision)
                            return false;
                        if (tm.getMaxPrecision() == null || tm.getMaxPrecision() < precision)
                            return false;
                    }
                    if (scale != null && !"String".equalsIgnoreCase(dataType)) { //过滤掉 type mapping 中 scale 为 null 或者 min max 范围不包含 字段精度的规则
                        if (tm.getMinScale() == null || tm.getMinScale() > scale)
                            return false;
                        if (tm.getMaxScale() == null || tm.getMaxScale() < scale)
                            return false;
                    }

                    if (precision == null && scale == null) {
                        return tm.getMaxPrecision() == null && tm.getMinPrecision() == null &&
                                tm.getMaxScale() == null && tm.getMinScale() == null;
                    } else if (precision == null) {
                        return tm.getMaxPrecision() == null && tm.getMinPrecision() == null;
                    } else if (scale == null) {
                        return tm.getMaxScale() == null && tm.getMinScale() == null;
                    }

                    return true;
                }).sorted((tm1, tm2) -> { // 按照 长度范围、精度范围排序，将最符合的排在上面

                    int tm1Factor = sortFactor.apply(tm1);
                    int tm2Factor = sortFactor.apply(tm2);

                    return tm1Factor - tm2Factor;

                }).collect(Collectors.toList());

                //optimalTypeList = _optimalTypeList.size() > 0 ? _optimalTypeList : typeMappings;
                //}

                if (optimalTypeList.size() == 1) {
                    optimalType = optimalTypeList.get(0);
                }

                if (optimalType == null) {
                    optimalType = typeMappings.get(0);
                }

                if (optimalType != null) {
                    field.setDataType(optimalType.getTapType());
                    field.setTapType(optimalType.getTapType());
                    field.setFixed(optimalType.getFixed());
                }
            }

        });

        return schema;
    }

    /**
     * 根据字段类型映射规则，将模型 schema中的通用字段类型转换为指定数据库字段类型
     * @param schema 包含通用字段类型的模型
     * @param metadataInstancesDto 将映射后的字段类型保存到这里
     * @param dataSourceConnectionDto 数据库类型
     */
    private MetadataInstancesDto processFieldToDB(Schema schema, MetadataInstancesDto metadataInstancesDto, DataSourceConnectionDto dataSourceConnectionDto, UserDetail user,
                                                  DAG.Options options) {

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
        Map<String, List<TypeMappingsEntity>> typeMapping =
                typeMappingsService.getTypeMapping(databaseType, TypeMappingDirection.TO_DATATYPE);

        schema.setInvalidFields(new ArrayList<>());
        String finalDbVersion = dbVersion;
        Map<String, Field> fields = schema.getFields().stream().collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));


        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(databaseType, user);
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

        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create().withTapTypeDataTypeMap(tapMap));
        TapResult<LinkedHashMap<String, TapField>> convert = PdkSchemaConvert.getTargetTypesGenerator().convert(nameFieldMap
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

            if (field.isDeleted()) {
                return;
            }

            Field originalField = fields.get(field.getFieldName());
            String originalDataType = field.getOriginalDataType();
            String dataType = field.getDataType();
            String cacheKey = dataType + "-" + finalDbVersion;
            String tapTypeType = field.getTapType();
            boolean isManual = Field.SOURCE_MANUAL.equals(field.getSource());

            // 标记不支持的字段并直接跳过类型映射
            if (!dataTypeSupportService.supportDataType(sourceNodeDatabaseType, databaseType, originalDataType)) {
                field.setDeleted(true);
                field.setDataTypeSupport(false);

                originalField.setDeleted(true);
                originalField.setDataTypeSupport(false);
                return;
            }

            field.setColumnSize(field.getPrecision());// compatible create table sql for v1 version

            // Match custom type mapping first
            if (!isManual && options != null && options.getCustomTypeMappings() != null && originalDataType != null) {
                Optional<CustomTypeMapping> optional = options.getCustomTypeMappings().stream()
                        .filter(ctm -> originalDataType.equalsIgnoreCase(ctm.getSourceType())).findFirst();
                if (optional.isPresent()) {
                    CustomTypeMapping ctm = optional.get();
                    field.setDataType(ctm.getTargetType());
                    if (ctm.getLength() != null){
                        field.setPrecision(ctm.getLength()); // 长度
                    }
                    if (ctm.getPrecision() != null) {
                        field.setScale(ctm.getPrecision()); // 精度
                    }
                    return;
                }
            }

            if (!isManual && databaseType.equalsIgnoreCase(sourceNodeDatabaseType)) {
                if (originalField != null) {
                    field.setDataType(originalField.getOriginalDataType());
                    return;
                }
            }

            List<TypeMappingsEntity> typeMappings = Collections.emptyList();
            if (typeMapping.containsKey(cacheKey)) {
                typeMappings = typeMapping.get(cacheKey);
            } else if (typeMapping.containsKey(tapTypeType  + "-" + finalDbVersion)) {
                typeMappings = typeMapping.get(tapTypeType  + "-" + finalDbVersion);
            } else if (typeMapping.containsKey(tapTypeType  + "-*")) {
                typeMappings = typeMapping.get(tapTypeType  + "-*");
            } else if (typeMapping.containsKey(dataType + "-*")) {
                cacheKey = dataType + "-*";
                typeMappings = typeMapping.get(cacheKey);
            }

            if (typeMappings == null || typeMappings.size() == 0) {
                field.setDataType(null);
                schema.getInvalidFields().add(field.getFieldName());
                log.error("Not found db type mapping rule for databaseType={}, databaseVersion={}, tapFieldType={}", databaseType, finalDbVersion, dataType);
                return;
            }

            TypeMappingsEntity optimalType = null;
            Integer precision = field.getPrecision();
            Integer scale = field.getScale();
            Function<TypeMappingsEntity, Integer> sortFactor = (TypeMappingsEntity tm1) -> {
                long factorPrecision = 0;
                long factorScale = 0;
                if (precision != null) {
                    Long tm1MinPrecision = tm1.getMinPrecision();
                    Long tm1MaxPrecision = tm1.getMaxPrecision();
                    factorPrecision = (tm1MaxPrecision != null ? tm1MaxPrecision : 0L) -
                            (tm1MinPrecision != null ? tm1MinPrecision : 0L);
                }
                if (scale != null) {
                    Long tm1MinScale = tm1.getMinScale();
                    Long tm1MaxScale = tm1.getMaxScale();
                    factorScale = (tm1MaxScale != null ? tm1MaxScale : 0L) -
                            (tm1MinScale != null ? tm1MinScale : 0L);
                }
                return Long.valueOf(factorPrecision + factorScale).intValue();
            };

            if (StringUtils.isNotBlank(originalDataType)) { // 先根据源库类型名称 与 目标库类型名称相同，找到最优映射规则
                optimalType = typeMappings.stream().filter(typeMappingsEntity -> originalDataType
                        .equalsIgnoreCase(typeMappingsEntity.getDbType())).findFirst().orElse(null);
            }

            List<TypeMappingsEntity> optimalTypeList = typeMappings;

            // 根据源库的 fixed 标记，缩小目标库类型规则范围
            if (optimalTypeList.size() > 0 && originalField != null && originalField.getFixed() != null) {
                List<TypeMappingsEntity> _optimalTypeList = optimalTypeList.stream()
                        .filter(t -> t.getFixed() != null && t.getFixed().equals(originalField.getFixed()))
                        .collect(Collectors.toList());
                if (_optimalTypeList.size() > 0) {
                    optimalTypeList = _optimalTypeList;
                }
            }

            if (optimalType == null) {
                //if(precision != null || scale != null) {
                optimalTypeList = typeMappings.stream().filter(tm -> {
                        if (precision != null) { // 过滤掉 type mapping 中 precision 为 null 或者 min max 范围不包含 字段长度的规则
                            if (tm.getMinPrecision() == null || tm.getMinPrecision() > precision)
                                return false;
                            if (tm.getMaxPrecision() == null || tm.getMaxPrecision() < precision)
                                return false;
                        }
                        if (scale != null && !"String".equalsIgnoreCase(dataType)) { //过滤掉 type mapping 中 scale 为 null 或者 min max 范围不包含 字段精度的规则
                            if (tm.getMinScale() == null || tm.getMinScale() > scale)
                                return false;
                            if (tm.getMaxScale() == null || tm.getMaxScale() < scale)
                                return false;
                        }

                        if (precision == null && scale == null) {
                            return tm.getMaxPrecision() == null && tm.getMinPrecision() == null &&
                                    tm.getMaxScale() == null && tm.getMinScale() == null;
                        } else if (precision == null) {
                            return tm.getMaxPrecision() == null && tm.getMinPrecision() == null;
                        } else if (scale == null) {
                            return tm.getMaxScale() == null && tm.getMinScale() == null;
                        }

                        return true;
                    }).sorted((tm1, tm2) -> { // 按照 长度范围、精度范围排序，将最符合的排在上面

                        int tm1Factor = sortFactor.apply(tm1);
                        int tm2Factor = sortFactor.apply(tm2);

                        return tm1Factor - tm2Factor;

                    }).collect(Collectors.toList());

                    //optimalTypeList = _optimalTypeList.size() > 0 ? _optimalTypeList : typeMappings;
                //}

                if (optimalTypeList.size() == 1) {
                    optimalType = optimalTypeList.get(0);
                }
            }

            if (optimalType == null && optimalTypeList.size() > 1) { // 有多个最优类型，根据 dbTypeDefault=true 过滤
                optimalType = optimalTypeList.stream()
                        .filter(TypeMappingsEntity::isDbTypeDefault)
                        .findFirst().orElse(null);
            }
            if (optimalType == null && optimalTypeList.size() == 0) {
                optimalTypeList = typeMappings;
            }

            if (optimalType == null)
                optimalType = optimalTypeList.stream().filter(TypeMappingsEntity::isTapTypeDefault).findFirst().orElse(null);

            if (optimalType == null && optimalTypeList.size() > 0){
                optimalType = optimalTypeList.get(0);
            }

            if (optimalType == null) {
                field.setDataType(null);
                schema.getInvalidFields().add(field.getFieldName());
                log.error("Not found db type mapping rule for databaseType={}, databaseVersion={}, tapFieldType={}", databaseType, finalDbVersion, dataType);
            } else {
                field.setDataType(optimalType.getDbType());
                field.setOriginalDataType(optimalType.getDbType());
                field.setDataType1(optimalType.getCode()); // 兼容 旧的 convert
                field.setDataCode(optimalType.getCode());
                //field.setSource(com.tapdata.tm.metadatainstance.bean.Field.SOURCE_JOB_ANALYZE);

                if ((optimalType.getTapType().equalsIgnoreCase("String") || optimalType.getTapType().equalsIgnoreCase("Null")) &&
                        (field.getPrecision() == null || field.getPrecision() < 0) &&
                        (optimalType.getMaxPrecision() != null && optimalType.getMaxPrecision() != 0)
                ) {
                    field.setPrecision(100);
                }
                if (optimalType.getMinScale() == null && optimalType.getMaxScale() == null)
                    field.setScale(null);
                if (optimalType.getMinPrecision() == null && optimalType.getMaxPrecision() == null)
                    field.setPrecision( null );
                // 将 字段长度和精度设置为符合默认类型范围的长度和精度
                if (field.getPrecision() != null) {
                    Long minPrecision = ensureLong(optimalType.getMinPrecision());
                    Long maxPrecision = ensureLong(optimalType.getMaxPrecision());
                    if (field.getPrecision() < minPrecision || field.getPrecision() > maxPrecision)
                        field.setPrecision(maxPrecision.intValue());
                }

                if (field.getScale() != null) {
                    Long minScale = ensureLong(optimalType.getMinScale());
                    Long maxScale = ensureLong(optimalType.getMaxScale());
                    if (field.getScale() < minScale || field.getScale() > maxScale)
                        field.setScale(maxScale.intValue());
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
        DataSourceConnectionDto dto = dataSourceService.findById(toObjectId(connectionId));

        UserDetail userDetail = userService.loadUserById(toObjectId(dto.getUserId()));
        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(dto.getDatabase_type(), userDetail);
        dto.setDefinitionPdkId(definitionDto.getPdkId());
        dto.setDefinitionVersion(definitionDto.getVersion());
        dto.setDefinitionGroup(definitionDto.getGroup());
        dto.setDefinitionBuildNumber(String.valueOf(definitionDto.getBuildNumber()));
        DataSourceConnectionDto dataSourceDto = new DataSourceConnectionDto();
        BeanUtils.copyProperties(dto, dataSourceDto);
        return dataSourceDto;
    }

    @Override
    public void updateTransformRate(String taskId, int total, int finished) {
        //需要做一下频率控制 下面算法是每十分之一发一次
        if (((finished -1) * 10 / total) == (finished *10 / total)) {
            return;
        }

        Map<String, Object> data = new HashMap<>();
        data.put("totalTable", total);
        data.put("finishTable", finished);
        data.put("transformStatus", total == finished ? "finished" : "loading");


        Map<String, Object> map = new HashMap<>();
        map.put("type", MessageType.EDIT_FLUSH.getType());
        map.put("opType", "transformRate");
        map.put("taskId", taskId);
        map.put("data", data);
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setData(map);
        queueDto.setType("pipe");

        log.info("build transform rate websocket context, queueDto = {}", queueDto);
        messageQueueService.sendMessage(queueDto);
    }

    @Override
    public boolean checkNewestVersion(String taskId, String nodeId, String version) {
        Criteria criteria = Criteria.where("dataFlowId").is(taskId).and("stageId").is(nodeId);
        MetadataTransformerDto transformer = metadataTransformerService.findOne(new Query(criteria));

        // meta transformer version not equals return;
        if (Objects.nonNull(transformer) && !org.apache.commons.lang3.StringUtils.equals(version, transformer.getVersion())) {
            log.warn("meta transformer version not equals, transformer = {}, uuid = {}", transformer, version);
            return false;
        } else {
            return true;
        }
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
        List<MetadataInstancesDto> all = metadataInstancesService.findAll(query);
        Map<String, MetadataInstancesDto> metaMaps = all.stream().collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m, (m1, m2) -> m1));

        //更新两张中间状态表
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, MetadataTransformerItemEntity.class);
        for (SchemaTransformerResult schemaTransformerResult : schemaTransformerResults) {
            MetadataTransformerItemDto metadataTransformerItemDto = new MetadataTransformerItemDto();
            BeanUtils.copyProperties(schemaTransformerResult, metadataTransformerItemDto);
            metadataTransformerItemDto.setDataFlowId(taskId);
            metadataTransformerItemDto.setSinkNodeId(nodeId);
            metadataTransformerItemDto.setUuid(uuid);
            MetadataInstancesDto metadataInstancesDto = metaMaps.get(schemaTransformerResult.getSinkQulifiedName());
            if (metadataInstancesDto != null) {
                metadataTransformerItemDto.setSinkTableId(metadataInstancesDto.getId().toHexString());
                metadataTransformerItemDto.setSinkObjectName(metadataInstancesDto.getOriginalName());
            }

            List<FieldsMapping> fieldsMappings = getFieldsMapping(metaMaps.get(schemaTransformerResult.getSourceQualifiedName()), metaMaps.get(schemaTransformerResult.getSinkQulifiedName()));
            metadataTransformerItemDto.setFieldsMapping(fieldsMappings);

            Criteria criteria2 = Criteria.where("dataFlowId").is(taskId)
                    .and("sinkNodeId").is(nodeId)
                    .and("sinkTableId").is(metadataTransformerItemDto.getSinkTableId());

            Query query1 = new Query(criteria2);
            Update update = metadataTransformerItemService.buildUpdateSet(metadataTransformerItemDto);
            bulkOperations.upsert(query1, update);
        }

        bulkOperations.execute();

        Query query1 = new Query(Criteria.where("dataFlowId").is(taskId).and("sinkNodeId").is(nodeId).and("uuid").ne(uuid));
        metadataTransformerItemService.deleteAll(query1);

        Criteria criteria = Criteria.where("dataFlowId").is(taskId).and("stageId").is(nodeId);
        MetadataTransformerDto transformer = metadataTransformerService.findOne(new Query(criteria));
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
        metadataTransformerService.save(transformer);
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
        return taskService.findById(new ObjectId(taskId));
    }

    public ObjectId getTaskId() {
        return null;
    }
}
