package com.tapdata.tm.metadatainstance.service;

import cn.hutool.core.bean.BeanUtil;
import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dag.service.DAGService;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.param.ClassificationParam;
import com.tapdata.tm.metadatainstance.param.TablesSupportInspectParam;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.vo.*;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MetadataUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SchemaTransformUtils;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @Author: Zed
 * @Date: 2021/09/11
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MetadataInstancesService extends BaseService<MetadataInstancesDto, MetadataInstancesEntity, ObjectId, MetadataInstancesRepository> {
    private DataSourceService dataSourceService;
    private DataSourceDefinitionService dataSourceDefinitionService;
    private UserService userService;
    private TaskService taskService;

    private DAGService dagService;


    private MetaDataHistoryService metaDataHistoryService;

    public MetadataInstancesDto add(MetadataInstancesDto record, UserDetail user) {
        return save(record, user);
    }


    public MetadataInstancesDto modifyById(ObjectId id, MetadataInstancesDto record, UserDetail user) {
        record.setId(id);

        beforeCreateOrUpdate(record, user);
        beforeUpdateById(id, record);
        save(record, user);
//        afterCreateOrUpdate(record, user);
        afterUpdateById(id, record);
        return null;
    }


    public MetadataInstancesService(@NonNull MetadataInstancesRepository repository) {
        super(repository, MetadataInstancesDto.class, MetadataInstancesEntity.class);
    }

    @Override
    protected void beforeSave(MetadataInstancesDto record, UserDetail user) {
        if (record != null && CollectionUtils.isNotEmpty(record.getFields())) {
            for (Field field : record.getFields()) {
                if (field.getId() == null) {
                    field.setId(ObjectId.get().toHexString());
                }
            }
        }

        beforeCreateOrUpdate(record, user);
    }

    public Page<MetadataInstancesDto> list(Filter filter, UserDetail user) {
        Where where = filter.getWhere();
        //TODO 这个id 后续需要处理一下
        Object sourceId = where.get("source.id");
        where.remove("source.id");

        if (sourceId != null) {
            where.put("source._id", sourceId);
        }
        where.put("is_deleted", ImmutableMap.of("$ne", true));

        if (null != where.get("classifications.id")) {
            Map<String, List> classficitionIn = (Map) where.get("classifications.id");
            List<String> classficitionIds = classficitionIn.get("$in");
            List<ObjectId> objectIdList = new ArrayList<>();
            classficitionIds.forEach(classficitionId -> {
                objectIdList.add(MongoUtils.toObjectId(classficitionId));
            });
            classficitionIn.put("$in", objectIdList);
        }

        Page<MetadataInstancesDto> page = find(filter, user);
        List<MetadataInstancesDto> metadataInstancesDtoList = page.getItems();

        afterFindAll(metadataInstancesDtoList, user);
        afterFind(metadataInstancesDtoList);
        return page;
    }

    /**
     * 数据校验的下拉框使用，不分页返回,不用根据userID 查询
     *
     * @param filter
     * @param userDetail
     * @return
     */
    public List<MetadataInstancesVo> findInspect(Filter filter, UserDetail userDetail) {
        Where where = filter.getWhere();
        Object sourceId = where.get("source.id");
        if (sourceId != null) {
            where.remove("source.id");

            where.put("source._id", sourceId);
        }
        List<MetadataInstancesDto> metadataInstancesDtoList = super.findAll(filter);
        List<MetadataInstancesVo> metadataInstancesVoList = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(metadataInstancesDtoList)) {
            for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtoList) {
                MetadataInstancesVo metadataInstancesVo = BeanUtil.copyProperties(metadataInstancesDto, MetadataInstancesVo.class);
                metadataInstancesVoList.add(metadataInstancesVo);
            }
        }

        return metadataInstancesVoList;
    }

    public MetadataInstancesDto queryById(ObjectId id, com.tapdata.tm.base.dto.Field fields, UserDetail user) {
        MetadataInstancesDto metadata = findById(id, fields, user);
        afterFindOne(metadata, user);
        afterFindById(metadata);
        afterFind(metadata);
        return metadata;
    }


    public MetadataInstancesDto queryByOne(Filter filter, UserDetail user) {
        Where where = filter.getWhere();
        Object sourceId = where.get("source.id");

        if (sourceId != null) {
            where.remove("source.id");

            where.put("source._id", sourceId);
        }
        where.put("is_deleted", false);
        MetadataInstancesDto metadata = findOne(filter, user);
        afterFindOne(metadata, user);
        afterFind(metadata);
        return metadata;
    }

    /**
     * 查询任务统计信息
     *
     * @param skip
     * @param limit
     * @return
     */
    public List<MetadataInstancesDto> jobStats(long skip, int limit) {
        LookupOperation lookUp = LookupOperation.newLookup().
                from("Connections").
                localField("source.connections.target").
                foreignField("_id").
                as("targetConnection");

        Criteria jobCriteria = Criteria.where("meta_type").is("job");
        Criteria targetCriteria = Criteria.where("targetConnection.database_type").is("mongodb");
        AggregationOperation match = Aggregation.match(jobCriteria);
        AggregationOperation match1 = Aggregation.match(targetCriteria);
        ProjectionOperation project = Aggregation.project("classifications", "source.name", "source.stats");
        LimitOperation limitOperation = Aggregation.limit(limit);
        SkipOperation skipOperation = Aggregation.skip(skip);
        Aggregation aggregation = Aggregation.newAggregation(match, lookUp, match1, project, skipOperation, limitOperation);
        List<MetadataInstancesEntity> metadataInstances = repository.getMongoOperations().aggregate(aggregation, "MetadataInstances", MetadataInstancesEntity.class).getMappedResults();

        List<MetadataInstancesDto> metadataInstancesDtos = convertToDto(metadataInstances, MetadataInstancesDto.class);
        return metadataInstancesDtos;
    }

    public List<MetadataInstancesDto> schema(Filter filter, UserDetail user) {
        Page<MetadataInstancesDto> page = find(filter, user);
        if (page.getTotal() != 0) {
            for (MetadataInstancesDto item : page.getItems()) {
                String metaType = item.getMetaType();
                if ((MetaType.mongo_view.name().equals(metaType) && item.getPipline() != null) || MetaType.collection.name().equals(metaType)
                        || MetaType.table.name().equals(metaType) || MetaType.view.name().equals(metaType)) {
                    List<MetadataInstancesDto> objects = new ArrayList<>();
                    objects.add(item);
                    Schema schema = SchemaTransformUtils.newSchema2oldSchema(objects);
                    item.setSchema(schema);

                }
            }
        }
        return page.getItems();
    }

    public List<MetadataInstancesDto> lienage(String id) {

        Criteria criteria = Criteria.where("is_deleted").exists(false);
        Criteria criteria1 = Criteria.where("is_deleted").is(false);
        Criteria deleteCriteria = new Criteria().orOperator(criteria, criteria1);

        GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder().
                from("MetadataInstances").
                startWith("$lienage.qualified_name").
                connectFrom("lienage.qualified_name").
                connectTo("qualified_name").
                restrict(deleteCriteria).
                as("tree");


        ProjectionOperation project = Aggregation.project("_id", "qualified_name", "meta_type",
                "lienage", "name", "tree", "fields", "fields_lienage", "source", "original_name", "table_lienage");
        Aggregation aggregation = Aggregation.newAggregation(graphLookupOperation, project);
        List<MetadataInstancesEntity> metadataInstances = repository.getMongoOperations().aggregate(aggregation, "MetadataInstances", MetadataInstancesEntity.class).getMappedResults();

        List<MetadataInstancesDto> metadataInstancesDtos = convertToDto(metadataInstances, MetadataInstancesDto.class);
        return metadataInstancesDtos;
    }


    public void beforeCreateOrUpdate(MetadataInstancesDto data, UserDetail user) {

        String connectionId = data.getConnectionId();
        DataSourceConnectionDto connectionDto = null;
        if (StringUtils.isNotBlank(connectionId)) {
            connectionId = connectionId.replace("CONN_", "");
        } else {
            com.tapdata.tm.base.dto.Field map = new com.tapdata.tm.base.dto.Field();
            map.put("source._id", true);
            if (data.getId() != null) {
                MetadataInstancesDto old = findById(data.getId(), map);
                if (old != null) {
                    connectionId = old.getSource().get_id();
                }
            }

        }
        if (StringUtils.isNotBlank(connectionId)) {
            connectionDto = dataSourceService.findById(toObjectId(connectionId), user);
        }

        if (MetaType.collection.name().equals(data.getMetaType()) || MetaType.mongo_view.name().equals(data.getMetaType())) {
            if (StringUtils.isBlank(data.getConnectionId())) {
                return;
            }
            Criteria criteria = Criteria.where("source._id").is(connectionId);
            Criteria criteria1 = Criteria.where("source.id").is(toObjectId(connectionId));
            Criteria or = Criteria.where("meta_type").in(MetaType.database.name(), MetaType.directory.name(), MetaType.ftp.name())
                    .orOperator(criteria, criteria1);
            MetadataInstancesDto mObj = findOne(new Query(or));
            if (connectionDto != null) {
                connectionDto.setBuildModelId(connectionDto.getId().toHexString());
                if (StringUtils.isNotBlank(data.getOriginalName())) {
                    data.setQualifiedName(MetaDataBuilderUtils.generateQualifiedName(data.getMetaType(), connectionDto, data.getOriginalName()));
                }
                connectionDto.setSchema(null);
                SourceDto sourceDto = new SourceDto();
                BeanUtils.copyProperties(connectionDto, sourceDto);
                data.setSource(sourceDto);
            }
            if (null != mObj) {
                data.setDatabaseId(mObj.getId().toHexString());
            }
            data.setConnectionId(null);
        }


        if (connectionDto != null) {
            List<Field> fields = data.getFields();
            if (CollectionUtils.isNotEmpty(fields)) {
                for (Field field : fields) {
                    if (StringUtils.isBlank(field.getId())) {
                        field.setDataTypeTemp(field.getDataType());
                        field.setSourceDbType(connectionDto.getDatabase_type());
                        field.setId(new ObjectId().toHexString());
                        field.setSource("auto");
                    }
                }

                TapTable tapTable = PdkSchemaConvert.toPdk(data);
                if (tapTable.getNameFieldMap() != null && tapTable.getNameFieldMap().size() != 0) {
                    LinkedHashMap<String, TapField> updateFieldMap = new LinkedHashMap<>();
                    tapTable.getNameFieldMap().forEach((k, v) -> {
                        if (v.getTapType() == null) {
                            updateFieldMap.put(k, v);
                        }
                    });

                    if (updateFieldMap.size() != 0) {
                        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(connectionDto.getDatabase_type(), user);
                        if (definitionDto != null) {
                            PdkSchemaConvert.tableFieldTypesGenerator.autoFill(updateFieldMap, DefaultExpressionMatchingMap.map(definitionDto.getExpression()));

                            updateFieldMap.forEach((k, v) -> {
                                tapTable.getNameFieldMap().replace(k, v);
                            });
                        }
                        MetadataInstancesDto metadataInstancesDto = PdkSchemaConvert.fromPdk(tapTable);
                        data.setFields(metadataInstancesDto.getFields());
                    }
                }
            }
        }
    }


    private void afterFindById(MetadataInstancesDto result) {
        if (result != null && CollectionUtils.isNotEmpty(result.getFields())) {
            List<Field> fields = result.getFields();
            for (Field field : fields) {
                field.setPrimaryKey(false);

                if (field.getPrimaryKeyPosition() != null) {
                    field.setPrimaryKey(field.getPrimaryKeyPosition() > 0);
                }

                field.setForeignKey(false);
                if (field.getForeignKeyPosition() != null) {
                    field.setForeignKey(field.getForeignKeyPosition() > 0);
                }
            }
        }

    }

    public void afterFindOne(MetadataInstancesDto result, UserDetail user) {
        if (result == null || StringUtils.isBlank(result.getMetaType())) {
            return;
        }

        if (MetaType.database.name().equals(result.getMetaType()) || MetaType.directory.name().equals(result.getMetaType())
                || MetaType.ftp.name().equals(result.getMetaType())) {
            List<String> inMetaTypes = new ArrayList<>();
            inMetaTypes.add(MetaType.collection.name());
            inMetaTypes.add(MetaType.view.name());
            inMetaTypes.add(MetaType.table.name());
            inMetaTypes.add(MetaType.mongo_view.name());
            Criteria criteria = Criteria.where("databaseId").is(result.getId().toHexString()).and("meta_type").in(inMetaTypes).and("is_delete").is(false);
            Query query = new Query(criteria);
            query.fields().include("id", "original_name");
            List<MetadataInstancesDto> collections = findAllDto(query, user);
            result.setCollections(collections);
        } else if (MetaType.collection.name().equals(result.getMetaType()) || MetaType.table.name().equals(result.getMetaType())
                || MetaType.view.name().equals(result.getMetaType()) || MetaType.mongo_view.name().equals(result.getMetaType())) {

            List<String> inMetaTypes = new ArrayList<>();
            inMetaTypes.add(MetaType.database.name());
            inMetaTypes.add(MetaType.directory.name());
            inMetaTypes.add(MetaType.ftp.name());
            Criteria criteria = Criteria.where("_id").is(result.getDatabaseId()).and("meta_type").in(inMetaTypes).and("is_delete").is(false);
            Query query = new Query(criteria);
            query.fields().include("original_name");
            List<MetadataInstancesDto> collections = findAllDto(query, user);
            if (CollectionUtils.isNotEmpty(collections)) {
                result.setDatabase(collections.get(0).getOriginalName());
            }
        }
    }

    public void afterFindAll(List<MetadataInstancesDto> results, UserDetail user) {
        Set<String> userIds = new HashSet<>();
        Set<ObjectId> databaseIds = new HashSet<>();
        for (MetadataInstancesDto result : results) {
            userIds.add(result.getUserId());
            if (StringUtils.isNotBlank(result.getMetaType()) && MetaDataBuilderUtils.metaTypePropertyMap.get(result.getMetaType()).isModel()) {
                databaseIds.add(MongoUtils.toObjectId(result.getDatabaseId()));
            }
        }

        List<UserDto> userDtos = userService.findAll(new Query(Criteria.where("id").in(userIds)));
        Map<String, UserDto> userDtoMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(userDtos)) {
            userDtoMap = userDtos.stream().collect(Collectors.toMap(u -> u.getId().toHexString(), u -> u));
        }

        List<String> inMetaTypes = new ArrayList<>();
        inMetaTypes.add(MetaType.database.name());
        inMetaTypes.add(MetaType.directory.name());
        inMetaTypes.add(MetaType.ftp.name());
        Criteria criteria = Criteria.where("_id").in(databaseIds).and("meta_type").in(inMetaTypes).and("is_delete").is(false);
        Query query = new Query(criteria);
        query.fields().include("id", "original_name");
        List<MetadataInstancesDto> collections = findAllDto(query, user);
        Map<String, String> databaseNameMap = collections.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), MetadataInstancesDto::getOriginalName));

        for (MetadataInstancesDto result : results) {
            SourceDto source = result.getSource();
            if (source != null && StringUtils.isNotBlank(source.getUser_id()) && userDtoMap.get(source.getUser_id()) != null) {
                UserDto userDto = userDtoMap.get(source.getUser_id());
                result.setUsername(StringUtils.isNotBlank(userDto.getUsername()) ? userDto.getUsername() : userDto.getEmail().split("@")[0]);
            }

            if (StringUtils.isNotBlank(result.getMetaType()) && MetaDataBuilderUtils.metaTypePropertyMap.get(result.getMetaType()).isModel()) {
                result.setDatabase(databaseNameMap.get(result.getDatabaseId()));
            }
        }
    }

    public void afterFind(MetadataInstancesDto metadata) {
        List<MetadataInstancesDto> metadatas = new ArrayList<>();
        if (metadata != null) {
            metadatas.add(metadata);
        }
        afterFind(metadatas);
    }

    public void afterFind(List<MetadataInstancesDto> metadatas) {
        for (MetadataInstancesDto result : metadatas) {
            if (CollectionUtils.isNotEmpty(result.getFields())) {
                result.getFields().stream().filter(field -> !field.isDeleted()).forEach(field -> {
                    if (field.getIsNullable() != null && field.getIsNullable() instanceof String) {
                        field.setIsNullable("YES".equals(field.getIsNullable()));
                    }
                });
            }

            if (result.getSource() != null && StringUtils.isNotBlank(result.getMetaType())) {
                String metaType = result.getMetaType();
                if (MetaType.database.name().equals(metaType) || MetaDataBuilderUtils.metaTypePropertyMap.get(metaType).isModel()) {
                    ObjectId connectionId = result.getSource().getId();
                    if (connectionId == null) {
                        connectionId = MongoUtils.toObjectId(result.getSource().get_id());
                    }

                    DataSourceConnectionDto connectionDto = dataSourceService.findById(connectionId);
                    if (connectionDto != null) {
                        DataSourceService.desensitizeMongoConnection(connectionDto);
                        SourceDto sourceDto = new SourceDto();
                        BeanUtils.copyProperties(connectionDto, sourceDto);
                        sourceDto.set_id(sourceDto.getId().toHexString());
                        result.setSource(sourceDto);
                    }

                }
            }

            result.setDevVersion(null);
        }
    }

    /**
     * let mapping = {
     * job: 'Jobs',
     * api: 'Modules',
     * database: 'Connections',
     * dataflow: 'DataFlows'
     * };
     * 为何没有table ？？？
     *
     * @param classificationParamList
     * @return
     */
    public Map<String, Object> classifications(List<ClassificationParam> classificationParamList) {
        Map<String, Object> res = new HashMap<>();
        res.put("rows", 0);
        List<String> failedIds = new ArrayList<>();
        res.put("failed_ids", failedIds);


        int rows = 0;
        Map<String, String> metaTypeToCollection = new HashMap<>();
        metaTypeToCollection.put("job", "Jobs");
        metaTypeToCollection.put("api", "Modules");
        metaTypeToCollection.put("database", "Connections");
        metaTypeToCollection.put("dataflow", "DataFlows");

        for (ClassificationParam classificationParam : classificationParamList) {
            Query query = new Query(Criteria.where("_id").is(classificationParam.getId()));
            Update update = Update.update("classifications", classificationParam.getClassifications());
            UpdateResult updateResult = repository.getMongoOperations().upsert(query, update, "MetadataInstances");
            if (updateResult != null && updateResult.getModifiedCount() > 0) {
                rows += updateResult.getModifiedCount();

                MetadataInstancesDto metadataInstancesDto = findById(MongoUtils.toObjectId(classificationParam.getId()));
                String metaType = metadataInstancesDto.getMetaType();

                Query query1 = new Query(Criteria.where("_id").is(toObjectId(metadataInstancesDto.getSource().get_id())));
                Update update1 = new Update().set("listtags", classificationParam.getClassifications());
                String collectionName = metaTypeToCollection.get(metaType);
                if (!StringUtils.isEmpty(collectionName)) {
                    repository.getMongoOperations().updateFirst(query1, update1, collectionName);
                }

              /*  Object service = serviceMap.get(metaDataRs.getMetaType());

                Query query1 = new Query(Criteria.where("_id").is(toObjectId(metaDataRs.getSource().get_id())));
                Update update1 = new Update().set("listtags", metadata.getClassifications());

                if (service instanceof DataSourceService) {
                    ((DataSourceService) service).update(query1, update1);
                } else {
                    ((BaseService) service).update(query1, update1);
                }*/

            } else {
                failedIds.add(classificationParam.getId());
            }

        }

        res.put("rows", rows);
        return res;
    }


    public void beforeUpdateById(ObjectId id, MetadataInstancesDto data) {
        if (id != null) {
            MetadataInstancesDto metadata = findById(id);
            // TODO __data这种东西先不处理
        }
    }

    //TODO
    public void afterUpdateById(ObjectId id, MetadataInstancesDto data) {
    }


    public MetadataUtil.CompareResult compareHistory(ObjectId id, int historyVersion) {
        MetadataInstancesDto metadata = findById(id);
        MetadataUtil.CompareResult compareResult = null;
        if (metadata != null && CollectionUtils.isNotEmpty(metadata.getHistories())) {
            List<MetadataInstancesDto> histories = metadata.getHistories();
            MetadataInstancesDto secondMeta = null;
            for (MetadataInstancesDto history : histories) {
                if (historyVersion == history.getVersion()) {
                    secondMeta = history;
                }
            }

            if (secondMeta != null) {
                compareResult = MetadataUtil.compare(metadata, secondMeta);
            } else {
                throw new BizException("MetaData.HistoryVersionInvalid", "Metadata history version is invalid, version: " + historyVersion);
            }
        } else {
            throw new BizException("MetaData.HistoryNotFound", "Metadata not found or have no histories, id: " + id);
        }
        return compareResult;
    }


    public List<MetadataInstancesDto> tableConnection(String name, UserDetail user) {
        Criteria criteria = Criteria.where("original_name").regex(name, "i")
                .and("meta_type").in(MetaType.table.name(), MetaType.collection.name(), MetaType.view.name())
                .and("is_deleted").is(false);


        List<MetadataInstancesDto> metaArr = findAllDto(new Query(criteria), user);

        List<ObjectId> connId = new ArrayList<>();
        for (MetadataInstancesDto metadata : metaArr) {
            SourceDto source = metadata.getSource();
            if (source != null) {
                if (source.getId() != null) {
                    connId.add(source.getId());
                }
            }

            Criteria criteria1 = Criteria.where("source.id").in(connId)
                    .and("meta_type").is(MetaType.database.name()).
                    and("is_deleted").ne(true);
            Query query = new Query(criteria1);
            query.fields().include("id", "name", "meta_type", "original_name", "source");
            List<MetadataInstancesDto> connObj = findAllDto(query, user);
            return connObj;

        }
        return null;
    }


    //这个接口应该也是没有地方用的，因为原来的nodejs代码这里有个频繁创建changestream的监听，
//    public void loadSchema(String tablesJson, UserDetail user) {
//        int timeout = SettingsEnum.JOB_HEART_TIMEOUT.getIntValue();
//        long findTime = System.currentTimeMillis() - timeout;
//        Criteria criteria = Criteria.where("worker_type").is("connector").and("ping_time").gte(findTime);
//        WorkerDto worker = workerService.findOne(new Query(criteria));
//        if (worker != null) {
//            MessageQueueDto messageQueueDto = new MessageQueueDto();
//            messageQueueDto.setSender(UUIDUtil.getUUID());
//            messageQueueDto.setReceiver(worker.getProcessId());
//            messageQueueDto.setType("pipe");
//
//            List<Table> tables = JsonUtil.parseJsonUseJackson(tablesJson, new TypeReference<List<Table>>() {});
//            for (Table table : tables) {
//                table.setUserId(user.getUserId());
//            }
//
//            Map<String, Object> data = new HashMap<>();
//            data.put("type", "reloadSchema");
//            data.put("tables", tables);
//            messageQueueService.save(messageQueueDto);
//
//            //TODO change stream相关
//        }
//    }

    //sam说这个不需要实现
    public void dataMap(String level, String tag, String connectionId, String tableName) {

    }


    public List<MetadataInstancesDto> originalData(String isTarget, String qualified_name, UserDetail user) {
        return findByQualifiedName(qualified_name, user);
    }

    public MetadataInstancesDto findBySourceIdAndTableName(String sourceId, String tableName, UserDetail userDetail) {
        Criteria criteria = Criteria
                .where("meta_type").in("table", "collection", "view")
                .and("original_name").is(tableName)
                .and("is_deleted").ne(true)
                .and("source._id").is(sourceId);

        return findOne(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesDto> findBySourceIdAndTableNameList(String sourceId, List<String> tableNames, UserDetail userDetail) {
        Criteria criteria = Criteria
                .where("meta_type").in(Lists.of("table", "collection", "view"))
                .and("is_deleted").ne(true)
                .and("source._id").is(sourceId);

        if (CollectionUtils.isNotEmpty(tableNames)) {
            criteria.and("original_name").in(tableNames);
        }

        return findAllDto(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesEntity> findEntityBySourceIdAndTableNameList(String sourceId, List<String> tableNames, UserDetail userDetail) {
        Criteria criteria = Criteria
                .where("meta_type").in(Lists.of("table", "collection", "view"))
                .and("is_deleted").ne(true)
                .and("source._id").is(sourceId);

        if (CollectionUtils.isNotEmpty(tableNames)) {
            criteria.and("original_name").in(tableNames);
        }

        return findAll(Query.query(criteria), userDetail);
    }

    public Update buildUpdateSet(MetadataInstancesEntity entity) {
        return repository.buildUpdateSet(entity);
    }

    public List<MetadataInstancesDto> findByQualifiedName(String qualifiedName, UserDetail user) {
        Where where = new Where().and("qualified_name", qualifiedName);
        return findAll(where, user);
    }

    public MetadataInstancesDto findByQualifiedNameNotDelete(String qualifiedName, UserDetail user, String... fieldName) {
        Criteria criteria = Criteria.where("qualified_name").is(qualifiedName).and("is_deleted").ne(true);

        Query query = new Query(criteria);
        return findOne(query, user);
    }

    public List<MetadataInstancesDto> findByQualifiedNameList(List<String> qualifiedNames) {
        Criteria criteria = Criteria.where("qualified_name").in(qualifiedNames).and("is_deleted").ne(true);

        Query query = new Query(criteria);
        return findAll(query);
    }



    public List<MetadataInstancesDto> findByQualifiedNameNotDelete(List<String> qualifiedNames, UserDetail user, String... fieldName) {
        Criteria criteria = Criteria.where("qualified_name").in(qualifiedNames).and("is_deleted").ne(true);

        Query query = new Query(criteria);
        return findAllDto(query, user);
    }

    public List<MetadataInstancesDto> findDatabaseScheme(List<String> databaseIds, UserDetail user) {
        Criteria criteria = Criteria.where("source._id").in(databaseIds).and("meta_type").is("database").and("is_deleted").ne(true);

        Query query = new Query(criteria);
        return findAllDto(query, user);
    }

    public int bulkSave(List<MetadataInstancesDto> metadataInstancesDtos,
                        DataSourceConnectionDto dataSourceConnectionDto,
                        UserDetail userDetail,
                        Map<String, MetadataInstancesEntity> existsMetadataInstances) {

        // 这里只保存 original_name, fields;
        //   判定新增模型时，需要设置 create Source 为 推演，执行 MetadataBuilder.build 方法构建新模型
        // 需要比对现有模型，并记录模型历史

        BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
        metadataInstancesDtos.forEach(metadataInstancesDto -> {

            String qualifiedName = metadataInstancesDto.getQualifiedName();

            // 需要增加版本
            if (existsMetadataInstances.containsKey(qualifiedName)) {
                MetadataInstancesEntity existsMetadataInstance = existsMetadataInstances.get(qualifiedName);
                existsMetadataInstance.setVersion(existsMetadataInstance.getVersion() == null ? 1 : existsMetadataInstance.getVersion());
                int newVersion = existsMetadataInstance.getVersion() + 1;

                MetadataInstancesDto historyModel = new MetadataInstancesDto();
                BeanUtils.copyProperties(existsMetadataInstance, historyModel);
                historyModel.setId(null);
                historyModel.setVersionUserId(userDetail.getUserId());
                historyModel.setVersionUserName(userDetail.getUsername());
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
                update.set("original_name", metadataInstancesDto.getOriginalName());
                update.set("name", metadataInstancesDto.getName());

                Query where = Query.query(Criteria.where("id").is(existsMetadataInstance.getId()));
                repository.applyUserDetail(where, userDetail);
                repository.beforeUpsert(update, userDetail);
                bulkOperations.updateOne(where, update);
            } else { // 直接写入

                MetadataInstancesDto _metadataInstancesDto = MetaDataBuilderUtils.build(
                        metadataInstancesDto.getMetaType(), dataSourceConnectionDto, userDetail.getUserId(), userDetail.getUsername(),
                        metadataInstancesDto.getOriginalName(),
                        metadataInstancesDto, null, metadataInstancesDto.getDatabaseId(), "job_analyze", null);

                MetadataInstancesEntity metadataInstance = convertToEntity(MetadataInstancesEntity.class, _metadataInstancesDto);


                //这个操作有可能是插入操作，所以需要校验字段是否又id，如果没有就set id进去
                beforeSave(metadataInstancesDto, userDetail);
                Update update = repository.buildUpdateSet(metadataInstance, userDetail);
                Query where = Query.query(Criteria.where("qualified_name").is(metadataInstance.getQualifiedName()));
                repository.applyUserDetail(where, userDetail);
                repository.beforeUpsert(update, userDetail);
                bulkOperations.upsert(where, update);
            }
        });
        BulkWriteResult result = bulkOperations.execute();
        return result.getModifiedCount();
    }



    public int bulkSave(List<MetadataInstancesDto> insertMetaDataDtos,
                        Map<String, MetadataInstancesDto> updateMetaMap, UserDetail userDetail, boolean saveHistory) {

        BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);

        boolean write = false;

        if (CollectionUtils.isNotEmpty(insertMetaDataDtos)) {
            for (MetadataInstancesDto metadataInstancesDto : insertMetaDataDtos) {
                MetadataInstancesEntity metadataInstance = convertToEntity(MetadataInstancesEntity.class, metadataInstancesDto);


                //这个操作有可能是插入操作，所以需要校验字段是否又id，如果没有就set id进去
                beforeSave(metadataInstancesDto, userDetail);
                Update update = repository.buildUpdateSet(metadataInstance, userDetail);
                Query where = Query.query(Criteria.where("qualified_name").is(metadataInstance.getQualifiedName()));
                repository.applyUserDetail(where, userDetail);
                repository.beforeUpsert(update, userDetail);
                bulkOperations.upsert(where, update);
                write = true;
            }
        }


        if (updateMetaMap != null) {
            for (Map.Entry<String, MetadataInstancesDto> entry : updateMetaMap.entrySet()) {
                MetadataInstancesDto value = entry.getValue();
                List<MetadataInstancesDto> histories = value.getHistories();


                value.setHistories(null);
                MetadataInstancesEntity entity = convertToEntity(MetadataInstancesEntity.class, value);
                Update update = repository.buildUpdateSet(entity, userDetail);

                if (CollectionUtils.isNotEmpty(histories)) {
                    if (saveHistory) {
                        //保存历史，用于自动ddl
                        MetadataInstancesDto metadataInstancesDto = histories.get(0);
                        metadataInstancesDto.setFields(value.getFields());
                        metadataInstancesDto.setIndexes(value.getIndexes());
                        metadataInstancesDto.setDeleted(false);
                        metadataInstancesDto.setCreateSource(value.getCreateSource());
                        metadataInstancesDto.setVersion(value.getVersion());
                        metadataInstancesDto.setHistories(null);
                        insertMetaDataDtos.add(metadataInstancesDto);
                    }

                    Document basicDBObject = new Document("$each", histories);
                    basicDBObject.append("$slice", -5);
                    update.push("histories", basicDBObject);
                }
                Query where = Query.query(Criteria.where("_id").is(entry.getKey()));

                bulkOperations.updateOne(where, update);
                write = true;
            }
        }

        if (write) {
            BulkWriteResult result = bulkOperations.execute();

            //保存历史版本
            if (saveHistory && CollectionUtils.isNotEmpty(insertMetaDataDtos)) {
                metaDataHistoryService.saveHistory(insertMetaDataDtos);
            }
            return result.getModifiedCount();
        } else {
            return 0;
        }
    }

    public Pair<Integer, Integer> bulkUpsetByWhere(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user, Map<String, String> newModelMap) {


        BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
        int modifyCount = 0;
        int insertCount = 0;
        int num = 0;
        for (MetadataInstancesDto dto : metadataInstancesDtos) {
            num++;


            Criteria criteria = Criteria.where("qualified_name").is(newModelMap.get(dto.getOriginalName()));
            Query query = new Query(criteria);
            repository.applyUserDetail(query, user);
            Update update = repository.buildUpdateSet(convertToEntity(MetadataInstancesEntity.class, dto), user);
            repository.beforeUpsert(update, user);

            //这个操作有可能是插入操作，所以需要校验字段是否又id，如果没有就set id进去
            beforeSave(dto, user);
            bulkOperations.upsert(query, update);
            if (num % 1000 == 0) {
                BulkWriteResult execute = bulkOperations.execute();
                modifyCount += execute.getModifiedCount();
                insertCount += execute.getInsertedCount();
            }
        }

        BulkWriteResult execute = bulkOperations.execute();
        modifyCount += execute.getModifiedCount();
        insertCount += execute.getInsertedCount();

        return ImmutablePair.of(modifyCount, insertCount);
    }

    public List<String> tables(String connectId, String sourceType) {
        Criteria criteria = Criteria.where("source._id").is(connectId)
                .and("sourceType").is(sourceType)
                .and("is_deleted").is(false)
                .and("meta_type").in(MetaType.collection.name(), MetaType.table.name());
        Query query = new Query(criteria);
        query.fields().include("original_name");
        List<MetadataInstancesDto> metadataInstancesDtos = findAll(query);
        return metadataInstancesDtos.stream().map(MetadataInstancesDto::getOriginalName).collect(Collectors.toList());
    }

    public TableSupportInspectVo tableSupportInspect(String connectId, String tableName) {
        TableSupportInspectVo tableSupportInspectVo = new TableSupportInspectVo();
        Criteria criteria = Criteria.where("source._id").is(connectId)
                .and("is_deleted").is(false)
                .and("original_name").is(tableName)
                .and("meta_type").in(MetaType.collection.name(), MetaType.table.name());
        Query query = new Query(criteria);
        query.fields().include("fields");
        MetadataInstancesDto metadataInstancesDtos = findOne(query);
        List<Field> fieldList = metadataInstancesDtos.getFields() == null ? new ArrayList<>() : metadataInstancesDtos.getFields();
        Optional primaryKeyPosition = fieldList.stream().filter(field -> field.getPrimaryKeyPosition() > 0).findAny();
        tableSupportInspectVo.setTableName(tableName);
        tableSupportInspectVo.setSupportInspect(primaryKeyPosition.isPresent());
        return tableSupportInspectVo;
    }

    public List<TableSupportInspectVo> tablesSupportInspect(TablesSupportInspectParam tablesSupportInspectParam) {
        List<TableSupportInspectVo> tableSupportInspectVoList = new ArrayList<>();
        Criteria criteria = Criteria.where("source._id").is(tablesSupportInspectParam.getConnectionId())
                .and("is_deleted").is(false)
                .and("original_name").in(tablesSupportInspectParam.getTableNames())
                .and("meta_type").in(MetaType.collection.name(), MetaType.table.name());
        Query query = new Query(criteria);
        query.fields().include("fields", "original_name");
        List<MetadataInstancesDto> metadataInstancesDtos = findAll(query);

        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            TableSupportInspectVo tableSupportInspectVo = new TableSupportInspectVo();
            List<Field> fieldList = metadataInstancesDto.getFields() == null ? new ArrayList<>() : metadataInstancesDto.getFields();
            Optional primaryKeyPosition = fieldList.stream().filter(field -> field.getPrimaryKeyPosition() > 0).findAny();

            tableSupportInspectVo.setTableName(metadataInstancesDto.getOriginalName());
            tableSupportInspectVo.setSupportInspect(primaryKeyPosition.isPresent());
            tableSupportInspectVoList.add(tableSupportInspectVo);
        }
        return tableSupportInspectVoList;
    }


    public Table getMetadata(String connectionId, String metaType, String tableName, UserDetail user) {
        DataSourceConnectionDto connectionDto = dataSourceService.findById(toObjectId(connectionId), user);
        if (connectionDto == null) {
            return null;
        }
        String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, connectionDto, tableName);
        Criteria criteria = Criteria.where("qualified_name").is(qualifiedName);

        MetadataInstancesDto metedata = findOne(new Query(criteria));

        if (metedata != null) {
            return getOldSchema(metedata);
        }
        return null;
    }

    public TapTable getMetadataV2(String connectionId, String metaType, String tableName, UserDetail user) {
        DataSourceConnectionDto connectionDto = dataSourceService.findById(toObjectId(connectionId), user);
        if (connectionDto == null) {
            return null;
        }

        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(connectionDto.getDatabase_type(), user);

        connectionDto.setDefinitionGroup(definitionDto.getGroup());
        connectionDto.setDefinitionPdkId(definitionDto.getPdkId());
        connectionDto.setDefinitionScope(definitionDto.getScope());
        connectionDto.setDefinitionVersion(definitionDto.getVersion());



        String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, connectionDto, tableName);
        Criteria criteria = Criteria.where("qualified_name").is(qualifiedName);

        MetadataInstancesDto metedata = findOne(new Query(criteria));

        if (metedata != null) {
            return PdkSchemaConvert.toPdk(metedata);
        }
        return null;
    }

    public List<Table> findOldByNodeId(Filter filter, UserDetail user) {
        Where where = filter.getWhere();
        if (where == null) {
            return null;
        }
        String nodeId = (String) where.get("nodeId");
        List<MetadataInstancesDto> metadatas = findByNodeId(nodeId, null, user, null);
        if (CollectionUtils.isNotEmpty(metadatas)) {
            return metadatas.stream().map(this::getOldSchema).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    public Map<String, String> findTableMapByNodeId(Filter filter, UserDetail user) {
        Where where = filter.getWhere();
        if (where == null) {
            return null;
        }
        String nodeId = (String) where.get("nodeId");
        return findKVByNode(nodeId, user);
    }

    private Table getOldSchema(MetadataInstancesDto metadata) {
        List<MetadataInstancesDto> tables = new ArrayList<>();
        tables.add(metadata);
        Schema schema = SchemaTransformUtils.newSchema2oldSchema(tables);
        if (schema != null && CollectionUtils.isNotEmpty(schema.getTables())) {
            return schema.getTables().get(0);
        } else {
            return null;
        }
    }


    public Map<String, String> findKVByNode(String nodeId, UserDetail user) {
        Criteria criteria = Criteria.where("dag.nodes.id").is(nodeId);
        Query query = new Query(criteria);
        query.fields().include("dag");
        TaskDto taskDto = taskService.findOne(query, user);

        Map<String, String> kv = new HashMap<>();
        if (taskDto != null && taskDto.getDag() != null) {
            DAG dag = taskDto.getDag();
            Node node = dag.getNode(nodeId);
            kv = getNodeMapping(user, dag, kv, node);
        }
        return kv;
    }

    private Map<String, String> getNodeMapping(UserDetail user, DAG dag, Map<String, String> kv, Node node) {


        if (node instanceof ProcessorNode) {
            kv.put(node.getId(), getQualifiedNameByNodeId(node, user, null, null));
            if (node instanceof MergeTableNode) {
                List<Node> predecessors = dag.predecessors(node.getId());
                for (Node predecessor : predecessors) {
                    getNodeMapping(user, dag, kv, predecessor);
                }

            }
        } else if (node instanceof TableNode) {
            kv.put(((TableNode) node).getTableName(), getQualifiedNameByNodeId(node, user, null, null));
        } else {
            List<MetadataInstancesDto> metadatas = findByNodeId(node.getId(), Lists.of("original_name", "qualified_name"), user, dag);
            if (CollectionUtils.isNotEmpty(metadatas)) {
                kv = metadatas.stream()
                        .collect(Collectors.toMap(MetadataInstancesDto::getOriginalName
                                , MetadataInstancesDto::getQualifiedName, (m1, m2) -> m1));
            }
        }
        return kv;
    }

    public String getQualifiedNameByNodeId(Node node, UserDetail user, DataSourceConnectionDto dataSource, DataSourceDefinitionDto definitionDto) {
        if (node == null) {
            return null;
        }

        if (node instanceof TableNode) {
            if (dataSource == null) {
                dataSource = dataSourceService.findById(MongoUtils.toObjectId(((TableNode) node).getConnectionId()));
            }

            if (definitionDto == null) {
                definitionDto = dataSourceDefinitionService.getByDataSourceType(dataSource.getDatabase_type(), user);
            }

            dataSource.setDefinitionGroup(definitionDto.getGroup());
            dataSource.setDefinitionPdkId(definitionDto.getPdkId());
            dataSource.setDefinitionScope(definitionDto.getScope());
            dataSource.setDefinitionVersion(definitionDto.getVersion());
            String metaType = "table";
            if ("mongodb".equals(dataSource.getDatabase_type())) {
                metaType = "collection";
            }

            String tableName = ((TableNode) node).transformTableName(((TableNode) node).getTableName());
            return MetaDataBuilderUtils.generateQualifiedName(metaType, dataSource, tableName);
        } else if (node instanceof ProcessorNode) {
            return MetaDataBuilderUtils.generateQualifiedName(com.tapdata.tm.commons.util.MetaType.processor_node.name(), node.getId(), null);
        }
        return null;
    }


    public List<MetadataInstancesDto> findByNodeId(String nodeId, List<String> fields, UserDetail user, DAG dag) {

        if (dag == null) {
            Criteria criteria = Criteria.where("dag.nodes.id").is(nodeId);
            Query query = new Query(criteria);
            query.fields().include("dag");
            TaskDto taskDto = taskService.findOne(query, user);
            if (taskDto != null) {
                dag = taskDto.getDag();
            }
        }


        List<MetadataInstancesDto> metadatas = new ArrayList<>();
        if (dag != null) {
            Node node = dag.getNode(nodeId);
            if (node != null) {
                Criteria criteriaTable = Criteria.where("meta_type").in("table", "collection", "view");
                Criteria criteriaNode = Criteria.where("meta_type").is(MetaType.processor_node.name());
                Query queryMetadata = new Query();
                if (CollectionUtils.isNotEmpty(fields)) {
                    String[] fieldArrays = fields.toArray(new String[0]);
                    queryMetadata.fields().include(fieldArrays);
                }
                if (node instanceof TableRenameProcessNode || node instanceof MigrateFieldRenameProcessorNode) {
                    queryMetadata.addCriteria(criteriaNode);
                    String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, null);
                    criteriaNode.and("qualified_name").regex("^"+qualifiedName+".*")
                            .and("is_deleted").ne(true);
                    metadatas.addAll(findAll(queryMetadata));
                } else if (Node.NodeCatalog.processor.equals(node.getCatalog())) {
                    queryMetadata.addCriteria(criteriaNode);
                    String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, null);
                    criteriaNode.and("qualified_name").is(qualifiedName).and("is_deleted").ne(true);
                    metadatas.add(findOne(queryMetadata, user));
                } else if (node instanceof TableNode) {
                    queryMetadata.addCriteria(criteriaTable);
                    TableNode tableNode = (TableNode) node;
                    if (StringUtils.isBlank(tableNode.getTableName())) {
                        return metadatas;
                    }
                    criteriaTable.and("source._id").is(tableNode.getConnectionId())
                            .and("original_name").is(tableNode.getTableName()).and("is_deleted").ne(true);
                    metadatas.add(findOne(queryMetadata, user));
                } else if (node instanceof DatabaseNode) {
                    queryMetadata.addCriteria(criteriaTable);
                    DatabaseNode tableNode = (DatabaseNode) node;
                    List<String> tableNames;
                    if (dag.getSources().contains(tableNode)) {
                        tableNames = tableNode.getTableNames();
                    } else if (dag.getTargets().contains(tableNode)) {
                        tableNames = tableNode.getSyncObjects().get(0).getObjectNames();
                    } else {
                        throw new BizException("table node is error nodeId:" + tableNode.getId());
                    }
                    criteriaTable.and("source._id").is(tableNode.getConnectionId())
                            .and("originalName").in(tableNames).and("is_deleted").ne(true);
                    metadatas = findAllDto(queryMetadata, user);
                } else if (node instanceof LogCollectorNode) {
                    LogCollectorNode logNode = (LogCollectorNode) node;
                    List<String> connectionIds = logNode.getConnectionIds();
                    if (CollectionUtils.isNotEmpty(connectionIds)) {
                        String connectionId = connectionIds.get(0);
                        queryMetadata.addCriteria(criteriaTable);
                        criteriaTable.and("source._id").is(connectionId)
                                .and("originalName").in(logNode.getTableNames()).and("is_deleted").ne(true);
                        metadatas = findAllDto(queryMetadata, user);
                    }
                }
            }

        }
        metadatas = metadatas.stream().filter(Objects::nonNull).collect(Collectors.toList());
        return metadatas;
    }

    public List<Map<String, Object>> search(String type, String keyword, String lastId, Integer pageSize, UserDetail user) {
        List<String> metaTypes = Lists.newArrayList("table", "collection");
        Criteria criteria;
        if ("table".equals(type)) {
            criteria = Criteria.where("meta_type").in(metaTypes)
                    .orOperator(Criteria.where("original_name").regex(keyword), Criteria.where("name").regex(keyword)
                            , Criteria.where("comment").regex(keyword));
        } else if ("column".equals(type)) {
            criteria = Criteria.where("meta_type").in(metaTypes)
                    .orOperator(Criteria.where("fields.field_name").regex(keyword), Criteria.where("fields.alias_name").regex(keyword)
                            /*, Criteria.where("fields.comment").regex(keyword)*/);
        } else {
            throw new BizException("IllegalArgument", type);
        }

        if (StringUtils.isNotBlank(lastId)) {
            criteria.and("_id").gt(MongoUtils.toObjectId(lastId));
        }
        criteria.and("is_deleted").ne(true);
        Query query = new Query(criteria);
        query.with(Sort.by("_id").ascending());
        query.limit(pageSize);

        List<MetadataInstancesDto> metadatas = findAllDto(query, user);
        if (CollectionUtils.isEmpty(metadatas)) {
            return null;
        }
        List<Map<String, Object>> resArr = new ArrayList<>();
        for (MetadataInstancesDto item : metadatas) {
            Map<String, Object> obj = new HashMap<>();
            obj.put("id", item.getId().toString());
            Map<String, Object> table = new HashMap<>();
            table.put("name", item.getName());
            table.put("original_name", item.getOriginalName());
            table.put("comment", item.getComment() == null ? "" : item.getComment());
            obj.put("table", table);

            if ("column".equals(type)) {
                List<Map<String, Object>> column = new ArrayList<>();
                for (Field field : item.getFields()) {
                    StringBuilder sb = new StringBuilder("");
                    if (field.getFieldName() != null) {
                        sb.append(field.getFieldName()).append(" , ");
                    }
                    if (field.getAliasName() != null) {
                        sb.append(field.getAliasName()).append(" , ");
                    }
                    if (field.getComment() != null) {
                        sb.append(field.getComment()).append(" , ");
                    }
                    if (sb.toString().contains(keyword)) {
                        Map<String, Object> colObj = new HashMap<>();
                        colObj.put("field_name", field.getFieldName());
                        colObj.put("original_field_name", field.getOriginalFieldName());
                        colObj.put("comment", field.getComment());
                        colObj.put("type", field.getJavaType());
                        column.add(colObj);
                    }
                }
                obj.put("columns", column);
            }
            resArr.add(obj);
        }
        return resArr;

    }

    public List<MetaTableVo> tableSearch(String connectionId, String keyword, String lastId, Integer pageSize, UserDetail user) {
        Criteria criteria =
                Criteria.where("source._id").is(connectionId)
                        .and("is_deleted").ne(true)
                        .and("meta_type").in(MetaType.collection.name(), MetaType.table.name())
                        .orOperator(Criteria.where("original_name").regex(keyword), Criteria.where("name").regex(keyword)
                                , Criteria.where("comment").regex(keyword));

        if (StringUtils.isNotBlank(lastId)) {
            criteria.and("_id").gt(new ObjectId(lastId));
        }
        Query query = new Query(criteria);
        if (pageSize != 0) {
            query.limit(pageSize);
        }
        query.with(Sort.by(Sort.Order.asc("_id")));

        List<MetadataInstancesDto> metaData = findAllDto(query, user);
        if (CollectionUtils.isEmpty(metaData)) {
            return null;
        }
        List<MetaTableVo> resArr = Lists.newArrayList();
        for (MetadataInstancesDto item : metaData) {
            MetaTableVo tableVo = new MetaTableVo();
            tableVo.setId(item.getId().toString());
            tableVo.setName(item.getName());
            tableVo.setOriginalName(item.getOriginalName());
            tableVo.setComment(item.getComment() == null ? "" : item.getComment());

            resArr.add(tableVo);
        }
        return resArr;

    }

    public MetaTableCheckVo checkTableNames(String connectionId, List<String> names, UserDetail user) {
        List<String> metaTypes = Lists.newArrayList(MetaType.table.name());
        Criteria criteria = Criteria.where("source._id").is(connectionId)
                .and("is_deleted").is(false)
                .and("meta_type").in(metaTypes)
                .and("original_name").in(names);
        Query query = new Query(criteria);

        List<MetadataInstancesDto> metaData = findAllDto(query, user);
        if (CollectionUtils.isEmpty(metaData)) {
            return null;
        }

        List<String> collect = metaData.stream().map(MetadataInstancesDto::getName).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(collect)) {
            return null;
        }

        List<String> exitsTables = Lists.newArrayList();
        List<String> errorTables = Lists.newArrayList();
        names.forEach(t -> {
            if (collect.contains(t)) {
                exitsTables.add(t);
            } else {
                errorTables.add(t);
            }
        });

        return new MetaTableCheckVo(exitsTables, errorTables);
    }


    public Page findMetadataList(Filter filter, UserDetail userDetail) {
        Page page = find(filter, userDetail);
        return page;
    }


    public TableListVo findTablesById(String id) {
//        List<Map<String, String>> collections = new ArrayList<>();
        MetadataInstancesDto metadataInstancesDto = findById(MongoUtils.toObjectId(id));
        if (null != metadataInstancesDto.getSource()) {
//            String databaseId = metadataInstancesDto.getSource().get_id();

      /*         List<MetadataInstancesDto> metadataInstancesTable = findAll(Query.query(Criteria.where("source._id").is(databaseId)
                    .and("is_deleted").ne(true)
                    .orOperator(Criteria.where("meta_type").is(MetaType.table.toString()), Criteria.where("meta_type").is(MetaType.collection.toString()))));
         if (CollectionUtils.isNotEmpty(metadataInstancesTable)) {
                metadataInstancesTable.forEach(singleTable -> {
                    Map collectionMap = new HashMap();
                    collectionMap.put("id", singleTable.getId().toString());
                    collectionMap.put("name", singleTable.getOriginalName());
                    collections.add(collectionMap);
                });
            }*/
        }
        TableListVo tableListVo = BeanUtil.copyProperties(metadataInstancesDto, TableListVo.class);
//        tableListVo.setCollections(collections);
        return tableListVo;
    }


    public void batchImport(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user, boolean cover) {
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            long count = count(new Query(new Criteria().orOperator(Criteria.where("_id").is(metadataInstancesDto.getId()),
                    Criteria.where("qualified_name").is(metadataInstancesDto.getQualifiedName()))));
            if (count == 0) {
                repository.importEntity(convertToEntity(MetadataInstancesEntity.class, metadataInstancesDto), user);
            } else if (cover) {
                save(metadataInstancesDto, user);
            }
        }
    }


    public Page<TapTable> getTapTable(Filter filter, UserDetail loginUser) {

        Page<MetadataInstancesDto> list = list(filter, loginUser);
        Page<TapTable> tapTablePage = new Page<>();
        tapTablePage.setTotal(list.getTotal());
        List<TapTable> tapTables = new ArrayList<>();
        List<MetadataInstancesDto> items = list.getItems();
        if (CollectionUtils.isNotEmpty(items)) {
            tapTables = items.stream().map(PdkSchemaConvert::toPdk).collect(Collectors.toList());
        }

        tapTablePage.setItems(tapTables);
        return tapTablePage;
    }

    public List<Field> getMergeNodeParentField(String taskId, String nodeId, UserDetail user) {
        TaskDto taskDto = taskService.findById(toObjectId(taskId), user);
        DAG dag = taskDto.getDag();
        List<Node> successors = dag.successors(nodeId);
        MergeTableNode node = null;
        for (Node successor : successors) {
            if (successor instanceof MergeTableNode) {
                node = (MergeTableNode) successor;
                break;
            }
        }


        if (node != null) {
            List<List<Field>> parentFields = new ArrayList<>();
            List<String> parentNodes = null;
            List<MergeTableProperties> mergeProperties = node.getMergeProperties();

            //获取目标节点的父节点id
            for (MergeTableProperties mergeProperty : mergeProperties) {
                List<String> list = new ArrayList<>();
                parentNodes = getParentNode(list, mergeProperty, nodeId);
                if (parentNodes != null) {
                    break;
                }
            }

            if (parentNodes != null) {
                node.setService(dagService);
                for (String parentNode : parentNodes) {
                    String qualifiedName = node.getQualifiedNameByNodeId(dag, parentNode);
                    MetadataInstancesDto metadataInstancesDto = findByQualifiedNameNotDelete(qualifiedName, user);
                    List<Field> fields = metadataInstancesDto.getFields();
                    parentFields.add(fields);
                }
            }

            if (CollectionUtils.isNotEmpty(parentFields)) {
//                Map<String, Field> fieldMap = parentFields.stream().flatMap(List::stream).collect(Collectors.toMap(Field::getFieldName, f -> f, (f1, f2) -> f1));
//                return new ArrayList<>(fieldMap.values());
                List<String> collect = parentFields.stream().flatMap(List::stream).map(Field::getId).collect(Collectors.toList());

                String qualifiedName = node.getQualifiedNameByNodeId(dag, node.getId());

                MetadataInstancesDto processNode = findByQualifiedNameNotDelete(qualifiedName, user);
                List<Field> fields = processNode.getFields();
                if (CollectionUtils.isNotEmpty(fields)) {
                    return fields.stream().filter(f -> collect.contains(f.getId())).collect(Collectors.toList());
                }

            }
        }

        return Lists.newArrayList();
    }

    private List<String> getParentNode(List<String> parents, MergeTableProperties mergeTableProperties, String targetId) {
        if (targetId.equals(mergeTableProperties.getId())) {
            return parents;
        }

        List<MergeTableProperties> children = mergeTableProperties.getChildren();
        parents.add(mergeTableProperties.getId());
        for (MergeTableProperties child : children) {
            List<String> parentNode = getParentNode(new ArrayList<>(parents), child, targetId);
            if (parentNode != null) {
                return parentNode;
            }
        }

        return null;
    }
}
