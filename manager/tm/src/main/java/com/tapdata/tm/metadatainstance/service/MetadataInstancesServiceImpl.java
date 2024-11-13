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
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.MigrateProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.dag.vo.FieldChangeRuleGroup;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import com.tapdata.tm.commons.util.FilterMetadataInstanceUtil;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.commons.util.MetaType;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import com.tapdata.tm.commons.util.RemoveBracketsUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dag.service.DAGService;
import com.tapdata.tm.discovery.bean.DiscoveryFieldDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionServiceImpl;
import com.tapdata.tm.ds.service.impl.DataSourceServiceImpl;
import com.tapdata.tm.metadatainstance.bean.MultiPleTransformReq;
import com.tapdata.tm.metadatainstance.dto.DataType2TapTypeDto;
import com.tapdata.tm.metadatainstance.dto.DataTypeCheckMultipleVo;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.param.ClassificationParam;
import com.tapdata.tm.metadatainstance.param.TablesSupportInspectParam;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.vo.MetaTableCheckVo;
import com.tapdata.tm.metadatainstance.vo.MetaTableVo;
import com.tapdata.tm.metadatainstance.vo.MetadataInstancesVo;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import com.tapdata.tm.metadatainstance.vo.TableListVo;
import com.tapdata.tm.metadatainstance.vo.TableSupportInspectVo;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.dto.UserDto;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MetadataUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SchemaTransformUtils;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapStringMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.DataMap;
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
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.ComparisonOperators;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators;
import org.springframework.data.mongodb.core.aggregation.GraphLookupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.LookupOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SkipOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.MongoUtils.applyField;
import static com.tapdata.tm.utils.MongoUtils.applySort;
import static com.tapdata.tm.utils.MongoUtils.toObjectId;

/**
 * @Author: Zed
 * @Date: 2021/09/11
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MetadataInstancesServiceImpl extends MetadataInstancesService{
    private DataSourceServiceImpl dataSourceService;
    private DataSourceDefinitionServiceImpl dataSourceDefinitionService;
    private UserService userService;
    private TaskService taskService;
    private DAGService dagService;
    private MetaDataHistoryService metaDataHistoryService;
    private MongoTemplate mongoTemplate;
    public static final String IS_DELETED = "is_deleted";
    public static final String IS_DELETE = "is_delete";
    public static final String SOURCE_ID = "source._id";
    public static final String SOURCE_ID_WITHOUT_UNDERLINE = "source.id";
    public static final String TASK_ID = "taskId";
    public static final String META_TYPE = "meta_type";
    public static final String QUALIFIED_NAME = "qualified_name";
    public static final String ORIGINAL_NAME = "original_name";
    public static final String LOWER_CAME_ORIGINAL_NAME = "originalName";
    public static final String SOURCE_TYPE = "sourceType";
    public static final String NODE_ID = "nodeId";
    public static final String COMMENT = "comment";
    public static final String MONGODB = "mongodb";
    public static final String METADATA_INSTANCES = "MetadataInstances";
    public static final String FIELDS = "fields";
    public static final String TABLE = "table";
    public static final String COLLECTION = "collection";
    public static final String HISTORIES = "histories";
    public static final String TRANSFORM_UUID = "transformUuid";
    public static final String TABLE_NAME = "tableName";
    public static final String DAG_NODES_ID = "dag.nodes.id";
    public static final String TABLE_ID = "tableId";
    public static final String ILLEGAL_ARGUMENT = "IllegalArgument";
    public static final String TABLE_COMMENT = "tableComment";
    public static final String PARTITION_MASTER_TABLE_ID = "partitionMasterTableId";
    public static final int UPSERT_BATCH_SIZE = 1000;

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


    public MetadataInstancesServiceImpl(@NonNull MetadataInstancesRepository repository) {
        super(repository);
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
        if (null == record) return;
        beforeCreateOrUpdate(record, user);
    }

    public Page<MetadataInstancesDto> list(Filter filter, UserDetail user) {
        Where where = filter.getWhere();
        //TODO 这个id 后续需要处理一下
        Object sourceId = where.get(SOURCE_ID_WITHOUT_UNDERLINE);
        where.remove(SOURCE_ID_WITHOUT_UNDERLINE);

        if (sourceId != null) {
            where.put(SOURCE_ID, sourceId);
        }
        where.put(IS_DELETED, ImmutableMap.of("$ne", true));

        if (null != where.get("classifications.id")) {
            Map<String, List> classficitionIn = (Map) where.get("classifications.id");
            List<String> classficitionIds = classficitionIn.get("$in");
            List<ObjectId> objectIdList = new ArrayList<>();
            classficitionIds.forEach(classficitionId -> {
                objectIdList.add(MongoUtils.toObjectId(classficitionId));
            });
            classficitionIn.put("$in", objectIdList);
        }

        Page<MetadataInstancesDto> page = null;
        if (filter.getWhere().containsKey(TASK_ID)) {
            Object taskId = filter.getWhere().remove(TASK_ID);
            Criteria criteria = repository.whereToCriteria(filter.getWhere());
            criteria.orOperator(Criteria.where(TASK_ID).is(taskId), Criteria.where(TASK_ID).exists(false));
            // maybe model deduction slow then task model not save, could query physics table meta

            Query query = new Query(criteria);
            long count = count(query);

            if (filter.getLimit() > 0) {
                query.limit(filter.getLimit());
            } else {
                query.limit(20);
            }
            query.skip(Math.max(filter.getSkip(), 0));
            query.fields().include(COMMENT);
            applyField(query, filter.getFields());
            applySort(query, filter.getSort());
            List<MetadataInstancesDto> allDto = findAll(query);
            page = new Page<>();
            page.setTotal(count);
            page.setItems(allDto);
        } else {
            page = find(filter);
        }
        List<MetadataInstancesDto> metadataInstancesDtoList = page.getItems();
        afterFindAll(metadataInstancesDtoList);
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
        Object sourceId = where.get(SOURCE_ID_WITHOUT_UNDERLINE);
        if (sourceId != null) {
            where.remove(SOURCE_ID_WITHOUT_UNDERLINE);

            where.put(SOURCE_ID, sourceId);
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
        Object sourceId = where.get(SOURCE_ID_WITHOUT_UNDERLINE);

        if (sourceId != null) {
            where.remove(SOURCE_ID_WITHOUT_UNDERLINE);

            where.put(SOURCE_ID, sourceId);
        }
        where.put(IS_DELETED, false);
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

        Criteria jobCriteria = Criteria.where(META_TYPE).is("job");
        Criteria targetCriteria = Criteria.where("targetConnection.database_type").is(MONGODB);
        AggregationOperation match = Aggregation.match(jobCriteria);
        AggregationOperation match1 = Aggregation.match(targetCriteria);
        ProjectionOperation project = Aggregation.project("listtags", "source.name", "source.stats");
        LimitOperation limitOperation = Aggregation.limit(limit);
        SkipOperation skipOperation = Aggregation.skip(skip);
        Aggregation aggregation = Aggregation.newAggregation(match, lookUp, match1, project, skipOperation, limitOperation);
        List<MetadataInstancesEntity> metadataInstances = repository.getMongoOperations().aggregate(aggregation, METADATA_INSTANCES, MetadataInstancesEntity.class).getMappedResults();

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

        Criteria criteria = Criteria.where(IS_DELETED).exists(false);
        Criteria criteria1 = Criteria.where(IS_DELETED).is(false);
        Criteria deleteCriteria = new Criteria().orOperator(criteria, criteria1);

        GraphLookupOperation graphLookupOperation = GraphLookupOperation.builder().
                from(METADATA_INSTANCES).
                startWith("$lienage.qualified_name").
                connectFrom("lienage.qualified_name").
                connectTo(QUALIFIED_NAME).
                restrict(deleteCriteria).
                as("tree");


        ProjectionOperation project = Aggregation.project("_id", QUALIFIED_NAME, META_TYPE,
                "lienage", "name", "tree", FIELDS, "fields_lienage", "source", ORIGINAL_NAME, "table_lienage");
        Aggregation aggregation = Aggregation.newAggregation(graphLookupOperation, project);
        List<MetadataInstancesEntity> metadataInstances = repository.getMongoOperations().aggregate(aggregation, METADATA_INSTANCES, MetadataInstancesEntity.class).getMappedResults();

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
            map.put(SOURCE_ID, true);
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
            Criteria criteria = Criteria.where(SOURCE_ID).is(connectionId);
            Criteria criteria1 = Criteria.where(SOURCE_ID_WITHOUT_UNDERLINE).is(toObjectId(connectionId));
            Criteria or = Criteria.where(META_TYPE).in(MetaType.database.name(), MetaType.directory.name(), MetaType.ftp.name())
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
                        field.setId(MetaDataBuilderUtils.generateFieldId(connectionId, data.getOriginalName(), field.getFieldName()));
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
                            PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(updateFieldMap, DefaultExpressionMatchingMap.map(definitionDto.getExpression()));

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


    protected void afterFindById(MetadataInstancesDto result) {
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
            Criteria criteria = Criteria.where("databaseId").is(result.getId().toHexString()).and(META_TYPE).in(inMetaTypes).and(IS_DELETE).is(false);
            Query query = new Query(criteria);
            query.fields().include("id", ORIGINAL_NAME);
            List<MetadataInstancesDto> collections = findAllDto(query, user);
            result.setCollections(collections);
        } else if (MetaType.collection.name().equals(result.getMetaType()) || MetaType.table.name().equals(result.getMetaType())
                || MetaType.view.name().equals(result.getMetaType()) || MetaType.mongo_view.name().equals(result.getMetaType())) {

            List<String> inMetaTypes = new ArrayList<>();
            inMetaTypes.add(MetaType.database.name());
            inMetaTypes.add(MetaType.directory.name());
            inMetaTypes.add(MetaType.ftp.name());
            Criteria criteria = Criteria.where("_id").is(result.getDatabaseId()).and(META_TYPE).in(inMetaTypes).and(IS_DELETE).is(false);
            Query query = new Query(criteria);
            query.fields().include(ORIGINAL_NAME);
            List<MetadataInstancesDto> collections = findAllDto(query, user);
            if (CollectionUtils.isNotEmpty(collections)) {
                result.setDatabase(collections.get(0).getOriginalName());
            }
        }
    }

    public void afterFindAll(List<MetadataInstancesDto> results) {
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
        Criteria criteria = Criteria.where("_id").in(databaseIds).and(META_TYPE).in(inMetaTypes).and(IS_DELETE).is(false);
        Query query = new Query(criteria);
        query.fields().include("id", ORIGINAL_NAME);
        List<MetadataInstancesDto> collections = findAll(query);
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
            if(StringUtils.isBlank(result.getComment())){
                result.setComment("");
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
                        DataSourceServiceImpl.desensitizeMongoConnection(connectionDto);
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
            UpdateResult updateResult = repository.getMongoOperations().upsert(query, update, METADATA_INSTANCES);
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
            List<Field> fieldsAfter = data.getFieldsAfter();
            if (fieldsAfter != null) {
                BeanUtil.copyProperties(metadata, data);
                data.setFieldsAfter(fieldsAfter);
            }
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
        Criteria criteria = Criteria.where(ORIGINAL_NAME).regex(name, "i")
                .and(META_TYPE).in(MetaType.table.name(), MetaType.collection.name(), MetaType.view.name())
                .and(IS_DELETED).is(false);


        List<MetadataInstancesDto> metaArr = findAllDto(new Query(criteria), user);

        List<ObjectId> connId = new ArrayList<>();
        for (MetadataInstancesDto metadata : metaArr) {
            SourceDto source = metadata.getSource();
            if (source != null) {
                if (source.getId() != null) {
                    connId.add(source.getId());
                }
            }

            Criteria criteria1 = Criteria.where(SOURCE_ID_WITHOUT_UNDERLINE).in(connId)
                    .and(META_TYPE).is(MetaType.database.name()).
                    and(IS_DELETED).ne(true);
            Query query = new Query(criteria1);
            query.fields().include("id", "name", META_TYPE, ORIGINAL_NAME, "source");
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

    public MetadataInstancesDto findBySourceIdAndTableName(String sourceId, String tableName, String taskId, UserDetail userDetail) {
        Criteria criteria = Criteria
                .where(META_TYPE).in(TABLE, COLLECTION, "view")
                .and(ORIGINAL_NAME).is(tableName)
                .and(IS_DELETED).ne(true)
                .and(SOURCE_ID).is(sourceId)
                .and(TASK_ID).is(taskId);

        return findOne(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesDto> findSourceSchemaBySourceId(String sourceId, List<String> tableNames, UserDetail userDetail, String... fields) {
        Criteria criteria = Criteria
                .where(META_TYPE).in(Lists.of(TABLE, COLLECTION, "view"))
                .and(IS_DELETED).ne(true)
                .and(SOURCE_ID).is(sourceId)
                .and(SOURCE_TYPE).is(SourceTypeEnum.SOURCE.name())
                .and(TASK_ID).exists(false);
        if (CollectionUtils.isNotEmpty(tableNames)) {
            criteria.and(ORIGINAL_NAME).in(tableNames);
        }

        Query query = new Query(criteria);
        if (fields != null && fields.length > 0) {
            query.fields().include(fields);
        }
        return findAllDto(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesDto> findBySourceIdAndTableNameList(String sourceId, List<String> tableNames, UserDetail userDetail, String taskId) {
        Criteria criteria = Criteria
                .where(META_TYPE).in(Lists.of(TABLE, COLLECTION, "view"))
                .and(IS_DELETED).ne(true)
                .and(SOURCE_ID).is(sourceId)
                .and(TASK_ID).is(taskId);

        if (CollectionUtils.isNotEmpty(tableNames)) {
            criteria.and(ORIGINAL_NAME).in(tableNames);
        }

        return findAllDto(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesDto> findBySourceIdAndTableNameListNeTaskId(String sourceId, List<String> tableNames, UserDetail userDetail) {
        Criteria criteria = Criteria
                .where(META_TYPE).in(Lists.of(TABLE, COLLECTION, "view"))
                .and(IS_DELETED).ne(true)
                .and(SOURCE_ID).is(sourceId)
                .and(TASK_ID).exists(false);

        if (CollectionUtils.isNotEmpty(tableNames)) {
            criteria.and(ORIGINAL_NAME).in(tableNames);
        }

        return findAllDto(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesEntity> findEntityBySourceIdAndTableNameList(String sourceId, List<String> tableNames, UserDetail userDetail, String taskId) {
        Criteria criteria = Criteria
                .where(META_TYPE).in(Lists.of(TABLE, COLLECTION, "view"))
                .and(IS_DELETED).ne(true)
                .and(SOURCE_ID).is(sourceId)
                .and(TASK_ID).is(taskId);

        if (CollectionUtils.isNotEmpty(tableNames)) {
            criteria.and(ORIGINAL_NAME).in(tableNames);
        }

        return findAll(Query.query(criteria), userDetail);
    }

    public Update buildUpdateSet(MetadataInstancesEntity entity) {
        return repository.buildUpdateSet(entity);
    }

    public List<MetadataInstancesDto> findByQualifiedName(String qualifiedName, UserDetail user) {
        Where where = new Where().and(QUALIFIED_NAME, qualifiedName);
        return findAll(where, user);
    }

    public MetadataInstancesDto findByQualifiedNameNotDelete(String qualifiedName, UserDetail user, String... fieldName) {
        Criteria criteria = Criteria.where(QUALIFIED_NAME).is(qualifiedName).and(IS_DELETED).ne(true);

        Query query = new Query(criteria);
        return findOne(query);
    }

    public List<MetadataInstancesDto> findByQualifiedNameList(List<String> qualifiedNames, String taskId) {
        Criteria criteria = Criteria.where(QUALIFIED_NAME).in(qualifiedNames)
                .and(IS_DELETED).ne(true)
                .and(TASK_ID).is(taskId);

        Query query = new Query(criteria);
        return findAll(query);
    }



    public List<MetadataInstancesDto> findByQualifiedNameNotDelete(List<String> qualifiedNames, UserDetail user, String... excludeFiled) {
        Criteria criteria = Criteria.where(QUALIFIED_NAME).in(qualifiedNames).and(IS_DELETED).ne(true);

        Query query = new Query(criteria);
        query.fields().exclude(excludeFiled);
        return findAllDto(query, user);
    }

    public List<MetadataInstancesDto> findDatabaseSchemeNoHistory(List<String> databaseIds, UserDetail user) {
        Criteria criteria = Criteria.where(SOURCE_ID).in(databaseIds).and(META_TYPE).is("database").and(IS_DELETED).ne(true);

        Query query = new Query(criteria);
        query.fields().exclude(HISTORIES);
        return findAllDto(query, user);
    }

    public int bulkSave(List<MetadataInstancesDto> metadataInstancesDtos,
                        MetadataInstancesDto dataSourceMetadataInstance,
                        DataSourceConnectionDto dataSourceConnectionDto,
                        DAG.Options options,
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

                Map<String, Field> existsFieldMap = existsMetadataInstance.getFields().stream()
                        .collect(Collectors.toMap(Field::getOriginalFieldName, f -> f, (f1, f2) -> f1));

                HashMap<String, Field> fields = new HashMap<>();
                metadataInstancesDto.getFields().forEach(field -> {
                    if (existsFieldMap.containsKey(field.getOriginalFieldName())) {
                        Field existsField = existsFieldMap.get(field.getOriginalFieldName());
                        field.setId(existsField.getId());

                        boolean isManual = Field.SOURCE_MANUAL.equals(existsField.getSource());
                        if (isManual) {
                            field.setDataType(existsField.getDataType());
                            field.setPrecision(existsField.getPrecision());
                            field.setScale(existsField.getScale());
                        }
                    }
                    if (StringUtils.isBlank(field.getId())) {
                        field.setId(new ObjectId().toHexString());
                    }

                    // Make sure the target model fields do not have the same name
                    if (!fields.containsKey(field.getFieldName())) {
                        fields.put(field.getFieldName(), field);
                    }
                });

                // 未设置 rollback 时，默认保留用户配置过的字段
                // rollback = 'all' or (rollback = 'table' and rollbackTable = metadataInstancesDto.getOriginalName()) , 回滚当前表
                String rollback = options != null ? options.getRollback() : null;
                String rollbackTable = options != null ? options.getRollbackTable() : null;
                String fieldsNameTransform = options != null ? options.getFieldsNameTransform() : null;
                if ("all".equalsIgnoreCase(rollback) ||
                        (TABLE.equalsIgnoreCase(rollback) &&
                                metadataInstancesDto.getOriginalName().equalsIgnoreCase(rollbackTable))) {

                    metadataInstancesDto.getFields().forEach(field -> {
                        if (existsFieldMap.containsKey(field.getOriginalFieldName())) {
                            Field existsField = existsFieldMap.get(field.getOriginalFieldName());
                            /*if ("manual".equalsIgnoreCase(existsField.getSource())
                                    && existsField.getIsAutoAllowed() != null && !existsField.getIsAutoAllowed()) {*/
                            String transformDataType = field.getDataType();
                                BeanUtils.copyProperties(existsField, field);
                                if (TABLE.equalsIgnoreCase(rollback) && StringUtils.isNotBlank(fieldsNameTransform)) {
                                    if ("toUpperCase".equalsIgnoreCase(fieldsNameTransform)) {
                                        field.setFieldName(field.getOriginalFieldName().toUpperCase());
                                    } else if("toLowerCase".equalsIgnoreCase(fieldsNameTransform)) {
                                        field.setFieldName(field.getOriginalFieldName().toLowerCase());
                                    } else {
                                        field.setFieldName(field.getOriginalFieldName());
                                    }
                                } else {
                                    field.setFieldName(field.getOriginalFieldName());
                                }
                                field.setJavaType(field.getOriginalJavaType());
                                field.setPrecision(field.getOriPrecision());
                                field.setDataType(transformDataType); //field.getOriginalDataType());
                                field.setDeleted(false);
                            //}
                            field.setId(existsField.getId());
                        }
                    });
                }

                Update update = new Update();
                update.set("version", newVersion);
                ArrayList<MetadataInstancesDto> hisModels = new ArrayList<>();
                hisModels.add(historyModel);
                BasicDBObject basicDBObject = new BasicDBObject("$each", hisModels);
                basicDBObject.append("$slice", -5);
                update.push(HISTORIES, basicDBObject);
                update.set(FIELDS, metadataInstancesDto.getFields());
                update.set("indices", metadataInstancesDto.getIndices());
                update.set(IS_DELETED, false);
                update.set("createSource", metadataInstancesDto.getCreateSource());
                update.set(ORIGINAL_NAME, metadataInstancesDto.getOriginalName());
                update.set("name", metadataInstancesDto.getName());

                Query where = Query.query(Criteria.where("id").is(existsMetadataInstance.getId()));
                repository.applyUserDetail(where, userDetail);
                repository.beforeUpsert(update, userDetail);
                bulkOperations.updateOne(where, update);
            } else { // 直接写入

                MetadataInstancesDto _metadataInstancesDto = MetaDataBuilderUtils.build(
                        metadataInstancesDto.getMetaType(), dataSourceConnectionDto, userDetail.getUserId(), userDetail.getUsername(),
                        metadataInstancesDto.getOriginalName(),
                        metadataInstancesDto, null, metadataInstancesDto.getDatabaseId(), "job_analyze",
                        null);

                MetadataInstancesEntity metadataInstance = convertToEntity(MetadataInstancesEntity.class, _metadataInstancesDto);


                //这个操作有可能是插入操作，所以需要校验字段是否又id，如果没有就set id进去
                beforeSave(metadataInstancesDto, userDetail);
                if ("vika".equals(dataSourceConnectionDto.getDatabase_type())) {
                    metadataInstance.setFields(null);
                    metadataInstance.setMetaType(MetaType.VikaDatasheet.name());
                } else if ("qingflow".equals(dataSourceConnectionDto.getDatabase_type())) {
                    metadataInstance.setFields(null);
                    metadataInstance.setMetaType(MetaType.qingFlowApp.name());
                }
                Update update = repository.buildUpdateSet(metadataInstance, userDetail);
                Query where = Query.query(Criteria.where(QUALIFIED_NAME).is(metadataInstance.getQualifiedName()));
                repository.applyUserDetail(where, userDetail);
                repository.beforeUpsert(update, userDetail);
                bulkOperations.upsert(where, update);
            }
        });
        BulkWriteResult result = bulkOperations.execute();
        return result.getModifiedCount();
    }



    public int bulkSave(List<MetadataInstancesDto> insertMetaDataDtos,
                        Map<String, MetadataInstancesDto> updateMetaMap, UserDetail userDetail, boolean saveHistory, String taskId, String uuid) {

        BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);

        boolean write = false;

        if (null == insertMetaDataDtos) {
            insertMetaDataDtos = new ArrayList<>();
        }

        List<String> qualifiedNames = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(insertMetaDataDtos)) {
            checkSetLastUpdate(insertMetaDataDtos,userDetail);

            List<MetadataInstancesDto> sourceMetas = new ArrayList<>();
            if (saveHistory) {
                for (MetadataInstancesDto insertMetaDataDto : insertMetaDataDtos) {
                    String qualifiedName = insertMetaDataDto.getQualifiedName();
                    if (qualifiedName.contains(taskId)) {
                        int i = qualifiedName.lastIndexOf("_");
                        MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
                        BeanUtils.copyProperties(insertMetaDataDto, metadataInstancesDto);
                        String oldQualifiedName = qualifiedName.substring(0, i);
                        metadataInstancesDto.setQualifiedName(oldQualifiedName);
                        metadataInstancesDto.setSourceType(com.tapdata.tm.commons.schema.bean.SourceTypeEnum.SOURCE.name());
                        metadataInstancesDto.setCreateSource("auto");
                        metadataInstancesDto.setTaskId(null);
                        metadataInstancesDto.setId(null);
                        sourceMetas.add(metadataInstancesDto);
                        //qualifiedNames.add(oldQualifiedName);
                    }
                }
            }

            insertMetaDataDtos.addAll(sourceMetas);


            //动态新增表做的兼容处理
            String insertUuid = uuid;
            if (saveHistory) {
                Query query = new Query(Criteria.where(TASK_ID).is(taskId)
                        .and(IS_DELETED).ne(true)
                        .and(TRANSFORM_UUID).exists(true));
                query.fields().include(TRANSFORM_UUID);
                MetadataInstancesDto one = findOne(query);
                insertUuid = one.getTransformUuid();
            }

            for (MetadataInstancesDto metadataInstancesDto : insertMetaDataDtos) {
                metadataInstancesDto.setTransformUuid(insertUuid);
                MetadataInstancesEntity metadataInstance = convertToEntity(MetadataInstancesEntity.class, metadataInstancesDto);


                //这个操作有可能是插入操作，所以需要校验字段是否又id，如果没有就set id进去
                beforeSave(metadataInstancesDto, userDetail);
                Update update = repository.buildUpdateSet(metadataInstance, userDetail);
                Query where = Query.query(Criteria.where(QUALIFIED_NAME).is(metadataInstance.getQualifiedName()));
                repository.applyUserDetail(where, userDetail);
                repository.beforeUpsert(update, userDetail);
                bulkOperations.upsert(where, update);
                write = true;
            }
        }


        if (updateMetaMap != null) {
            List<String> findQualifiedNames = new ArrayList<>();
            Map<String, MetadataInstancesDto> metaMap = new HashMap<>();
            for (MetadataInstancesDto value : updateMetaMap.values()) {
                findQualifiedNames.add(value.getQualifiedName());
            }
            Criteria criteria = Criteria.where(QUALIFIED_NAME).in(findQualifiedNames);
            Query query = new Query(criteria);
            query.fields().exclude(HISTORIES);
            List<MetadataInstancesDto> metadataInstancesDtos = findAllDto(query, userDetail);
            metaMap = metadataInstancesDtos.stream().collect(Collectors.toMap(m -> m.getId().toHexString(), m -> m));

            for (Map.Entry<String, MetadataInstancesDto> entry : updateMetaMap.entrySet()) {
                MetadataInstancesDto value = entry.getValue();

                value.setHistories(null);
                value.setSource(null);
                value.setId(null);


                if (StringUtils.isNotBlank(uuid) && !saveHistory) {
                    value.setTransformUuid(uuid);
                }
                MetadataInstancesEntity entity = convertToEntity(MetadataInstancesEntity.class, value);
                Update update = repository.buildUpdateSet(entity, userDetail);


                if (saveHistory) {
                    //保存历史，用于自动ddl
                    MetadataInstancesDto metadataInstancesDto = metaMap.get(entry.getKey());
                    if (metadataInstancesDto != null) {
                        metadataInstancesDto.setFields(value.getFields());
                        metadataInstancesDto.setIndexes(value.getIndexes());
                        metadataInstancesDto.setIndices(value.getIndices());
                        metadataInstancesDto.setDeleted(false);
                        metadataInstancesDto.setCreateSource(value.getCreateSource());
                        metadataInstancesDto.setVersion(value.getVersion());
                        metadataInstancesDto.setHistories(null);
                        insertMetaDataDtos.add(metadataInstancesDto);
                    }
                }

                Query where = Query.query(Criteria.where(QUALIFIED_NAME).is(value.getQualifiedName()));

                bulkOperations.updateOne(where, update);
                write = true;
                if (saveHistory) {
                    String qualifiedName = value.getQualifiedName();
                    int i = qualifiedName.lastIndexOf("_");
                    String oldQualifiedName = qualifiedName.substring(0, i);
                    value.setQualifiedName(oldQualifiedName);
                    qualifiedNames.add(oldQualifiedName);
                    value.setTaskId(null);

                    value.setCreateSource(null);
                    value.setSourceType(null);

                    MetadataInstancesEntity entityOld = convertToEntity(MetadataInstancesEntity.class, value);
                    Update updateOld = repository.buildUpdateSet(entityOld, userDetail);
                    Query whereOld = Query.query(Criteria.where(QUALIFIED_NAME).is(value.getQualifiedName()));

                    bulkOperations.updateOne(whereOld, updateOld);
                }
            }
        }

        if (write) {
            BulkWriteResult result = bulkOperations.execute();

            //保存历史版本
            if (saveHistory && CollectionUtils.isNotEmpty(insertMetaDataDtos)) {
                metaDataHistoryService.saveHistory(insertMetaDataDtos, taskId);
            }

            if (saveHistory) {
                qualifiedNameLinkLogic(qualifiedNames, userDetail, taskId);
            }

            if (StringUtils.isNotBlank(uuid) && !saveHistory) {
                Criteria deleteOldMetadata = Criteria.where(TASK_ID).is(taskId)
                        .and(TRANSFORM_UUID).ne(uuid);
                deleteAll(new Query(deleteOldMetadata), userDetail);
            }
            return result.getModifiedCount();
        } else {
            return 0;
        }
    }

    public Pair<Integer, Integer> bulkUpsetByWhere(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user) {


        BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
        int modifyCount = 0;
        int insertCount = 0;
        int num = 0;
        for (MetadataInstancesDto dto : metadataInstancesDtos) {
            num++;


            Criteria criteria = Criteria.where(QUALIFIED_NAME).is(dto.getQualifiedName());
            dto.setId(null);
            Query query = new Query(criteria);
            beforeSave(dto, user);
            repository.applyUserDetail(query, user);
            Update update = repository.buildUpdateSet(convertToEntity(MetadataInstancesEntity.class, dto), user);
            repository.beforeUpsert(update, user);

            //这个操作有可能是插入操作，所以需要校验字段是否又id，如果没有就set id进去
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
        Criteria criteria = Criteria.where(SOURCE_ID).is(connectId)
                .and(SOURCE_TYPE).is(sourceType)
                .and(IS_DELETED).ne(true)
                .and(TASK_ID).exists(false)
                .and(META_TYPE).in(MetaType.collection.name(), MetaType.table.name());
        Query query = new Query(criteria);
        query.fields().include(ORIGINAL_NAME);
        List<MetadataInstancesEntity> list = mongoTemplate.find(query, MetadataInstancesEntity.class);
        return list.stream().map(MetadataInstancesEntity::getOriginalName).collect(Collectors.toList());
    }


    public List<Map<String, String>> tableValues(String connectId, String sourceType) {
        Criteria criteria = Criteria.where(SOURCE_ID).is(connectId)
                .and(SOURCE_TYPE).is(sourceType)
                .and(IS_DELETED).ne(true)
                .and(TASK_ID).exists(false)
                .and(META_TYPE).in(MetaType.collection.name(), MetaType.table.name());
        Query query = new Query(criteria);
        query.fields().include(ORIGINAL_NAME,COMMENT);
        List<MetadataInstancesEntity> list = mongoTemplate.find(query, MetadataInstancesEntity.class);
        List<Map<String, String>> values = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(list)) {
            for (MetadataInstancesEntity entity : list) {
                Map<String, String> value = new HashMap<>();
                value.put(TABLE_NAME, entity.getOriginalName());
                value.put(TABLE_ID, entity.getId().toHexString());
                value.put(TABLE_COMMENT,StringUtils.isNotBlank(entity.getComment()) ? entity.getComment():"");
                values.add(value);
            }
        }
        return values;
    }

    protected void setPartitionFilterIfNeed(Criteria criteria, Boolean syncPartitionTableEnable) {
        Optional.ofNullable(syncPartitionTableEnable).ifPresent(syncMaster ->
                criteria.orOperator(
                        Criteria.where(PARTITION_MASTER_TABLE_ID).exists(false),
                        Criteria.where(PARTITION_MASTER_TABLE_ID).is(null),
                        Criteria.where("$expr").is(new Document(syncMaster ? "$eq" : "$ne", Arrays.asList("$partitionMasterTableId", "$name")))
                )
        );
    }

    public Page<Map<String, Object>> pageTables(String connectId, String sourceType, String regex, int skip, int limit, Boolean syncPartitionTableEnable) {
			Criteria criteria = Criteria.where(SOURCE_ID).is(connectId)
				.and(SOURCE_TYPE).is(sourceType)
				.and(IS_DELETED).ne(true)
				.and(TASK_ID).exists(false)
				.and(META_TYPE).in(MetaType.collection.name(), MetaType.table.name());

			if (null != regex) {
				regex = "^" + regex + "$";
				criteria.and(ORIGINAL_NAME).regex(regex);
			}

            setPartitionFilterIfNeed(criteria, syncPartitionTableEnable);
            Aggregation aggregation = Aggregation.newAggregation(
                    Aggregation.match(criteria),
                    Aggregation.unwind(FIELDS),
                    Aggregation.unwind("indices", true),
                    Aggregation.project()
                            .and(AggregationExpression.from(MongoExpression.create("{ \"$toString\": \"$_id\" }"))).as("_id")
                            .and(ORIGINAL_NAME).as(TABLE_NAME)
                            .and(COMMENT).as(TABLE_COMMENT)
                            .and(ConditionalOperators
                                    .when(ComparisonOperators.Eq.valueOf("fields.primaryKey").equalToValue(true))
                                    .then(1)
                                    .otherwise(0)
                            ).as("primaryKey")
                            .and(ConditionalOperators
                                    .when(ComparisonOperators.Eq.valueOf("indices.unique").equalToValue(true))
                                    .then(1)
                                    .otherwise(0)
                            ).as("uniqueIndex"),
                    Aggregation.group("_id")
                            .first(TABLE_NAME).as(TABLE_NAME)
                            .first(TABLE_COMMENT).as(TABLE_COMMENT)
                            .sum("primaryKey").as("primaryKeyCounts")
                            .sum("uniqueIndex").as("uniqueIndexCounts"),
                    Aggregation.project()
                            .and("_id").as(TABLE_ID)
                            .andInclude(TABLE_NAME, TABLE_COMMENT, "primaryKeyCounts", "uniqueIndexCounts")
                            .andExclude("_id"),
                    Aggregation.sort(Sort.by(TABLE_ID))
            );

			long totals;
			List<Map<String, Object>> values = new ArrayList<>();
			if (limit > 0) {
				Query query = new Query(criteria);
				query.fields().include(ORIGINAL_NAME, COMMENT);
				totals = mongoTemplate.count(query, MetadataInstancesEntity.class);
				if (totals > 0) {
					aggregation.getPipeline().add(Aggregation.skip((long) skip)).add(Aggregation.limit(limit));
					AggregationResults<Map> aggregate = mongoTemplate.aggregate(aggregation, METADATA_INSTANCES, Map.class);
					for (Map m : aggregate.getMappedResults()) {
						values.add(m);
					}
				} else {
					values = new ArrayList<>();
				}
			} else {
				AggregationResults<Map> aggregate = mongoTemplate.aggregate(aggregation, METADATA_INSTANCES, Map.class);
				totals = aggregate.getMappedResults().size();
				for (Map m : aggregate.getMappedResults()) {
					values.add(m);
				}
			}

			return new Page<>(totals, values);
		}

    public TableSupportInspectVo tableSupportInspect(String connectId, String tableName) {
        TableSupportInspectVo tableSupportInspectVo = new TableSupportInspectVo();
        Criteria criteria = Criteria.where(SOURCE_ID).is(connectId)
                .and(IS_DELETED).is(false)
                .and(ORIGINAL_NAME).is(tableName)
                .and(META_TYPE).in(MetaType.collection.name(), MetaType.table.name());
        Query query = new Query(criteria);
        query.fields().include(FIELDS);
        MetadataInstancesDto metadataInstancesDtos = findOne(query);
        List<Field> fieldList = metadataInstancesDtos.getFields() == null ? new ArrayList<>() : metadataInstancesDtos.getFields();
        Optional primaryKeyPosition = fieldList.stream().filter(field -> field.getPrimaryKeyPosition() > 0).findAny();
        tableSupportInspectVo.setTableName(tableName);
        tableSupportInspectVo.setSupportInspect(primaryKeyPosition.isPresent());
        return tableSupportInspectVo;
    }

    public List<TableSupportInspectVo> tablesSupportInspect(TablesSupportInspectParam tablesSupportInspectParam) {
        List<TableSupportInspectVo> tableSupportInspectVoList = new ArrayList<>();
        Criteria criteria = Criteria.where(SOURCE_ID).is(tablesSupportInspectParam.getConnectionId())
                .and(IS_DELETED).is(false)
                .and(ORIGINAL_NAME).in(tablesSupportInspectParam.getTableNames())
                .and(META_TYPE).in(MetaType.collection.name(), MetaType.table.name());
        Query query = new Query(criteria);
        query.fields().include(FIELDS, ORIGINAL_NAME);
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
        Criteria criteria = Criteria.where(QUALIFIED_NAME).is(qualifiedName);

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
        Criteria criteria = Criteria.where(QUALIFIED_NAME).is(qualifiedName);

        MetadataInstancesDto metedata = findOne(new Query(criteria));

        if (metedata != null) {
            return PdkSchemaConvert.toPdk(metedata);
        }
        return null;
    }

    public Map<String, List<TapTableDto>> getMetadataV3(Map<String, FindMetadataDto> params, UserDetail user) {
        Map<String, List<TapTableDto>> result = new HashMap<>();
        for (Map.Entry<String, FindMetadataDto> entry : params.entrySet()) {
            String connectionId = entry.getKey();
            FindMetadataDto findMetadataDto = entry.getValue();
            List<TapTableDto> tapTableDtoList = new ArrayList<>();
            result.put(connectionId, tapTableDtoList);
            DataSourceConnectionDto connectionDto = dataSourceService.findById(toObjectId(connectionId), user);
            if (connectionDto == null) {
                continue;
            }
            DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(connectionDto.getDatabase_type(), user);

            connectionDto.setDefinitionGroup(definitionDto.getGroup());
            connectionDto.setDefinitionPdkId(definitionDto.getPdkId());
            connectionDto.setDefinitionScope(definitionDto.getScope());
            connectionDto.setDefinitionVersion(definitionDto.getVersion());
            String metaType = findMetadataDto.getMetaType();
            List<String> tableNames = findMetadataDto.getTableNames();
            List<String> qualifiedNames = new ArrayList<>();
            tableNames.forEach(tableName -> qualifiedNames.add(MetaDataBuilderUtils.generateQualifiedName(metaType, connectionDto, tableName)));
            Criteria criteria = Criteria.where(QUALIFIED_NAME).in(qualifiedNames);
            List<MetadataInstancesDto> metadataInstancesDtoList = findAll(Query.query(criteria));
            if (null == metadataInstancesDtoList) {
                continue;
            }
            for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtoList) {
                tapTableDtoList.add(new TapTableDto(metadataInstancesDto.getQualifiedName(), PdkSchemaConvert.toPdk(metadataInstancesDto)));
            }
        }
        return result;
    }

    public List<Table> findOldByNodeId(Filter filter, UserDetail user) {
        Where where = filter.getWhere();
        if (where == null) {
            return null;
        }
        String nodeId = (String) where.get(NODE_ID);
        List<MetadataInstancesDto> metadatas = findByNodeId(nodeId, null, user, null);
        if (CollectionUtils.isNotEmpty(metadatas)) {
            return metadatas.stream().map(this::getOldSchema).collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    public Map<String, String> findTableMapByNodeId(Filter filter) {
        Where where = filter.getWhere();
        if (where == null) {
            return null;
        }
        String nodeId = (String) where.get(NODE_ID);
        return findKVByNode(nodeId);
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


    public Map<String, String> findKVByNode(String nodeId) {
        Criteria criteria = Criteria.where(DAG_NODES_ID).is(nodeId);
        Query query = new Query(criteria);
        TaskDto taskDto = taskService.findOne(query);

        UserDetail userDetail = userService.loadUserById(new ObjectId(taskDto.getUserId()));

        Map<String, String> kv = new HashMap<>();
        if (taskDto != null && taskDto.getDag() != null) {
            DAG dag = taskDto.getDag();
            Node node = dag.getNode(nodeId);
            kv = getNodeMapping(userDetail, taskDto, kv, node);
        }
        return kv;
    }

    protected Map<String, String> getNodeMapping(UserDetail user, TaskDto taskDto, Map<String, String> kv, Node node) {
        if (node instanceof ProcessorNode) {
            kv.put(node.getId(), getQualifiedNameByNodeId(node, user, null, null, taskDto.getId().toHexString()));
            if (node instanceof MergeTableNode) {
                List<Node> predecessors = taskDto.getDag().predecessors(node.getId());
                for (Node predecessor : predecessors) {
                    getNodeMapping(user, taskDto, kv, predecessor);
                }
            }
        } else if (node instanceof TableNode) {
            kv.put(((TableNode) node).getTableName(), getQualifiedNameByNodeId(node, user, null, null, taskDto.getId().toHexString()));
        } else {
			boolean need2Parse = true;
			if (node instanceof LogCollectorNode) {
				LogCollectorNode logCollectorNode = (LogCollectorNode) node;
				Map<String, LogCollecotrConnConfig> connConfigs = logCollectorNode.getLogCollectorConnConfigs();
				if (null != connConfigs && !connConfigs.isEmpty()) {
					need2Parse = false;
					List<MetadataInstancesDto> metadatas = findByNodeId(node.getId(), Lists.of(ORIGINAL_NAME, QUALIFIED_NAME, SOURCE_ID), user, taskDto);
					if (CollectionUtils.isNotEmpty(metadatas)) {
						Map<String, String> connectionNamespace = new HashMap<>();
						BiFunction<String, String, String> keyParser = (connectionId, originalName) -> {
							String prefix = connectionNamespace.computeIfAbsent(connectionId, cid -> {
								com.tapdata.tm.base.dto.Field fields = new com.tapdata.tm.base.dto.Field();
								fields.put("namespace", true);
								DataSourceConnectionDto connectionDto = dataSourceService.findById(new ObjectId(cid), fields);
								if (null != connectionDto && null != connectionDto.getNamespace() && !connectionDto.getNamespace().isEmpty()) {
									return String.join(".", connectionDto.getNamespace());
								}
								throw new RuntimeException("Connection '" + cid + "' not found 'namespace' property");
							});
							return String.join(".", prefix, originalName);
						};

						kv = metadatas.stream().collect(Collectors.toMap(m -> {
							SourceDto source = m.getSource();
							if (null == source)
								throw new RuntimeException("MetadataInstances '" + m.getQualifiedName() + "' not found 'source' property");
							return keyParser.apply(m.getSource().get_id(), m.getOriginalName());
						}, MetadataInstancesDto::getQualifiedName, (m1, m2) -> m1));
					}
				}
			}

			if (need2Parse) {
				List<MetadataInstancesDto> metadatas = findByNodeId(node.getId(), Lists.of(ORIGINAL_NAME, QUALIFIED_NAME), user, taskDto);
				if (CollectionUtils.isNotEmpty(metadatas)) {
					kv = metadatas.stream()
						.collect(Collectors.toMap(MetadataInstancesDto::getOriginalName
							, MetadataInstancesDto::getQualifiedName, (m1, m2) -> m1));
				}
			}
        }
        return kv;
    }

    public String findHeartbeatQualifiedNameByNodeId(Filter filter, UserDetail user) {
        Where where = filter.getWhere();
        if (where == null) {
            return null;
        }

        final String nodeId = (String) where.get(NODE_ID);
        AtomicReference<String> taskId = new AtomicReference<>();
        return Optional.ofNullable(nodeId).map(nid -> {
            // find running task
            Query query = new Query(Criteria.where(DAG_NODES_ID).is(nid));
            query.fields().include("_id");
            return taskService.findOne(query, user);
        }).map(task -> task.getId().toHexString()).map(tid -> {
            // get heartbeat task dag of the connection node
            Query query = new Query(Criteria.where(ConnHeartbeatUtils.TASK_RELATION_FIELD).is(tid));
            query.fields().include("_id", "dag");
            return taskService.findOne(query, user);
        }).map(taskDto -> {
            taskId.set(taskDto.getId().toHexString());
            return taskDto.getDag();
        }).map(DAG::getTargets).map(targets -> {
            // if target size is not only one to be fix the logic
            if (targets.size() == 1) {
                return targets.get(0);
            }
            return null;
        }).map(node -> {
            if (node instanceof DataNode) {
                DataNode dataNode = (DataNode) node;
                String connectionId = dataNode.getConnectionId();
                DataSourceConnectionDto dataSource = dataSourceService.findById(MongoUtils.toObjectId(connectionId));
                return MetaDataBuilderUtils.generatePdkQualifiedName(dataNode.getType(), connectionId, ConnHeartbeatUtils.TABLE_NAME, dataSource.getDefinitionPdkId(), dataSource.getDefinitionGroup(), dataSource.getDefinitionVersion(), taskId.get());
            }
            return null;
        }).orElse(null);
    }

    public String getQualifiedNameByNodeId(Node node, UserDetail user, DataSourceConnectionDto dataSource, DataSourceDefinitionDto definitionDto, String taskId) {
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
            String metaType = TABLE;
            if (MONGODB.equals(dataSource.getDatabase_type())) {
                metaType = COLLECTION;
            }

            String tableName = ((TableNode) node).transformTableName(((TableNode) node).getTableName());
            return MetaDataBuilderUtils.generateQualifiedName(metaType, dataSource, tableName, taskId);
        } else if (node instanceof ProcessorNode) {
            return MetaDataBuilderUtils.generateQualifiedName(com.tapdata.tm.commons.util.MetaType.processor_node.name(), node.getId(), null, taskId);
        }
        return null;
    }

    public List<String> findDatabaseNodeQualifiedName(String nodeId, UserDetail user, TaskDto taskDto, DataSourceConnectionDto dataSource, DataSourceDefinitionDto definitionDto, List<String> includes) {
        if (taskDto == null || taskDto.getDag() == null) {
            Criteria criteria = Criteria.where(DAG_NODES_ID).is(nodeId);
            Query query = new Query(criteria);
            query.fields().include("dag");
            taskDto = taskService.findOne(query, user);
        }

        String taskId = taskDto.getId().toHexString();

        List<String> qualifiedNames = new ArrayList<>();
        DAG dag = taskDto.getDag();
        if (taskDto.getDag() != null) {
            Node node = dag.getNode(nodeId);
            if (node != null) {

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
                String metaType = TABLE;
                if (MONGODB.equals(dataSource.getDatabase_type())) {
                    metaType = COLLECTION;
                }

                List<String> tableNames;
                if (CollectionUtils.isNotEmpty(includes)) {
                    tableNames = includes;
                } else {

                    DatabaseNode tableNode = (DatabaseNode) node;
                    if (dag.getSources().contains(tableNode)) {
                        tableNames = tableNode.getTableNames();
                    } else if (dag.getTargets().contains(tableNode)) {
                        tableNames = tableNode.getSyncObjects().get(0).getObjectNames();
                    } else {
                        throw new BizException("table node is error nodeId:" + tableNode.getId());
                    }
                }

                if(CollectionUtils.isNotEmpty(tableNames)) {
                    for (String tableName : tableNames) {
                        String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(metaType, dataSource, tableName, taskId);
                        qualifiedNames.add(qualifiedName);
                    }
                }
            }

        }

        return qualifiedNames;
    }

    public List<MetadataInstancesDto> findByNodeId(String nodeId, UserDetail userDetail) {
        Criteria criteria = Criteria
                .where(IS_DELETED).ne(true)
                .and(NODE_ID).is(nodeId);

        return findAllDto(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesDto> findByNodeId(String nodeId, UserDetail userDetail, String taskId, String... fields) {
        Criteria criteria = Criteria
                .where(IS_DELETED).ne(true)
                .and(NODE_ID).is(nodeId)
                .and(TASK_ID).is(taskId);

        return findAllDto(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesDto> findByTaskId(String taskId, UserDetail userDetail) {
        Criteria criteria = Criteria
                .where(IS_DELETED).ne(true)
                .and(TASK_ID).is(taskId);

        return findAllDto(Query.query(criteria), userDetail);
    }

    public List<MetadataInstancesDto> findByNodeId(String nodeId, List<String> fields, UserDetail user, TaskDto taskDto) {
        Page<MetadataInstancesDto> page = findByNodeId(nodeId, fields, user, taskDto, null, null, 1, 0);
        return page.getItems();
    }

    /**
     * Retrieves a map of metadata instances grouped by node IDs.
     *
     * @param  nodeIds    a list of node IDs to retrieve metadata instances for
     * @param  fields     a list of fields to include in the metadata instances
     * @param  user       the user detail
     * @param  taskDto    the task DTO
     * @return            a map of metadata instances grouped by node IDs
     */
    public Map<String, List<MetadataInstancesDto>> findByNodeIds(List<String> nodeIds, List<String> fields, UserDetail user, TaskDto taskDto) {
        Map<String, List<MetadataInstancesDto>> result = new HashMap<>();

        for (String nodeId : nodeIds) {
            Page<MetadataInstancesDto> page = findByNodeId(nodeId, fields, user, taskDto, null, null, 1, 0);
            List<MetadataInstancesDto> list = page.getItems();

            if (CollectionUtils.isNotEmpty(list)) {
                for (MetadataInstancesDto metadataInstancesDto : list) {
                    ////页面显示排序问题处理
                    MetadataInstancesDto.sortField(metadataInstancesDto.getFields());
                }
            }

            result.put(nodeId, list);
        }

        return result;
    }

    public Page<MetadataInstancesDto> findByNodeId(String nodeId, List<String> fields, UserDetail user, TaskDto taskDto, String tableFilter, String filterType, int page, int pageSize) {
			  user.setFreeAuth();
        if (taskDto == null || taskDto.getDag() == null) {
            Criteria criteria = Criteria.where(DAG_NODES_ID).is(nodeId);
            Query query = new Query(criteria);
            query.fields().include("dag");
            taskDto = taskService.findOne(query, user);
        }

        if (taskDto == null) {
            throw new BizException("Task.nodeRefresh");
        }

        String taskId = taskDto.getId().toHexString();

        long totals = 0;
        List<MetadataInstancesDto> metadatas = new ArrayList<>();
        DAG dag = taskDto.getDag();
        if (taskDto.getDag() != null) {
            Node node = dag.getNode(nodeId);
            if (node != null) {
                Criteria criteriaTable = Criteria.where(META_TYPE).in(TABLE, COLLECTION, "view");
                Criteria criteriaNode = Criteria.where(META_TYPE).is(MetaType.processor_node.name());
                Query queryMetadata = new Query();
                if (pageSize > 0) {
                    queryMetadata.skip((long) (Math.max(1, page) - 1) * pageSize);
                    queryMetadata.limit(pageSize);
                }
                if (CollectionUtils.isNotEmpty(fields)) {
                    String[] fieldArrays = fields.toArray(new String[0]);
                    queryMetadata.fields().include(fieldArrays);
                }
                if (node instanceof MigrateProcessorNode) {
                    Criteria criteria = Criteria.where(NODE_ID).is(nodeId).and(IS_DELETED).ne(true);
                    if (StringUtils.isNotBlank(tableFilter)) {
                        Pattern pattern = Pattern.compile(tableFilter, Pattern.CASE_INSENSITIVE);
                        criteria.and(LOWER_CAME_ORIGINAL_NAME).regex(pattern);
                    }
                    Query nodeQuery = new Query(criteria);
                    List<MetadataInstancesDto> all = findAll(nodeQuery);
                    Map<String, MetadataInstancesDto> currentMap = all.stream()
                            .collect(Collectors.toMap(MetadataInstancesDto::getOriginalName
                                    , s->s, (m1, m2) -> m1));
                    // todo 下面逻辑可能会出现问题，相当于copy一份原表名的模型，数据上存在"错误"，加入表A改名B 表B改名A
                    if (isAgentReq()) {
                        if (node instanceof TableRenameProcessNode) {
                            LinkedHashSet<TableRenameTableInfo> tableNames = ((TableRenameProcessNode) node).getTableNames();
                            if (CollectionUtils.isNotEmpty(tableNames)) {
                                for (TableRenameTableInfo tableName : tableNames) {
                                    MetadataInstancesDto metadataInstancesDto = currentMap.get(tableName.getCurrentTableName());
                                    if (metadataInstancesDto != null) {
                                        MetadataInstancesDto metadataInstancesDto1 = new MetadataInstancesDto();
                                        MetadataInstancesDto metadataInstancesDto2 = new MetadataInstancesDto();
                                        BeanUtils.copyProperties(metadataInstancesDto, metadataInstancesDto1);
                                        BeanUtils.copyProperties(metadataInstancesDto, metadataInstancesDto2);
                                        metadataInstancesDto1.setOriginalName(tableName.getOriginTableName());
                                        metadataInstancesDto2.setOriginalName(tableName.getPreviousTableName());
                                        all.add(metadataInstancesDto1);
                                        all.add(metadataInstancesDto2);
                                    }
                                }
                            }
                        }
                    }
                    metadatas.addAll(all);
                } else if (Node.NodeCatalog.processor.equals(node.getCatalog())) {
                    queryMetadata.addCriteria(criteriaNode);
                    String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.processor_node.name(), nodeId, null, taskId);
                    criteriaNode.and(QUALIFIED_NAME).is(qualifiedName).and(IS_DELETED).ne(true);
                    MetadataInstancesDto one = findOne(queryMetadata, user);
                    if (one != null) {
                        metadatas.add(one);
                    }
                } else if (node instanceof TableNode) {
                    queryMetadata.addCriteria(criteriaTable);
                    TableNode tableNode = (TableNode) node;
                    if (StringUtils.isBlank(tableNode.getTableName())) {
                        return new Page<>(0, new ArrayList<>());
                    }
                    criteriaTable.and(SOURCE_ID).is(tableNode.getConnectionId())
                            .and(ORIGINAL_NAME).is(tableNode.getTableName()).and(TASK_ID).is(taskId).and(IS_DELETED).ne(true);
                    MetadataInstancesDto one = findOne(queryMetadata, user);
                    if (one != null) {
                        metadatas.add(one);
                    }
                } else if (node instanceof DatabaseNode) {
                    queryMetadata.addCriteria(criteriaTable);
                    DatabaseNode tableNode = (DatabaseNode) node;
                    List<String> tableNames = Collections.emptyList();
                    if (node.sourceType() == Node.SourceType.source) {
                        tableNames = tableNode.getTableNames();
                    } else if (node.sourceType() == Node.SourceType.target) {
                        if (CollectionUtils.isNotEmpty(tableNode.getSyncObjects())) {
                            tableNames = tableNode.getSyncObjects().get(0).getObjectNames();
                        }
                    } else {
                        throw new BizException("table node is error nodeId:" + tableNode.getId());
                    }

                    if (StringUtils.isNotBlank(filterType)) {
                        if ("updateEx".equals(filterType)) {
                            criteriaTable.and("hasPrimaryKey").is(false)
                                    .and("hasUnionIndex").is(false)
                                    .and("hasUpdateField").is(false);
                        } else if ("transformEx".equals(filterType)) {
                            criteriaTable.and("hasTransformEx").is(true);
                        }
                    }

                    if (CollectionUtils.isEmpty(tableNames)) {
                        metadatas = Lists.newArrayList();
                    } else{
                        criteriaTable.and(NODE_ID).is(nodeId)
                                .and(TASK_ID).is(taskId)
                                .and(IS_DELETED).ne(true);

                        if (StringUtils.isNotBlank(tableFilter)) {
                            Pattern pattern = Pattern.compile(tableFilter, Pattern.CASE_INSENSITIVE);
                            criteriaTable.and(LOWER_CAME_ORIGINAL_NAME).regex(pattern);
                        }

                        metadatas = findAllDto(queryMetadata, user);
                        totals = count(new Query(criteriaTable), user);
                        //totals = tableNames.size();
                    }

                } else if (node instanceof LogCollectorNode) {
                    LogCollectorNode logNode = (LogCollectorNode) node;
					Map<String, LogCollecotrConnConfig> connConfigs = logNode.getLogCollectorConnConfigs();
					if (null != connConfigs && !connConfigs.isEmpty()) {
						List<Criteria> criteriaList = new ArrayList<>();
						for (LogCollecotrConnConfig config : connConfigs.values()) {
							criteriaList.add(Criteria.where(SOURCE_ID).is(config.getConnectionId())
								.and(LOWER_CAME_ORIGINAL_NAME).in(config.getTableNames()));
						}
						criteriaTable.and(IS_DELETED).ne(true).orOperator(criteriaList);
						queryMetadata.addCriteria(criteriaTable);
						metadatas = findAllDto(queryMetadata, user);
						totals = count(queryMetadata, user);
					} else {
						List<String> connectionIds = logNode.getConnectionIds();
						if (CollectionUtils.isNotEmpty(connectionIds)) {
							String connectionId = connectionIds.get(0);
							queryMetadata.addCriteria(criteriaTable);
							criteriaTable.and(SOURCE_ID).is(connectionId)
								.and(LOWER_CAME_ORIGINAL_NAME).in(logNode.getTableNames()).and(IS_DELETED).ne(true);
							metadatas = findAllDto(queryMetadata, user);
							totals = count(queryMetadata, user);
						}
					}
                }
            }

        }
        metadatas = metadatas.stream().filter(Objects::nonNull).collect(Collectors.toList());
        return new Page<>(Math.max(totals, metadatas.size()), metadatas);
    }



    public List<Map<String, Object>> search(String type, String keyword, String lastId, Integer pageSize, UserDetail user) {
        List<String> metaTypes = Lists.newArrayList(TABLE, COLLECTION);
        Criteria criteria;
        if (TABLE.equals(type)) {
            criteria = Criteria.where(META_TYPE).in(metaTypes)
                    .orOperator(Criteria.where(ORIGINAL_NAME).regex(keyword), Criteria.where("name").regex(keyword)
                            , Criteria.where(COMMENT).regex(keyword));
        } else if ("column".equals(type)) {
            criteria = Criteria.where(META_TYPE).in(metaTypes)
                    .orOperator(Criteria.where("fields.field_name").regex(keyword), Criteria.where("fields.alias_name").regex(keyword)
                            /*, Criteria.where("fields.comment").regex(keyword)*/);
        } else {
            throw new BizException(ILLEGAL_ARGUMENT, type);
        }

        if (StringUtils.isNotBlank(lastId)) {
            criteria.and("_id").gt(MongoUtils.toObjectId(lastId));
        }
        criteria.and(IS_DELETED).ne(true);
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
            table.put(ORIGINAL_NAME, item.getOriginalName());
            table.put(COMMENT, item.getComment() == null ? "" : item.getComment());
            obj.put(TABLE, table);

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
                        colObj.put(COMMENT, field.getComment());
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
                Criteria.where(SOURCE_ID).is(connectionId)
                        .and(IS_DELETED).ne(true)
                        .and(META_TYPE).in(MetaType.collection.name(), MetaType.table.name())
                        .orOperator(Criteria.where(ORIGINAL_NAME).regex(keyword), Criteria.where("name").regex(keyword)
                                , Criteria.where(COMMENT).regex(keyword));

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
        Criteria criteria = Criteria.where(SOURCE_ID).is(connectionId)
                .and(IS_DELETED).is(false)
                .and(META_TYPE).in(metaTypes)
                .and(ORIGINAL_NAME).in(names);
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
        MetadataInstancesDto metadataInstancesDto = findById(MongoUtils.toObjectId(id));
        TableListVo tableListVo = BeanUtil.copyProperties(metadataInstancesDto, TableListVo.class);
        return tableListVo;
    }


    public Map<String, MetadataInstancesDto> batchImport(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user, boolean cover, Map<String, DataSourceConnectionDto> conMap) {
        Map<String, MetadataInstancesDto> collect = metadataInstancesDtos.stream().collect(Collectors.toMap(k -> k.getQualifiedName(), v -> v, (k1, k2) -> k1));

        metadataInstancesDtos = new ArrayList<>(collect.values());
        Map<String, MetadataInstancesDto> metaMap = new HashMap<>();
        for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
            String connectionId = null;
            if (metadataInstancesDto.getSource() != null) {
                connectionId = metadataInstancesDto.getSource().get_id();
                if (connectionId == null && metadataInstancesDto.getSource().getId() != null) {
                    connectionId = metadataInstancesDto.getSource().getId().toHexString();
                }
            }

            if (connectionId != null) {
                DataSourceConnectionDto connectionDto = conMap.get(connectionId);
                if (connectionDto != null) {
                    SourceDto sourceDto = new SourceDto();
                    BeanUtils.copyProperties(connectionDto, sourceDto);
                    sourceDto.set_id(connectionDto.getId().toHexString());
                    metadataInstancesDto.setSource(sourceDto);
                }
            }
            MetadataInstancesDto newMeta = null;
            metadataInstancesDto.setListtags(null);
            newMeta = importEntity(metadataInstancesDto, user);
            metaMap.put(newMeta.getId().toHexString(), metadataInstancesDto);
        }
        return metaMap;
    }


    public Page<TapTable> getTapTable(Filter filter, UserDetail loginUser) {

        Page<MetadataInstancesDto> list = list(filter, loginUser);
        Page<TapTable> tapTablePage = new Page<>();
        tapTablePage.setTotal(list.getTotal());
        List<TapTable> tapTables = new ArrayList<>();
        List<MetadataInstancesDto> items = list.getItems();

        items.forEach(FilterMetadataInstanceUtil::filterMetadataInstancesFields);

        if (CollectionUtils.isNotEmpty(items)) {
            tapTables = items.stream().map(PdkSchemaConvert::toPdk).collect(Collectors.toList());
        }

        tapTablePage.setItems(tapTables);
        return tapTablePage;
    }

    public Page<TapTable> getTapTable(DatabaseNode node, UserDetail loginUser) {
        Filter filter = new Filter();
        filter.setWhere(new Where()
                .and(SOURCE_ID_WITHOUT_UNDERLINE, node.getConnectionId())
                .and(META_TYPE, MetaType.table)
                .and(SOURCE_TYPE, SourceTypeEnum.SOURCE)
        );

        DataSourceConnectionDto dataSource = dataSourceService.findById(toObjectId(node.getConnectionId()));
        if ("expression".equals(node.getMigrateTableSelectType())) {
            filter.getWhere().and(ORIGINAL_NAME, new Document("$regex", node.getTableExpression()));
        } else {
            List<String> qualifiedNames = new ArrayList<>();
            for (String tableName : node.getTableNames()) {
                qualifiedNames.add(MetaDataBuilderUtils.generateQualifiedName(MetaType.table.name(), dataSource, tableName));
            }
            filter.getWhere().and(QUALIFIED_NAME, new Document("$in", qualifiedNames));
        }


        return getTapTable(filter, loginUser);
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
                    if (metadataInstancesDto != null) {
                        List<Field> fields = metadataInstancesDto.getFields();
                        parentFields.add(fields);
                    }
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


    public void qualifiedNameLinkLogic(List<String> qualifiedNames, UserDetail user){
        List<MetadataInstancesDto> metadataInstancesDto = findByQualifiedNameNotDelete(qualifiedNames, user);
        linkLogic(metadataInstancesDto, user, null);
    }


    public void qualifiedNameLinkLogic(List<String> qualifiedNames, UserDetail user, String taskId){
        List<MetadataInstancesDto> metadataInstancesDto = findByQualifiedNameNotDelete(qualifiedNames, user);
        linkLogic(metadataInstancesDto, user, taskId);
    }

    //带有taskId的为ddl任务传过来的。所以不需要过滤一些运行状态的任务模型。
    public void linkLogic(List<MetadataInstancesDto> metadataInstancesDtos, UserDetail user, String taskId){
        try {

            for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {

                //查询得到所有的关联的逻辑模型表
                Criteria criteria = Criteria.where(META_TYPE).is(metadataInstancesDto.getMetaType()).and(ORIGINAL_NAME).is(metadataInstancesDto.getOriginalName())
                        .and(SOURCE_ID).is(metadataInstancesDto.getSource().get_id())
                        .and(IS_DELETED).ne(true).and(SOURCE_TYPE).is(SourceTypeEnum.VIRTUAL.name());

                if (StringUtils.isNotBlank(taskId)) {
                    criteria.and(TASK_ID).is(taskId);
                }
                Query query = new Query(criteria);
                List<MetadataInstancesDto> taskMetadatas = findAllDto(query, user);

                if (CollectionUtils.isNotEmpty(taskMetadatas)) {

                    List<ObjectId> taskIds = StringUtils.isNotBlank(taskId) ? Lists.of(MongoUtils.toObjectId(taskId)) :
                            taskMetadatas.stream().map(MetadataInstancesDto::getTaskId).filter(StringUtils::isNotBlank).map(MongoUtils::toObjectId).collect(Collectors.toList());

                    //对于正在运行中的任务的模型，不需要做下面的合并物理表的操作。
                    //下面这个过滤的逻辑，可能存在任务刚好点启动的时候，会被漏掉
                    Criteria criteriaTask = Criteria.where("_id").in(taskIds).and(IS_DELETED).ne(true);
                    if (StringUtils.isBlank(taskId)) {
                        criteriaTask.and("status").in(TaskDto.STATUS_EDIT, TaskDto.STATUS_WAIT_START);
                    }
                    Query queryTask = new Query(criteriaTask);
                    queryTask.fields().include("_id");
                    List<TaskDto> allDto = taskService.findAllDto(queryTask, user);
                    final Set<String> editTaskIds;

                    if (CollectionUtils.isNotEmpty(allDto)) {
                        editTaskIds = allDto.stream().map(t -> t.getId().toHexString()).collect(Collectors.toSet());
                        taskMetadatas = taskMetadatas.stream().filter(t -> editTaskIds.contains(t.getTaskId()) || StringUtils.isBlank(t.getTaskId())).collect(Collectors.toList());
                    } else {
                        continue;
                    }

                    com.tapdata.tm.commons.schema.Schema originalSchema = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(metadataInstancesDto), com.tapdata.tm.commons.schema.Schema.class);


                    if (CollectionUtils.isNotEmpty(taskMetadatas)) {
                        List<MetadataInstancesDto> updateMetadatas = new ArrayList<>();
                        //如果逻辑模型没有为空，则遍历合并物理模型跟逻辑模型，得到新的逻辑模型保存到库里面。
                        for (MetadataInstancesDto taskMetadata : taskMetadatas) {
                            com.tapdata.tm.commons.schema.Schema schema = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(taskMetadata), com.tapdata.tm.commons.schema.Schema.class);
                            schema = SchemaUtils.mergeSchema(Lists.of(SchemaUtils.cloneSchema(originalSchema)), schema, false);
                            MetadataInstancesDto metadataInstancesDto1 = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(schema), MetadataInstancesDto.class);
                            if (metadataInstancesDto1 != null) {
                                metadataInstancesDto1.setQualifiedName(taskMetadata.getQualifiedName());
                                updateMetadatas.add(metadataInstancesDto1);
                                if (updateMetadatas.size() % UPSERT_BATCH_SIZE  == 0) {
                                    bulkUpsetByWhere(updateMetadatas, user);
                                    updateMetadatas.clear();
                                }
                            }
                        }
                        if (CollectionUtils.isNotEmpty(updateMetadatas)) {
                            //批量入库
                            bulkUpsetByWhere(updateMetadatas, user);
                        }
                    }

                }
            }
        } catch (Exception e) {
            log.warn("update logic metadata failed");
        }
    }




    public void deleteTaskMetadata(String taskId, UserDetail user) {
        Criteria criteria = Criteria.where(TASK_ID).is(taskId);

        Query query = new Query(criteria);
        deleteAll(query, user);
    }

    public Map<String, TapType> dataType2TapType(DataType2TapTypeDto dto, UserDetail user) {
        Assert.notNull(dto.getDatabaseType(), "databaseType can not be null");
        Assert.notEmpty(dto.getDataTypes(), "dataTypes can not be null");
        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(dto.getDatabaseType(), user);
        Assert.notNull(definitionDto, "not found DataSourceDefinition of databaseType: " + dto.getDatabaseType());

        String expression = definitionDto.getExpression();
        DefaultExpressionMatchingMap expressionMatchingMap = DefaultExpressionMatchingMap.map(expression);

        LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
        int i = 1;
        for (String dataType : dto.getDataTypes()) {
            fields.put(dataType, new TapField(String.format("f%03d", i++), dataType));
        }

        PdkSchemaConvert.getTableFieldTypesGenerator().autoFill(fields, expressionMatchingMap);

        Map<String, TapType> types = new HashMap<>();
        for (TapField f : fields.values()) {
            types.put(f.getDataType(), f.getTapType());
        }
        return types;
    }

    public boolean checkTableExist(String connectionId, String tableName, UserDetail user) {
        Criteria criteria = Criteria.where(ORIGINAL_NAME).is(tableName)
                .and(IS_DELETED).ne(true)
                .and(SOURCE_ID).is(connectionId)
                .and(SOURCE_TYPE).is(SourceTypeEnum.SOURCE.name())
                .and(TASK_ID).exists(false);
        Query query = new Query(criteria);
        long count = count(query, user);
        return count > 0;
    }

    public void deleteLogicModel(String taskId, String nodeId) {
        Criteria criteria = Criteria.where(TASK_ID).is(taskId).and(NODE_ID).is(nodeId);
        Query query = new Query(criteria);
        deleteAll(query);
    }

    public long countUpdateExNum(String nodeId) {
        Criteria criteria = Criteria
                .where(IS_DELETED).ne(true)
                .and(NODE_ID).is(nodeId)
                .and(SOURCE_TYPE).is(SourceTypeEnum.VIRTUAL)
                .and("hasPrimaryKey").is(false)
                .and("hasUnionIndex").is(false)
                .and("hasUpdateField").is(false);
        return count(Query.query(criteria));
    }

    public long countTransformExNum(String nodeId) {
        Criteria criteria = Criteria
                .where(IS_DELETED).ne(true)
                .and(NODE_ID).is(nodeId)
                .and(SOURCE_TYPE).is(SourceTypeEnum.VIRTUAL)
                .and("hasTransformEx").is(true);
        return count(Query.query(criteria));
    }

    public long countTotalNum(String nodeId) {
        Criteria criteria = Criteria
                .where(IS_DELETED).ne(true)
                .and(NODE_ID).is(nodeId)
                .and(SOURCE_TYPE).is(SourceTypeEnum.VIRTUAL);
        return count(Query.query(criteria));
    }


    public void updateTableDesc(MetadataInstancesDto metadataInstances,UserDetail userDetail){
        if(org.springframework.util.StringUtils.isEmpty(metadataInstances.getId())){
            throw new BizException(ILLEGAL_ARGUMENT, "Id");
        }

        Criteria criteria = Criteria.where("_id").is(metadataInstances.getId());
        Query query = new Query(criteria);
        Update update = Update.update("description",metadataInstances.getDescription());
        update(query,update,userDetail);
    }

    public void updateTableFieldDesc(String id, DiscoveryFieldDto discoveryFieldDto,UserDetail userDetail){
        if(org.springframework.util.StringUtils.isEmpty(id)){
            throw new BizException(ILLEGAL_ARGUMENT, "Id");
        }
        Criteria criteria = Criteria.where("_id").is(MongoUtils.toObjectId(id)).and("fields.id").is(discoveryFieldDto.getId());
        Query query = new Query(criteria);
        Update update = Update.update("fields.$.description",discoveryFieldDto.getBusinessDesc());
        update(query,update, userDetail);
    }

    public MetadataInstancesDto importEntity(MetadataInstancesDto metadataInstancesDto, UserDetail userDetail) {

        Criteria criteria = Criteria.where(QUALIFIED_NAME).is(metadataInstancesDto.getQualifiedName());
        Query query = new Query(criteria);
        upsert(query, metadataInstancesDto, userDetail);
        return metadataInstancesDto;


    }

    public void updateTableCustomDesc(String qualifiedName, String customDesc, UserDetail user) {
        Criteria criteria = Criteria.where(QUALIFIED_NAME).is(qualifiedName);
        update(new Query(criteria), Update.update("customDesc", customDesc), user);
    }

    public void updateFieldCustomDesc(String qualifiedName, Map<String, String> fieldCustomDescMap, UserDetail user) {
        Criteria criteria = Criteria.where(QUALIFIED_NAME).is(qualifiedName);
        Query query = new Query(criteria);
        query.fields().include(FIELDS);
        MetadataInstancesDto dto = findOne(query, user);
        List<Field> fields = dto.getFields();
        if (CollectionUtils.isNotEmpty(fields)) {
            for (Field field : fields) {
                String customDesc = fieldCustomDescMap.get(field.getFieldName());
                field.setDescription(customDesc);
            }
        }
        update(new Query(criteria), Update.update(FIELDS, fields), user);

    }

    public DataTypeCheckMultipleVo dataTypeCheckMultiple(String databaseType, String dataType, UserDetail user) {
        DataTypeCheckMultipleVo dataTypeCheckMultipleVo = new DataTypeCheckMultipleVo();
        dataTypeCheckMultipleVo.setResult(false);
        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.getByDataSourceType(databaseType, user);
        if (definitionDto == null) {
            return dataTypeCheckMultipleVo;
        }

        String expression = definitionDto.getExpression();
        DefaultExpressionMatchingMap map = DefaultExpressionMatchingMap.map(expression);
        TypeExprResult<DataMap> exprResult = map.get(dataType);
        if (exprResult == null) {
            return dataTypeCheckMultipleVo;
        }

        if (exprResult.getParams() == null) {
            return dataTypeCheckMultipleVo;
        }

        Object tapMapping = exprResult.getValue().get("_tapMapping");
        if (tapMapping instanceof TapStringMapping) {
            dataTypeCheckMultipleVo.setResult(true);
        }

        String originType = dataType;
        int i = originType.indexOf("(");
        if (i >= 0) {
            originType = originType.substring(0, i);
        }

        dataTypeCheckMultipleVo.setOriginType(originType);

        return dataTypeCheckMultipleVo;
    }
    public Set<String> getTypeFilter(String nodeId,UserDetail userDetail){
        List<MetadataInstancesDto> metadataInstancesDtos = findByNodeId(nodeId, null, userDetail, null);
        Set<String> set = new HashSet<>();
        metadataInstancesDtos.forEach(metadataInstancesDto -> {
            metadataInstancesDto.getFields().forEach(field -> {
                set.add(RemoveBracketsUtil.removeBrackets(field.getDataType()));
            });
        });
    return set;
    }

    public MetadataInstancesDto multiTransform(MultiPleTransformReq multiPleTransformReq, UserDetail user) {

        MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
        metadataInstancesDto.setFields(multiPleTransformReq.getFields());
        List<Field> fields = metadataInstancesDto.getFields();
        if (CollectionUtils.isEmpty(fields)) {
            return metadataInstancesDto;
        }

        List<FieldChangeRule> rules = multiPleTransformReq.getRules();
        if (CollectionUtils.isEmpty(rules)) {
            return metadataInstancesDto;
        }

        DataSourceDefinitionDto dataSourceDefinitionDto = dataSourceDefinitionService.getByDataSourceType(multiPleTransformReq.getDatabaseType(), user);
        if (dataSourceDefinitionDto == null) {
            return metadataInstancesDto;
        }

        String expression = dataSourceDefinitionDto.getExpression();
        DefaultExpressionMatchingMap map = DefaultExpressionMatchingMap.map(expression);


        FieldChangeRuleGroup fieldChangeRuleGroup = new FieldChangeRuleGroup();
        for (FieldChangeRule rule : rules) {
            fieldChangeRuleGroup.add(multiPleTransformReq.getNodeId(), rule);
        }


        for (Field field : fields) {
            field.setDataType(field.getDataTypeTemp());
            fieldChangeRuleGroup.process(multiPleTransformReq.getNodeId(), multiPleTransformReq.getQualifiedName(), field, map);
        }

        return metadataInstancesDto;

    }

    public Boolean checkMetadataInstancesIndex(String cacheKeys,String id){
        AtomicBoolean result = new AtomicBoolean(false);
        MetadataInstancesDto metadataInstancesDto = findById(MongoUtils.toObjectId(id));
            List<TableIndex> indices = metadataInstancesDto.getIndices();
            if(CollectionUtils.isNotEmpty(indices)){
                indices.forEach(tableIndex -> {
                    if(StringUtils.isNotBlank(tableIndex.getColumns().get(0).getColumnName())){
                        String index = tableIndex.getColumns().stream().map(TableIndexColumn::getColumnName).collect(Collectors.joining(","));
                        if(index.equals(cacheKeys)) result.set(true);
                    }else{
                        String name = tableIndex.getIndexName();
                        name = name.substring(5);
                        Document dIndex = Document.parse(name);
                        if (dIndex == null) {
                            return;
                        }
                        String index = dIndex.get("key",Document.class).keySet().stream().map(Object::toString).collect(Collectors.joining(","));
                        if(index.equals(cacheKeys)) result.set(true);
                    }
                });
            }
        return result.get();
    }

    @Override
    public void checkSetLastUpdate(List<MetadataInstancesDto> insertMetaDataDtos,UserDetail user) {
        if(CollectionUtils.isEmpty(insertMetaDataDtos))return;
        Map<String,List<String>> map = insertMetaDataDtos.stream().filter(metadataInstancesDto -> metadataInstancesDto.getLastUpdate() == null)
                .collect(Collectors.groupingBy(metadataInstancesDto -> metadataInstancesDto.getSource().get_id(),
                        Collectors.mapping(MetadataInstancesDto::getQualifiedName, Collectors.toList())));
        Map<String,Long> lastUpdateMap = new HashMap<>();
        for(Map.Entry<String,List<String>> entry : map.entrySet()){
            Long lastUpdate = findDatabaseMetadataInstanceLastUpdate(entry.getKey(),user);
            if(lastUpdate == null)throw new BizException("lastUpdate is null");
            for(String qualifiedName : entry.getValue()){
                lastUpdateMap.put(qualifiedName,lastUpdate);
            }
        }
        for(MetadataInstancesDto metadataInstancesDto : insertMetaDataDtos){
            if(metadataInstancesDto.getLastUpdate() == null)metadataInstancesDto.setLastUpdate(lastUpdateMap.get(metadataInstancesDto.getQualifiedName()));
        }

    }

    @Override
    public Long findDatabaseMetadataInstanceLastUpdate(String connectionId, UserDetail user) {
        DataSourceConnectionDto connectionDto = dataSourceService.findById(toObjectId(connectionId), user);
        if (connectionDto == null) {
            throw new BizException("connection is null");
        }
        String qualifiedName = MetaDataBuilderUtils.generateQualifiedName(MetaType.database.name(), connectionDto, null);
        Criteria criteria = Criteria.where(QUALIFIED_NAME).is(qualifiedName);
        Query query = new Query(criteria);
        query.fields().include("lastUpdate");
        MetadataInstancesDto metedata = findOne(query);
        if(metedata == null)throw new BizException("metadataInstances is null");
        return metedata.getLastUpdate();
    }
}
