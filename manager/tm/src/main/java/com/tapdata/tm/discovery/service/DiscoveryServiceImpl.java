package com.tapdata.tm.discovery.service;
import com.google.common.collect.Lists;
import com.tapdata.tm.apiServer.dto.ApiServerDto;
import com.tapdata.tm.apiServer.service.ApiServerService;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.task.dto.TaskCollectionObjDto;
import com.tapdata.tm.discovery.bean.DataObjCategoryEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.discovery.bean.DataSourceCategoryEnum;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mongodb.ConnectionString;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.*;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.task.repository.TaskCollectionObjRepository;
import com.tapdata.tm.task.service.TaskCollectionObjService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import io.tapdata.entity.schema.type.*;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class DiscoveryServiceImpl implements DiscoveryService {

    private MetadataInstancesService metadataInstancesService;

    private MetadataInstancesRepository metaDataRepository;
    private TaskCollectionObjRepository taskRepository;
    private TaskCollectionObjService taskService;

    private MetadataDefinitionService metadataDefinitionService;

    private ModulesService modulesService;

    private DataSourceService dataSourceService;


    private ClusterStateService clusterStateService;
    private WorkerService workerService;

    private ApiServerService apiServerService;

    /**
     * 查询对象概览列表
     *
     * @param param
     * @return
     */


//    public Page<DataDiscoveryDto> find1(DiscoveryQueryParam param, UserDetail user) {
//        if (param.getPage() == null) {
//            param.setPage(1);
//        }
//
//        if (param.getPageSize() == null) {
//            param.setPageSize(20);
//        }
//
//        Page<DataDiscoveryDto> page = new Page<>();
//        page.setItems(Lists.newArrayList());
//        page.setTotal(0);
//
//
//        if (StringUtils.isNotBlank(param.getCategory())) {
//            if (!param.getCategory().equals(DataObjCategoryEnum.storage.name())) {
//                return page;
//            }
//        }
//        if (StringUtils.isNotBlank(param.getSourceCategory())) {
//            if (!param.getSourceCategory().equals(DataSourceCategoryEnum.connection.name())) {
//                return page;
//            }
//        }
//
//        Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
//                .and("taskId").exists(false)
//                .and("is_deleted").ne(true);
//        if (StringUtils.isNotBlank(param.getType())) {
//            criteria.and("meta_type").is(param.getType());
//        } else {
//            criteria.and("meta_type").ne("database");
//        }
//        if (StringUtils.isNotBlank(param.getSourceType())) {
//            criteria.and("source.database_type").is(param.getSourceType());
//        }
//
//
//
//        if (StringUtils.isNotBlank(param.getQueryKey())) {
//            String queryKey = param.getQueryKey();
//            queryKey = MongoUtils.replaceLike(queryKey);
//            criteria.orOperator(
//                    Criteria.where("originalName").regex(queryKey,"i"),
//                    Criteria.where("name").regex(queryKey,"i"),
//                    Criteria.where("comment").regex(queryKey,"i"),
//                    Criteria.where("source.name").regex(queryKey,"i"),
//                    Criteria.where("source.name").regex(queryKey,"i"),
//                    Criteria.where("alias_name").regex(queryKey,"i"));
//        }
//
//        if (StringUtils.isNotBlank(param.getTagId())) {
//            List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.newArrayList(MongoUtils.toObjectId(param.getTagId())));
//            List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());
//            criteria.and("listtags.id").in(tagIds);
//        }
//
//
//        Query query = new Query(criteria);
//        query.with(Sort.by(Sort.Direction.DESC, "createTime"));
//
//        long count = metadataInstancesService.count(query, user);
//
//        query.skip((long) (param.getPage() - 1) * param.getPageSize());
//        query.limit(param.getPageSize());
//        List<MetadataInstancesDto> allDto = metadataInstancesService.findAllDto(query, user);
//
//        List<DataDiscoveryDto> items = new ArrayList<>();
//        for (MetadataInstancesDto metadataInstancesDto : allDto) {
//            DataDiscoveryDto dto = new DataDiscoveryDto();
//            //dto.setRowNum();
//            SourceDto source = metadataInstancesDto.getSource();
//            if (source != null) {
//                dto.setSourceType(source.getDatabase_type());
//            }
//            dto.setId(metadataInstancesDto.getId().toHexString());
//            dto.setCategory(DataObjCategoryEnum.storage);
//            dto.setType(metadataInstancesDto.getMetaType());
//            dto.setName(metadataInstancesDto.getOriginalName());
//            dto.setSourceCategory(DataSourceCategoryEnum.connection);
//            dto.setSourceType(metadataInstancesDto.getSource() == null ? null : metadataInstancesDto.getSource().getDatabase_type());
//            dto.setSourceInfo(getConnectInfo(metadataInstancesDto.getSource(), metadataInstancesDto.getOriginalName()));
//            //dto.setSourceInfo();
//            //dto.setName();
//            //dto.setBusinessName();
//            //dto.setBusinessDesc();
//            dto.setListtags(metadataInstancesDto.getListtags());
//            List<Tag> listtags = dto.getListtags();
//            if (CollectionUtils.isNotEmpty(listtags)) {
//                List<ObjectId> ids = listtags.stream().map(t->MongoUtils.toObjectId(t.getId())).collect(Collectors.toList());
//                List<MetadataDefinitionDto> andParents = metadataDefinitionService.findAndParent(null, ids);
//                List<Tag> allTags = andParents.stream().map(s -> new Tag(s.getId().toHexString(), s.getValue())).collect(Collectors.toList());
//                dto.setAllTags(allTags);
//            }
//
//            items.add(dto);
//        }
//
//        page.setItems(items);
//        page.setTotal(count);
//        return page;
//    }
//
//    //@Override
//    public Page<DataDiscoveryDto> find2(DiscoveryQueryParam param, UserDetail user) {
//        if (param.getPage() == null) {
//            param.setPage(1);
//        }
//
//        if (param.getPageSize() == null) {
//            param.setPageSize(20);
//        }
//
//        Page<DataDiscoveryDto> page = new Page<>();
//        page.setItems(Lists.newArrayList());
//        page.setTotal(0);
//
//        Criteria taskCriteria = Criteria.where("is_deleted").ne(true);
//        Criteria apiCriteria = Criteria.where("status").is("active");
//
//        Criteria metadataCriteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
//                .and("taskId").exists(false)
//                .and("is_deleted").ne(true);
//        if (StringUtils.isNotBlank(param.getType())) {
//            metadataCriteria.and("meta_type").is(param.getType());
//            taskCriteria.and("syncType").is(param.getType());
//        } else {
//            taskCriteria.and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC);
//            metadataCriteria.and("meta_type").is("table");
//        }
//        if (StringUtils.isNotBlank(param.getSourceType())) {
//            metadataCriteria.and("source.database_type").is(param.getSourceType());
//            taskCriteria.and("agentId").is(param.getSourceType());
//        } else {
//            taskCriteria.and("agentId").exists(true);
//        }
//
//
//
//        if (StringUtils.isNotBlank(param.getQueryKey())) {
//            metadataCriteria.orOperator(
//                    Criteria.where("original_name").regex(param.getQueryKey()),
//                    Criteria.where("name").regex(param.getQueryKey()),
//                    Criteria.where("comment").regex(param.getQueryKey()),
//                    Criteria.where("source.name").regex(param.getQueryKey()),
//                    Criteria.where("alias_name").regex(param.getQueryKey()));
//
//            taskCriteria.orOperator(
//                    Criteria.where("name").regex(param.getQueryKey()),
//                    Criteria.where("desc").regex(param.getQueryKey()));
//
//            apiCriteria.orOperator(
//                    Criteria.where("name").regex(param.getQueryKey()),
//                    Criteria.where("tableName").regex(param.getQueryKey()));
//        }
//
//        if (StringUtils.isNotBlank(param.getTagId())) {
//            List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.newArrayList(MongoUtils.toObjectId(param.getTagId())));
//            List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());
//            metadataCriteria.and("listtags.id").in(tagIds);
//            taskCriteria.and("listtags.id").in(tagIds);
//        }
//
//        UnionWithOperation taskUnion = UnionWithOperation.unionWith("TaskCollectionObj")
//                .pipeline(
//                        Aggregation.project("createTime", "_id", "listtags", "syncType", "name", "agentId", "is_deleted"),
//                        Aggregation.match(taskCriteria)
//                );
//
//        UnionWithOperation apiUnion = UnionWithOperation.unionWith("Modules")
//                .pipeline(
//                        Aggregation.project("createTime", "_id", "listtags", "name", "apiType", "tableName", "status"),
//                        Aggregation.match(apiCriteria)
//                );
//
//
//        UnionWithOperation metadataUnion = UnionWithOperation.unionWith("MetadataInstances")
//                .pipeline(
//                        Aggregation.project("createTime", "_id", "listtags", "meta_type", "original_name"
//                                , "source"),
//                        Aggregation.match(metadataCriteria)
//                );
//        MatchOperation match = Aggregation.match(metadataCriteria);
//        MatchOperation match1 = Aggregation.match(new Criteria("_id").is("123456789"));
//        ProjectionOperation project = Aggregation.project("createTime", "_id", "listtags", "meta_type", "original_name"
//                , "source", "syncType", "name", "agentId", "apiType", "tableName");
//        LimitOperation limitOperation = Aggregation.limit(param.getPageSize());
//        SkipOperation skipOperation = Aggregation.skip((long) (param.getPage() - 1) * param.getPageSize());
//        SortOperation sortOperation = Aggregation.sort(Sort.Direction.DESC, "createTime");
//
//        Aggregation aggregation = null;
//        long total = 0L;
//        if (StringUtils.isNotBlank(param.getCategory()) || StringUtils.isNotBlank(param.getSourceCategory())) {
//            if (StringUtils.isNotBlank(param.getCategory()) && StringUtils.isNotBlank(param.getSourceCategory())) {
//                if (DataObjCategoryEnum.storage.name().equals(param.getCategory())) {
//                    if (DataSourceCategoryEnum.connection.name().equals(param.getSourceCategory())) {
//                        aggregation = Aggregation.newAggregation(match,  project, sortOperation, skipOperation, limitOperation);
//                        total = metadataInstancesService.count(new Query(metadataCriteria), user);
//                    } else {
//                        return page;
//                    }
//                } else if (DataObjCategoryEnum.job.name().equals(param.getCategory())) {
//                    if (DataSourceCategoryEnum.pipe.name().equals(param.getSourceCategory())) {
//                        aggregation = Aggregation.newAggregation(match1, taskUnion, project, sortOperation, skipOperation, limitOperation);
//                        total = taskRepository.count(new Query(taskCriteria), user);
//                    } else {
//                        return page;
//                    }
//                } else if (DataObjCategoryEnum.api.name().equals(param.getCategory())) {
//                    if (DataSourceCategoryEnum.server.name().equals(param.getSourceCategory())) {
//                        aggregation = Aggregation.newAggregation(match1, apiUnion, project, sortOperation, skipOperation, limitOperation);
//                        total = modulesService.count(new Query(apiCriteria), user);
//                    } else {
//                        return page;
//                    }
//                }
//            } else {
//                if (StringUtils.isNotBlank(param.getCategory())) {
//                    if (DataObjCategoryEnum.storage.name().equals(param.getCategory())) {
//                        aggregation = Aggregation.newAggregation(match, project, sortOperation, skipOperation, limitOperation);
//                        total = metadataInstancesService.count(new Query(metadataCriteria), user);
//                    } else if (DataObjCategoryEnum.job.name().equals(param.getCategory())) {
//                        aggregation = Aggregation.newAggregation(match1, taskUnion, project, sortOperation, skipOperation, limitOperation);
//                        total = taskRepository.count(new Query(taskCriteria), user);
//                    } else if (DataObjCategoryEnum.api.name().equals(param.getCategory())) {
//                        aggregation = Aggregation.newAggregation(match1, apiUnion, project, sortOperation, skipOperation, limitOperation);
//                        total = modulesService.count(new Query(apiCriteria), user);
//                    }
//
//                } else {
//                    if (DataSourceCategoryEnum.connection.name().equals(param.getSourceCategory())) {
//                        aggregation = Aggregation.newAggregation(match, project, sortOperation, skipOperation, limitOperation);
//                        total = metadataInstancesService.count(new Query(metadataCriteria), user);
//                    } else if (DataSourceCategoryEnum.pipe.name().equals(param.getSourceCategory())) {
//                        aggregation = Aggregation.newAggregation(match1, taskUnion, project, sortOperation, skipOperation, limitOperation);
//                        total = taskRepository.count(new Query(taskCriteria), user);
//                    } else if (DataSourceCategoryEnum.server.name().equals(param.getSourceCategory())) {
//                        aggregation = Aggregation.newAggregation(match1, apiUnion, project, sortOperation, skipOperation, limitOperation);
//                        total = modulesService.count(new Query(apiCriteria), user);
//                    }
//                }
//            }
//        } else {
//            long count1 = metadataInstancesService.count(new Query(metadataCriteria), user);
//            long count2 = taskRepository.count(new Query(taskCriteria), user);
//            long count3 = modulesService.count(new Query(apiCriteria), user);
//            total = count1 + count2 + count3;
//            aggregation = Aggregation.newAggregation(match, taskUnion, apiUnion, project, sortOperation, skipOperation, limitOperation);
//        }
//        //Aggregation aggregation = Aggregation.newAggregation(match, metadataUnion, taskUnion, apiUnion, project, sortOperation, skipOperation, limitOperation);
//        AggregationResults<UnionQueryResult> results = metaDataRepository.getMongoOperations().aggregate(aggregation, "MetadataInstances", UnionQueryResult.class);
//        List<UnionQueryResult> unionQueryResults = results.getMappedResults();
//
//        if (CollectionUtils.isEmpty(unionQueryResults)) {
//            return page;
//        }
//
//        List<DataDiscoveryDto> dataDiscoveryDtos = unionQueryResults.stream().map(this::convertToDataDiscovery).collect(Collectors.toList());
//
//        for (DataDiscoveryDto dataDiscoveryDto : dataDiscoveryDtos) {
//            List<Tag> listtags = dataDiscoveryDto.getListtags();
//            if (CollectionUtils.isNotEmpty(listtags)) {
//                List<ObjectId> ids = listtags.stream().map(t->MongoUtils.toObjectId(t.getId())).collect(Collectors.toList());
//                List<MetadataDefinitionDto> andParents = metadataDefinitionService.findAndParent(null, ids);
//                List<Tag> allTags = andParents.stream().map(s -> new Tag(s.getId().toHexString(), s.getValue())).collect(Collectors.toList());
//                dataDiscoveryDto.setAllTags(allTags);
//            }
//        }
//
//        page.setTotal(total);
//        page.setItems(dataDiscoveryDtos);
//        return page;
//    }



    @Override
    public Page<DataDiscoveryDto> find(DiscoveryQueryParam param, UserDetail user) {
        if (param.getPage() == null) {
            param.setPage(1);
        }

        if (param.getPageSize() == null) {
            param.setPageSize(20);
        }

        Page<DataDiscoveryDto> page = new Page<>();
        page.setItems(Lists.newArrayList());
        page.setTotal(0);

        Criteria taskCriteria = Criteria.where("is_deleted").ne(true);
        Criteria apiCriteria = Criteria.where("status").is("active").and("is_deleted").ne(true);

        Criteria metadataCriteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true);
        if (StringUtils.isNotBlank(param.getType())) {
            metadataCriteria.and("meta_type").is(param.getType());
            taskCriteria.and("syncType").is(param.getType());
            apiCriteria.and("apiType").is(param.getType());
        } else {
            taskCriteria.and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC);
            metadataCriteria.and("meta_type").is("table");
        }
        if (StringUtils.isNotBlank(param.getSourceType())) {
            metadataCriteria.and("source.database_type").is(param.getSourceType());
            taskCriteria.and("agentId").is(param.getSourceType());
//            apiCriteria.and("_id").is("123");
        } else {
            taskCriteria.and("agentId").exists(true);
        }

        if (!user.isRoot()) {
            taskCriteria.and("user_id").is(user.getUserId());
        }



        if (StringUtils.isNotBlank(param.getQueryKey())) {
            metadataCriteria.orOperator(
                    Criteria.where("original_name").regex(param.getQueryKey()),
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("comment").regex(param.getQueryKey()),
                    Criteria.where("source.name").regex(param.getQueryKey()),
                    Criteria.where("alias_name").regex(param.getQueryKey()));

            taskCriteria.orOperator(
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("desc").regex(param.getQueryKey()));

            apiCriteria.orOperator(
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("tableName").regex(param.getQueryKey()));
        }

        if (StringUtils.isNotBlank(param.getTagId())) {
            List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.newArrayList(MongoUtils.toObjectId(param.getTagId())));
            List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());
            metadataCriteria.and("listtags.id").in(tagIds);
            taskCriteria.and("listtags.id").in(tagIds);
            apiCriteria.and("listtags.id").in(tagIds);
        }

        long total = 0L;
        long metaTotal = 0L;
        long taskTotal = 0L;
        long apiTotal = 0L;
        if (StringUtils.isNotBlank(param.getCategory()) || StringUtils.isNotBlank(param.getSourceCategory())) {
            if (StringUtils.isNotBlank(param.getCategory()) && StringUtils.isNotBlank(param.getSourceCategory())) {
                if (DataObjCategoryEnum.storage.name().equals(param.getCategory())) {
                    if (DataSourceCategoryEnum.connection.name().equals(param.getSourceCategory())) {
                        metaTotal = metadataInstancesService.count(new Query(metadataCriteria), user);
                        taskCriteria.and("_id").is("123123123");
                        apiCriteria.and("_id").is("123123123");
                        total = metaTotal;
                    } else {
                        return page;
                    }
                } else if (DataObjCategoryEnum.job.name().equals(param.getCategory())) {
                    if (DataSourceCategoryEnum.pipe.name().equals(param.getSourceCategory())) {
                        taskTotal = taskRepository.count(new Query(taskCriteria), user);
                        metadataCriteria.and("_id").is("123123123");
                        apiCriteria.and("_id").is("123123123");
                        total = taskTotal;
                    } else {
                        return page;
                    }
                } else if (DataObjCategoryEnum.api.name().equals(param.getCategory())) {
                    if (DataSourceCategoryEnum.server.name().equals(param.getSourceCategory())) {
                        apiTotal = modulesService.count(new Query(apiCriteria), user);
                        taskCriteria.and("_id").is("123123123");
                        metadataCriteria.and("_id").is("123123123");
                        total = apiTotal;

                    } else {
                        return page;
                    }
                }
            } else {
                if (StringUtils.isNotBlank(param.getCategory())) {
                    if (DataObjCategoryEnum.storage.name().equals(param.getCategory())) {
                        total = metadataInstancesService.count(new Query(metadataCriteria), user);
                        taskCriteria.and("_id").is("123123123");
                        apiCriteria.and("_id").is("123123123");
                        metaTotal = total;
                    } else if (DataObjCategoryEnum.job.name().equals(param.getCategory())) {
                        total = taskRepository.count(new Query(taskCriteria), user);
                        metadataCriteria.and("_id").is("123123123");
                        apiCriteria.and("_id").is("123123123");
                        taskTotal = total;
                    } else if (DataObjCategoryEnum.api.name().equals(param.getCategory())) {
                        total = modulesService.count(new Query(apiCriteria), user);
                        taskCriteria.and("_id").is("123123123");
                        metadataCriteria.and("_id").is("123123123");
                        apiTotal = total;
                    }

                } else {
                    if (DataSourceCategoryEnum.connection.name().equals(param.getSourceCategory())) {
                        total = metadataInstancesService.count(new Query(metadataCriteria), user);
                        taskCriteria.and("_id").is("123123123");
                        apiCriteria.and("_id").is("123123123");
                        metaTotal = total;
                    } else if (DataSourceCategoryEnum.pipe.name().equals(param.getSourceCategory())) {
                        total = taskRepository.count(new Query(taskCriteria), user);
                        metadataCriteria.and("_id").is("123123123");
                        apiCriteria.and("_id").is("123123123");
                        taskTotal = total;
                    } else if (DataSourceCategoryEnum.server.name().equals(param.getSourceCategory())) {
                        total = modulesService.count(new Query(apiCriteria), user);
                        taskCriteria.and("_id").is("123123123");
                        metadataCriteria.and("_id").is("123123123");
                        apiTotal = total;
                    }
                }
            }
        } else {
            metaTotal = metadataInstancesService.count(new Query(metadataCriteria), user);
            taskTotal = taskRepository.count(new Query(taskCriteria), user);
            apiTotal = modulesService.count(new Query(apiCriteria), user);
            total = metaTotal + taskTotal + apiTotal;
        }

        long skip = (long) (param.getPage() - 1) * param.getPageSize();

        List<UnionQueryResult> unionQueryResults = new ArrayList<>();
        if (metaTotal >= skip + param.getPageSize()) {
            Query query = new Query(metadataCriteria);
            metaDataRepository.applyUserDetail(query, user);
            query.skip(skip);
            query.limit(param.getPageSize());
            List<UnionQueryResult> metaUnionQueryResults = metaDataRepository.getMongoOperations().find(query, UnionQueryResult.class, "MetadataInstances");
            unionQueryResults.addAll(metaUnionQueryResults);
        } else if (metaTotal + taskTotal >=  skip + param.getPageSize()) {
            if (metaTotal <= skip) {
                //只需要查询task
                Query query = new Query(taskCriteria);
                taskRepository.applyUserDetail(query, user);
                query.skip(skip - metaTotal);
                query.limit(param.getPageSize());
                List<UnionQueryResult> taskUnionQueryResults = taskRepository.getMongoOperations().find(query, UnionQueryResult.class, "TaskCollectionObj");
                unionQueryResults.addAll(taskUnionQueryResults);
            } else {
                //需要两个表
                Query query = new Query(metadataCriteria);
                metaDataRepository.applyUserDetail(query, user);
                query.skip(skip);
                query.limit(param.getPageSize());
                List<UnionQueryResult> metaUnionQueryResults = metaDataRepository.getMongoOperations().find(query, UnionQueryResult.class, "MetadataInstances");

                Query queryTask = new Query(taskCriteria);
                taskRepository.applyUserDetail(queryTask, user);
                queryTask.skip(skip - metaTotal);
                queryTask.limit(param.getPageSize() - metaUnionQueryResults.size());
                List<UnionQueryResult> taskUnionQueryResults = taskRepository.getMongoOperations().find(queryTask, UnionQueryResult.class, "TaskCollectionObj");
                unionQueryResults.addAll(metaUnionQueryResults);
                unionQueryResults.addAll(taskUnionQueryResults);
            }

        } else {
            if (metaTotal + taskTotal <= skip) {
                //只需要查询api
                Query query = new Query(apiCriteria);
                taskRepository.applyUserDetail(query, user);
                query.skip(skip- metaTotal - taskTotal);
                query.limit(param.getPageSize());
                query.with(Sort.by("createTime").descending());
                List<UnionQueryResult> apiUnionQueryResults = taskRepository.getMongoOperations().find(query, UnionQueryResult.class, "Modules");
                unionQueryResults.addAll(apiUnionQueryResults);
            } else if (metaTotal <= skip) {
                //需要查询task+api

                Query query = new Query(taskCriteria);
                taskRepository.applyUserDetail(query, user);
                query.skip(skip - metaTotal);
                query.limit(param.getPageSize());
                query.with(Sort.by("createTime").descending());
                List<UnionQueryResult> taskUnionQueryResults = taskRepository.getMongoOperations().find(query, UnionQueryResult.class, "TaskCollectionObj");

                Query queryApi = new Query(apiCriteria);
                taskRepository.applyUserDetail(queryApi, user);
                queryApi.skip(skip - metaTotal - taskTotal);
                queryApi.limit(param.getPageSize() - taskUnionQueryResults.size());
                queryApi.with(Sort.by("createTime").descending());
                List<UnionQueryResult> apiUnionQueryResults = taskRepository.getMongoOperations().find(queryApi, UnionQueryResult.class, "Modules");
                unionQueryResults.addAll(taskUnionQueryResults);
                unionQueryResults.addAll(apiUnionQueryResults);
            } else {
                //需要查询meta task api

                Query query = new Query(metadataCriteria);
                taskRepository.applyUserDetail(query, user);
                query.skip(skip);
                query.limit(param.getPageSize());
                query.with(Sort.by("createTime").descending());
                List<UnionQueryResult> metaUnionQueryResults = taskRepository.getMongoOperations().find(query, UnionQueryResult.class, "MetadataInstances");

                Query queryTask = new Query(taskCriteria);
                taskRepository.applyUserDetail(queryTask, user);
                queryTask.skip(skip - metaTotal);
                queryTask.limit(param.getPageSize() - metaUnionQueryResults.size());
                queryTask.with(Sort.by("createTime").descending());
                List<UnionQueryResult> taskUnionQueryResults = taskRepository.getMongoOperations().find(queryTask, UnionQueryResult.class, "TaskCollectionObj");

                Query queryApi = new Query(apiCriteria);
                taskRepository.applyUserDetail(queryApi, user);
                queryApi.skip(skip - metaTotal - taskTotal);
                queryApi.with(Sort.by("createTime").descending());
                queryApi.limit(param.getPageSize() - metaUnionQueryResults.size() + taskUnionQueryResults.size());
                List<UnionQueryResult> apiUnionQueryResults = taskRepository.getMongoOperations().find(queryApi, UnionQueryResult.class, "Modules");
                unionQueryResults.addAll(metaUnionQueryResults);
                unionQueryResults.addAll(taskUnionQueryResults);
                unionQueryResults.addAll(apiUnionQueryResults);
            }

        }

        if (CollectionUtils.isEmpty(unionQueryResults)) {
            return page;
        }


        List<String> agentIds = unionQueryResults.stream().filter(u -> StringUtils.isNotBlank(u.getSyncType()))
                .map(UnionQueryResult::getAgentId).collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(agentIds)) {
            Map<String, String> agentMap = new HashMap<>();
            Criteria agentIdCriteria = Criteria.where("systemInfo.process_id").in(agentIds).and("status").is("running");
            Query query = new Query(agentIdCriteria);
            query.fields().include("systemInfo");
            List<ClusterStateDto> workers = clusterStateService.findAll(query);
            workers = workers.stream().filter(Objects::nonNull).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(workers)) {
                agentMap = workers.stream().collect(Collectors.toMap(w -> w.getSystemInfo().getProcess_id(), w -> w.getSystemInfo().getIp(), (w1, w2) -> w1));
            }

            for (UnionQueryResult unionQueryResult : unionQueryResults) {
                if (StringUtils.isNotBlank(unionQueryResult.getAgentId())) {
                    unionQueryResult.setSourceInfo(agentMap.get(unionQueryResult.getAgentId()));
                }
            }
        }

        String clientURI = getClientURI(user);
        for (UnionQueryResult unionQueryResult : unionQueryResults) {
            if (StringUtils.isBlank(unionQueryResult.getMeta_type()) && StringUtils.isBlank(unionQueryResult.getSyncType())) {
                unionQueryResult.setSourceInfo(clientURI);
            }
        }


        List<DataDiscoveryDto> dataDiscoveryDtos = unionQueryResults.stream().map(this::convertToDataDiscovery).collect(Collectors.toList());

        for (DataDiscoveryDto dataDiscoveryDto : dataDiscoveryDtos) {
            List<Tag> listtags = dataDiscoveryDto.getListtags();
            if (CollectionUtils.isNotEmpty(listtags)) {
                List<ObjectId> ids = listtags.stream().map(t->MongoUtils.toObjectId(t.getId())).collect(Collectors.toList());
                List<MetadataDefinitionDto> andParents = metadataDefinitionService.findAndParent(null, ids);
                List<Tag> allTags = andParents.stream().map(s -> new Tag(s.getId().toHexString(), s.getValue())).collect(Collectors.toList());
                dataDiscoveryDto.setAllTags(allTags);
            }
        }

        page.setTotal(total);
        page.setItems(dataDiscoveryDtos);
        return page;
    }
    /**
     * 查询存储对象预览
     *
     * @param id
     * @return
     */
    @Override
    public Page<Object> storagePreview(String id, UserDetail user) {
        return null;
    }

    /**
     * 查询存储对象概览
     *
     * @param id
     * @return
     */
    @Override
    public DiscoveryStorageOverviewDto storageOverview(String id, UserDetail user) {
        MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findById(MongoUtils.toObjectId(id));
        DiscoveryStorageOverviewDto dto = new DiscoveryStorageOverviewDto();
        dto.setCreateAt(metadataInstancesDto.getCreateAt());
        dto.setVersion(metadataInstancesDto.getSchemaVersion());
        dto.setLastUpdAt(metadataInstancesDto.getLastUpdAt());
        dto.setFieldNum(CollectionUtils.isEmpty(metadataInstancesDto.getFields()) ? 0 : metadataInstancesDto.getFields().size());
        //dto.setRowNum();
        SourceDto source = metadataInstancesDto.getSource();
        if (source != null) {
            dto.setConnectionName(source.getName());
            dto.setConnectionType(source.getDatabase_type());
            dto.setConnectionDesc(source.getDescription());
            dto.setSourceType(source.getDatabase_type());

        }
        dto.setComment(metadataInstancesDto.getComment());
        dto.setId(metadataInstancesDto.getId().toHexString());
        dto.setName(metadataInstancesDto.getOriginalName());
        dto.setCategory(DataObjCategoryEnum.storage);
        dto.setType(metadataInstancesDto.getMetaType());
        dto.setSourceCategory(DataSourceCategoryEnum.connection);
        dto.setSourceInfo(getConnectInfo(metadataInstancesDto.getSource(), metadataInstancesDto.getOriginalName()));
        dto.setDescription(metadataInstancesDto.getDescription());
        //dto.setSourceInfo();
        //dto.setBusinessName();
        //dto.setBusinessDesc();
        dto.setListtags(metadataInstancesDto.getListtags());
        //dto.setAllTags();
        List<Field> fields = metadataInstancesDto.getFields();

        List<TableIndex> indices = metadataInstancesDto.getIndices();
        Set<String> indexNames = new HashSet<>();
        if (CollectionUtils.isNotEmpty(indices)) {
            for (TableIndex index : indices) {
                List<TableIndexColumn> columns = index.getColumns();
                for (TableIndexColumn column : columns) {
                    indexNames.add(column.getColumnName());
                }
            }
        }

        List<DiscoveryFieldDto> dataFields = new ArrayList<>();
        dto.setFields(dataFields);
        if (CollectionUtils.isNotEmpty(fields)) {
            for (Field field : fields) {
                DiscoveryFieldDto discoveryFieldDto = new DiscoveryFieldDto();
                discoveryFieldDto.setName(field.getFieldName());
                discoveryFieldDto.setDataType(field.getDataType());
                discoveryFieldDto.setPrimaryKey(field.getPrimaryKey());
                discoveryFieldDto.setForeignKey(field.getForeignKey() != null ? field.getForeignKey() : field.getForeignKeyTable() != null);
                discoveryFieldDto.setBusinessType(tapTypeString(field.getTapType()));

                discoveryFieldDto.setIndex(indexNames.contains(field.getFieldName()));

                if (field.getIsNullable() != null && field.getIsNullable() instanceof String) {
                    discoveryFieldDto.setNotNull("YES".equals(field.getIsNullable()));
                } else if (field.getIsNullable() != null && field.getIsNullable() instanceof Boolean) {
                    discoveryFieldDto.setNotNull(!(Boolean) field.getIsNullable());
                }
                discoveryFieldDto.setDefaultValue(field.getDefaultValue());
                //discoveryFieldDto.setBusinessName();
                //discoveryFieldDto.setBusinessType();
                discoveryFieldDto.setBusinessDesc(field.getComment());

                dataFields.add(discoveryFieldDto);

            }
        }
        return dto;
    }

    @Override
    public DiscoveryTaskOverviewDto taskOverview(String id, UserDetail user) {
        TaskCollectionObjDto taskDto = taskService.findById(MongoUtils.toObjectId(id), user);
        DiscoveryTaskOverviewDto dto = new DiscoveryTaskOverviewDto();
        dto.setCreateAt(taskDto.getCreateAt());
        dto.setLastUpdAt(taskDto.getLastUpdAt());
        int nodeNum = 0;

        DAG dag = taskDto.getDag();
        if (dag != null) {
            nodeNum = dag.getNodes().size();
        }

        dto.setNodeNum(nodeNum);
        dto.setTaskDesc(taskDto.getDesc());
        dto.setVersion("");
        dto.setAgentId(taskDto.getAgentId());
        dto.setDag(dag);
        dto.setId(id);
        dto.setCategory(DataObjCategoryEnum.job);
        dto.setType(taskDto.getSyncType());
        dto.setSourceCategory(DataSourceCategoryEnum.pipe);
        dto.setSourceType("数据管道");


        if (StringUtils.isNotBlank(taskDto.getAgentId())) {
            Criteria criteria = Criteria.where("systemInfo.process_id").is(taskDto.getAgentId()).and("status").is("running");
            Query query = new Query(criteria);
            query.fields().include("systemInfo");
            ClusterStateDto one = clusterStateService.findOne(query);
            if (one != null) {
                dto.setSourceInfo(one.getSystemInfo().getIp());
                dto.setAgentDesc(one.getSystemInfo().getIp());
                dto.setAgentName(one.getSystemInfo().getHostname());
            } else {
                Criteria agentIdCriteria = Criteria.where("process_id").is(taskDto.getAgentId()).and("worker_type").is("connector");
                Query query1 = new Query(agentIdCriteria);
                query.fields().include("hostname", "process_id");
                WorkerDto one1 = workerService.findOne(query1, user);
                dto.setAgentName(one1.getHostname());

            }
        }


        dto.setName(taskDto.getName());
        dto.setListtags(taskDto.getListtags());

        dto.setTaskConnections(getTaskConnection(dag));
        return dto;
    }


    private List<TaskConnectionsDto> getTaskConnection(DAG dag) {
        List<Node> nodes = dag.getNodes();
        List<ObjectId> connectionIds = nodes.stream().filter(n -> n instanceof DataParentNode).map(n -> MongoUtils.toObjectId(((DataParentNode<?>) n)
                .getConnectionId())).collect(Collectors.toList());
        Criteria criteria = Criteria.where("_id").in(connectionIds);
        Query query = new Query(criteria);
        List<DataSourceConnectionDto> dataSourceConnectionDtos = dataSourceService.findAll(query);
        Map<String, DataSourceConnectionDto> dataSourceConnectionDtoMap = dataSourceConnectionDtos.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), d -> d, (d1, d2) -> d1));

        Map<String, List<String>> inputMap = new HashMap<>();
        Map<String, List<String>> outputMap = new HashMap<>();

        List<TaskConnectionsDto> taskConnectionsDtos = new ArrayList<>();
        for (Edge edge : dag.getEdges()) {
            List<String> inputs = inputMap.get(edge.getTarget());
            if (inputs == null) {
                inputs = new ArrayList<>();
            }
            inputs.add(edge.getSource());

            inputMap.put(edge.getTarget(), inputs);

            List<String> targets = outputMap.get(edge.getSource());
            if (targets == null) {
                targets = new ArrayList<>();
            }
            targets.add(edge.getTarget());
            outputMap.put(edge.getSource(), targets);
        }


        for (Node node : nodes) {
            TaskConnectionsDto taskConnectionsDto = new TaskConnectionsDto();
            taskConnectionsDto.setName(node.getName());

            List<String> targets = outputMap.get(node.getId());
            List<String> inputs = inputMap.get(node.getId());


            String inputNodeName = "";
            StringBuilder inputName = new StringBuilder();
            if (CollectionUtils.isNotEmpty(inputs)) {
                for (String input : inputs) {
                    Node<?> node1 = dag.getNode(input);
                    inputName.append(node1.getName()).append(";");
                }
                inputNodeName = inputName.substring(0, inputName.length()-1);
            }

            String outputNodeName = "";
            StringBuilder outName = new StringBuilder();
            if (CollectionUtils.isNotEmpty(targets)) {
                for (String target : targets) {
                    Node<?> node1 = dag.getNode(target);
                    outName.append(node1.getName()).append(";");
                }
                outputNodeName = outName.substring(0, outName.length()-1);
            }

            taskConnectionsDto.setInputNodeName(inputNodeName);
            taskConnectionsDto.setOutputNodeName(outputNodeName);
            if (node instanceof DataParentNode) {
                if (CollectionUtils.isNotEmpty(targets)) {
                    taskConnectionsDto.setType("source");
                } else {
                    taskConnectionsDto.setType("target");
                }
                String connectionId = ((DataParentNode<?>) node).getConnectionId();
                DataSourceConnectionDto connectionDto = dataSourceConnectionDtoMap.get(connectionId);

                taskConnectionsDto.setConnectionName(connectionDto.getName());
                SourceDto sourceDto = JsonUtil.parseJsonUseJackson(JsonUtil.toJsonUseJackson(connectionDto), SourceDto.class);
                taskConnectionsDto.setConnectionInfo(getConnectInfo(sourceDto, null));
            } else {
                taskConnectionsDto.setType("calculate");
            }
            taskConnectionsDtos.add(taskConnectionsDto);

        }

        return taskConnectionsDtos;
    }

    @Override
    public DiscoveryApiOverviewDto apiOverview(String id, UserDetail user) {
        ModulesDto modulesDto = modulesService.findById(MongoUtils.toObjectId(id), user);
        DiscoveryApiOverviewDto dto = new DiscoveryApiOverviewDto();
        dto.setPaths(modulesDto.getPaths());
        dto.setFields(modulesDto.getFields());
        dto.setCreateAt(modulesDto.getCreateAt());
        dto.setLastUpdAt(modulesDto.getLastUpdAt());
        int inputParamNum = 4;
        if (CollectionUtils.isNotEmpty(modulesDto.getPaths())) {
            inputParamNum = modulesDto.getPaths().get(0).getParams().size();
        }
        dto.setInputParamNum(inputParamNum);
        dto.setOutputParamNum(modulesDto.getFields().size());
        dto.setId(id);

        String clientURI = getClientURI(user);

        dto.setSourceInfo(clientURI);
        dto.setDescription(clientURI);
        dto.setSourceType("数据服务");
        dto.setCategory(DataObjCategoryEnum.api);
        dto.setType(modulesDto.getApiType());
        dto.setSourceCategory(DataSourceCategoryEnum.server);
        dto.setName(modulesDto.getName());
        dto.setListtags(modulesDto.getListtags());

        return dto;
    }

    @NotNull
    private String getClientURI(UserDetail user) {
        ApiServerDto one = apiServerService.findOne(new Query(), user);
        String clientURI = one.getClientURI();
        int i = clientURI.indexOf("://");
        clientURI = clientURI.substring(i + 3);
        return clientURI;
    }

    @Override
    public Map<ObjectFilterEnum, List<String>> filterList(List<ObjectFilterEnum> filterTypes, UserDetail user) {
        Map<ObjectFilterEnum, List<String>> returnMap = new HashMap<>();
        for (ObjectFilterEnum filterType : filterTypes) {
            switch (filterType) {
                case objCategory:
                    List<String> objCategorys = objCategoryFilterList();
                    returnMap.put(ObjectFilterEnum.objCategory, objCategorys);
                    break;
                case objType:
                    List<String> objTypes = objTypeFilterList(user);
                    returnMap.put(ObjectFilterEnum.objType, objTypes);
                    break;
                case sourceCategory:
                    List<String> sourceCateGorys = sourceCategoryFilterList();
                    returnMap.put(ObjectFilterEnum.sourceCategory, sourceCateGorys);
                    break;
                case sourceType:
                    List<String> sourceTypes = sourceTypeFilterList(user);
                    returnMap.put(ObjectFilterEnum.sourceType, sourceTypes);
                    break;
                case itemType:
                    List<String> itemType = itemTypeFilterList();
                    returnMap.put(ObjectFilterEnum.itemType, itemType);
                    break;
                default:
                    break;
            }
        }
        return returnMap;
    }

    private List<String> itemTypeFilterList() {
        return Arrays.stream(ItemTypeEnum.values()).map(Enum::name).collect(Collectors.toList());
    }

    @Override
    public Page<DataDirectoryDto> findDataDirectory(DirectoryQueryParam param, UserDetail user) {
        if (param.getPage() == null) {
            param.setPage(1);
        }

        if (param.getPageSize() == null) {
            param.setPageSize(20);
        }

        Page<DataDirectoryDto> page = new Page<>();
        page.setItems(Lists.newArrayList());
        page.setTotal(0);

        Criteria taskCriteria = Criteria.where("is_deleted").ne(true).and("agentId").exists(true);
        boolean isRoot = true;
        if (!user.isRoot()) {
            isRoot = false;
            taskCriteria.and("user_id").is(user.getUserId());
        }
        Criteria apiCriteria = Criteria.where("status").is("active").and("is_deleted").ne(true);

        Criteria metadataCriteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true);
        if (StringUtils.isNotBlank(param.getObjType())) {
            metadataCriteria.and("meta_type").is(param.getObjType());
            //taskCriteria.and("syncType").is(param.getObjType());
            apiCriteria.and("apiType").is(param.getObjType());
        } else {
            metadataCriteria.and("meta_type").is("table");
        }

        if (StringUtils.isNotBlank(param.getQueryKey())) {
            metadataCriteria.orOperator(
                    Criteria.where("original_name").regex(param.getQueryKey()),
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("comment").regex(param.getQueryKey()),
                    Criteria.where("source.name").regex(param.getQueryKey()),
                    Criteria.where("alias_name").regex(param.getQueryKey()));

            taskCriteria.orOperator(
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("desc").regex(param.getQueryKey()));

            apiCriteria.orOperator(
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("tableName").regex(param.getQueryKey()));
        }


        if (StringUtils.isNotBlank(param.getTagId())) {
            MetadataDefinitionDto definitionDto = metadataDefinitionService.findById(MongoUtils.toObjectId(param.getTagId()));
            if (definitionDto != null) {
                List<String> itemTypes = definitionDto.getItemType();
                boolean isDefault = itemTypes.contains("default");
                List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.newArrayList(MongoUtils.toObjectId(param.getTagId())));
                if (!isDefault) {

                    if (StringUtils.isBlank(param.getObjType())) {
                        taskCriteria.and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC);
                    } else if ((!param.getObjType().equals(TaskDto.SYNC_TYPE_SYNC) && !param.getObjType().equals(TaskDto.SYNC_TYPE_MIGRATE))) {
                        taskCriteria.and("_id").is("1231231231");
                    } else {
                        taskCriteria.and("syncType").is(param.getObjType());
                    }
                    List<String> tagIds = andChild.stream().map(t->t.getId().toHexString()).collect(Collectors.toList());
                    metadataCriteria.and("listtags.id").in(tagIds);
                    taskCriteria.and("listtags.id").in(tagIds);
                    apiCriteria.and("listtags.id").in(tagIds);
                } else {
                    if (!definitionDto.getItemType().contains("Root")) {
                        DataObjCategoryEnum defaultObjEnum = getDefaultObjEnum(null, definitionDto.getId());
                        switch (defaultObjEnum) {
                            case storage:
                                taskCriteria.and("_id").is("1231231231");
                                apiCriteria.and("_id").is("1231231231");
                                List<String> linkIds = andChild.stream().map(MetadataDefinitionDto::getLinkId).filter(Objects::nonNull).collect(Collectors.toList());
                                if (CollectionUtils.isNotEmpty(linkIds)) {
                                    metadataCriteria.and("source._id").in(linkIds);
                                } else {
                                    return page;
                                }
                                break;
                            case job:
                                metadataCriteria.and("_id").is("1231231231");
                                apiCriteria.and("_id").is("1231231231");
                                if ("sync".equals(definitionDto.getValue())) {
                                    taskCriteria.and("syncType").is(TaskDto.SYNC_TYPE_SYNC);
                                } else if ("migrate".equals(definitionDto.getValue())) {
                                    taskCriteria.and("syncType").is(TaskDto.SYNC_TYPE_MIGRATE);
                                } else {
                                    taskCriteria.and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC);
                                }
                                if (isRoot) {
                                    taskCriteria.and("user_id").is(definitionDto.getUserId());
                                }
                                break;
                            case api:
                                metadataCriteria.and("_id").is("1231231231");
                                taskCriteria.and("_id").is("1231231231");
                                if (isRoot) {
                                    apiCriteria.and("user_id").is(definitionDto.getUserId());
                                }
                                break;
                            default:
                                break;

                        }
                    }
                }
            }
        }

        long total;
        long metaTotal;
        long taskTotal;

        metaTotal = metadataInstancesService.count(new Query(metadataCriteria), user);
        taskTotal = taskRepository.count(new Query(taskCriteria), user);
        long apiTotal = modulesService.count(new Query(apiCriteria), user);
        total = metaTotal + taskTotal + apiTotal;

        long skip = (long) (param.getPage() - 1) * param.getPageSize();

        List<UnionQueryResult> unionQueryResults = new ArrayList<>();
        if (metaTotal >= skip + param.getPageSize()) {
            Query query = new Query(metadataCriteria);
            metaDataRepository.applyUserDetail(query, user);
            query.skip(skip);
            query.limit(param.getPageSize());
            query.with(Sort.by("createTime").descending());
            List<UnionQueryResult> metaUnionQueryResults = metaDataRepository.getMongoOperations().find(query, UnionQueryResult.class, "MetadataInstances");
            unionQueryResults.addAll(metaUnionQueryResults);
        } else if (metaTotal + taskTotal >=  skip + param.getPageSize()) {
            if (metaTotal <= skip) {
                //只需要查询task
                Query query = new Query(taskCriteria);
                taskRepository.applyUserDetail(query, user);
                query.skip(skip - metaTotal);
                query.limit(param.getPageSize());
                query.with(Sort.by("createTime").descending());
                List<UnionQueryResult> taskUnionQueryResults = taskRepository.getMongoOperations().find(query, UnionQueryResult.class, "TaskCollectionObj");
                unionQueryResults.addAll(taskUnionQueryResults);
            } else {
                //需要两个表
                Query query = new Query(metadataCriteria);
                metaDataRepository.applyUserDetail(query, user);
                query.skip(skip);
                query.limit(param.getPageSize());
                query.with(Sort.by("createTime").descending());
                List<UnionQueryResult> metaUnionQueryResults = metaDataRepository.getMongoOperations().find(query, UnionQueryResult.class, "MetadataInstances");

                Query queryTask = new Query(taskCriteria);
                taskRepository.applyUserDetail(queryTask, user);
                queryTask.skip(skip - metaTotal);
                queryTask.limit(param.getPageSize() - metaUnionQueryResults.size());
                queryTask.with(Sort.by("createTime").descending());
                List<UnionQueryResult> taskUnionQueryResults = taskRepository.getMongoOperations().find(queryTask, UnionQueryResult.class, "TaskCollectionObj");
                unionQueryResults.addAll(metaUnionQueryResults);
                unionQueryResults.addAll(taskUnionQueryResults);
            }

        } else {
            if (metaTotal + taskTotal <= skip) {
                //只需要查询api
                Query query = new Query(apiCriteria);
                taskRepository.applyUserDetail(query, user);
                query.skip(skip- metaTotal - taskTotal);
                query.limit(param.getPageSize());
                query.with(Sort.by("createTime").descending());
                List<UnionQueryResult> apiUnionQueryResults = taskRepository.getMongoOperations().find(query, UnionQueryResult.class, "Modules");
                unionQueryResults.addAll(apiUnionQueryResults);
            } else if (metaTotal <= skip) {
                //需要查询task+api

                Query query = new Query(taskCriteria);
                taskRepository.applyUserDetail(query, user);
                query.skip(skip - metaTotal);
                query.limit(param.getPageSize());
                query.with(Sort.by("createTime").descending());
                List<UnionQueryResult> taskUnionQueryResults = taskRepository.getMongoOperations().find(query, UnionQueryResult.class, "TaskCollectionObj");

                Query queryTask = new Query(apiCriteria);
                taskRepository.applyUserDetail(queryTask, user);
                queryTask.skip(skip - metaTotal - taskTotal);
                queryTask.limit(param.getPageSize() - taskUnionQueryResults.size());
                queryTask.with(Sort.by("createTime").descending());
                List<UnionQueryResult> apiUnionQueryResults = taskRepository.getMongoOperations().find(queryTask, UnionQueryResult.class, "Modules");
                unionQueryResults.addAll(taskUnionQueryResults);
                unionQueryResults.addAll(apiUnionQueryResults);
            } else {
                //需要查询meta task api

                Query query = new Query(metadataCriteria);
                taskRepository.applyUserDetail(query, user);
                query.skip(skip);
                query.limit(param.getPageSize());
                query.with(Sort.by("createTime").descending());

                List<UnionQueryResult> metaUnionQueryResults = taskRepository.getMongoOperations().find(query, UnionQueryResult.class, "MetadataInstances");

                Query queryTask = new Query(taskCriteria);
                taskRepository.applyUserDetail(queryTask, user);
                queryTask.skip(skip - metaTotal);
                queryTask.limit(param.getPageSize() - metaUnionQueryResults.size());
                queryTask.with(Sort.by("createTime").descending());
                List<UnionQueryResult> taskUnionQueryResults = taskRepository.getMongoOperations().find(queryTask, UnionQueryResult.class, "TaskCollectionObj");

                Query queryApi = new Query(apiCriteria);
                taskRepository.applyUserDetail(queryApi, user);
                queryApi.skip(skip - metaTotal - taskTotal);
                queryApi.limit(param.getPageSize() - metaUnionQueryResults.size() + taskUnionQueryResults.size());
                queryApi.with(Sort.by("createTime").descending());
                List<UnionQueryResult> apiUnionQueryResults = taskRepository.getMongoOperations().find(queryApi, UnionQueryResult.class, "Modules");
                unionQueryResults.addAll(metaUnionQueryResults);
                unionQueryResults.addAll(taskUnionQueryResults);
                unionQueryResults.addAll(apiUnionQueryResults);
            }

        }

        if (CollectionUtils.isEmpty(unionQueryResults)) {
            return page;
        }


        List<DataDirectoryDto> items = unionQueryResults.stream().map(this::convertToDataDirectory).collect(Collectors.toList());

        page.setItems(items);
        page.setTotal(total);
        return page;
    }

    //@Override
    public Page<DataDirectoryDto> findDataDirectory1(DirectoryQueryParam param, UserDetail user) {
        if (param.getPage() == null) {
            param.setPage(1);
        }

        if (param.getPageSize() == null) {
            param.setPageSize(20);
        }

        Page<DataDirectoryDto> page = new Page<>();
        page.setItems(Lists.newArrayList());
        page.setTotal(0);

        Criteria taskCriteria = Criteria.where("is_deleted").ne(true);
        Criteria apiCriteria = Criteria.where("status").is("active");

        Criteria metadataCriteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true);
        if (StringUtils.isNotBlank(param.getObjType())) {
            metadataCriteria.and("meta_type").is(param.getObjType());
            taskCriteria.and("syncType").is(param.getObjType());
            apiCriteria.and("apiType").is(param.getObjType());
        } else {
            metadataCriteria.and("meta_type").is("table");
        }

        if (StringUtils.isNotBlank(param.getQueryKey())) {
            metadataCriteria.orOperator(
                    Criteria.where("original_name").regex(param.getQueryKey()),
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("comment").regex(param.getQueryKey()),
                    Criteria.where("source.name").regex(param.getQueryKey()),
                    Criteria.where("alias_name").regex(param.getQueryKey()));

            taskCriteria.orOperator(
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("desc").regex(param.getQueryKey()));

            apiCriteria.orOperator(
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("tableName").regex(param.getQueryKey()));
        }


        if (StringUtils.isNotBlank(param.getTagId())) {
            MetadataDefinitionDto definitionDto = metadataDefinitionService.findById(MongoUtils.toObjectId(param.getTagId()));
            if (definitionDto != null) {
                List<String> itemTypes = definitionDto.getItemType();
                boolean isDefault = itemTypes.contains("default");
                List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.newArrayList(MongoUtils.toObjectId(param.getTagId())));
                if (!isDefault) {

                    if (StringUtils.isBlank(param.getObjType())) {
                        taskCriteria.and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC);
                    }
                    List<String> tagIds = andChild.stream().map(t->t.getId().toHexString()).collect(Collectors.toList());
                    metadataCriteria.and("listtags.id").in(tagIds);
                    taskCriteria.and("listtags.id").in(tagIds);
                    apiCriteria.and("listtags.id").in(tagIds);
                } else {
                    if (!definitionDto.getValue().equals("Root")) {
                        DataObjCategoryEnum defaultObjEnum = getDefaultObjEnum(null, definitionDto.getId());
                        switch (defaultObjEnum) {
                            case storage:
                                taskCriteria.and("_id").is("1231231231");
                                apiCriteria.and("_id").is("1231231231");
                                List<String> linkIds = andChild.stream().map(MetadataDefinitionDto::getLinkId).filter(Objects::nonNull).collect(Collectors.toList());
                                if (CollectionUtils.isNotEmpty(linkIds)) {
                                    metadataCriteria.and("source._id").in(linkIds);
                                } else {
                                    return page;
                                }
                                break;
                            case job:
                                metadataCriteria.and("_id").is("1231231231");
                                apiCriteria.and("_id").is("1231231231");
                                if ("sync".equals(definitionDto.getValue())) {
                                    taskCriteria.and("syncType").is(TaskDto.SYNC_TYPE_SYNC);
                                } else if ("migrate".equals(definitionDto.getValue())) {
                                    taskCriteria.and("syncType").is(TaskDto.SYNC_TYPE_MIGRATE);
                                } else {
                                    taskCriteria.and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC);
                                }
                                break;
                            case api:
                                metadataCriteria.and("_id").is("1231231231");
                                taskCriteria.and("_id").is("1231231231");
                                break;
                        }
                    }
                }
            }
        }

        UnionWithOperation taskUnion = UnionWithOperation.unionWith("TaskCollectionObj")
                .pipeline(
                        Aggregation.project("createTime", "_id", "listtags", "syncType", "name", "agentId", "is_deleted"),
                        Aggregation.match(taskCriteria)
                );

        UnionWithOperation apiUnion = UnionWithOperation.unionWith("Modules")
                .pipeline(
                        Aggregation.project("createTime", "_id", "listtags", "name", "apiType", "tableName", "status"),
                        Aggregation.match(apiCriteria)
                );
        MatchOperation match = Aggregation.match(metadataCriteria);
        ProjectionOperation project = Aggregation.project("createTime", "_id", "listtags", "meta_type", "original_name"
                , "source", "syncType", "name", "agentId", "apiType", "tableName", "comment", "desc");
        LimitOperation limitOperation = Aggregation.limit(param.getPageSize());
        SkipOperation skipOperation = Aggregation.skip((long) (param.getPage() - 1) * param.getPageSize());
        SortOperation sortOperation = Aggregation.sort(Sort.Direction.DESC, "createTime");

        long count1 = metadataInstancesService.count(new Query(metadataCriteria), user);
        long count2 = taskRepository.count(new Query(taskCriteria), user);
        long count3 = modulesService.count(new Query(apiCriteria), user);
        long total = count1 + count2 + count3;
        Aggregation aggregation = Aggregation.newAggregation(match, taskUnion, apiUnion, project, sortOperation, skipOperation, limitOperation);
        AggregationResults<UnionQueryResult> results = metaDataRepository.getMongoOperations().aggregate(aggregation, "MetadataInstances", UnionQueryResult.class);
        List<UnionQueryResult> unionQueryResults = results.getMappedResults();
        List<DataDirectoryDto> items = unionQueryResults.stream().map(this::convertToDataDirectory).collect(Collectors.toList());

        page.setItems(items);
        page.setTotal(total);
        return page;
    }




    private List<String> objCategoryFilterList() {
        return Arrays.stream(DataObjCategoryEnum.values()).map(Enum::name).collect(Collectors.toList());
    }

    private List<String> objTypeFilterList(UserDetail user) {
        Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true)
                .and("meta_type").is("table");
        Query query = new Query(criteria);
        query.fields().include("meta_type");
        List<String> objTypes = metaDataRepository.findDistinct(query, "meta_type", user, String.class);
        objTypes.remove("database");
        objTypes.add(TaskDto.SYNC_TYPE_MIGRATE);
        objTypes.add(TaskDto.SYNC_TYPE_SYNC);

        return objTypes;
    }

    private List<String> sourceCategoryFilterList() {
        return Arrays.stream(DataSourceCategoryEnum.values()).map(Enum::name).collect(Collectors.toList());

    }

    private List<String> sourceTypeFilterList(UserDetail user) {
        Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true)
                .and("meta_type").is("table");
        Query query = new Query(criteria);
        query.fields().include("source.database_type");
        List<String> sourceTypes = metaDataRepository.findDistinct(query, "source.database_type", user, String.class);

        Criteria criteriaTask = Criteria.where("is_deleted").ne(true)
                .and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)
                .and("agentId").exists(true);
        Query query1 = new Query(criteriaTask);
        query1.fields().include("agentId");
        List<String> taskSourceTypes = taskRepository.findDistinct(query1, "agentId", user, String.class);
        sourceTypes.addAll(taskSourceTypes);
        sourceTypes = sourceTypes.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
        return sourceTypes;
    }


    public void updateListTags(List<TagBindingParam> tagBindingParams, List<String> tagIds, UserDetail user) {
        Criteria criteriaTags = Criteria.where("_id").in(tagIds);
        Query query = new Query(criteriaTags);
        List<MetadataDefinitionDto> all = metadataDefinitionService.findAll(query);
        List<Tag> allTags = all.stream().map(s -> new Tag(s.getId().toHexString(), s.getValue())).collect(Collectors.toList());


        for (TagBindingParam tagBindingParam : tagBindingParams) {

            com.tapdata.tm.base.dto.Field field = new com.tapdata.tm.base.dto.Field();
            field.put("listtags", true);
            switch (tagBindingParam.getObjCategory()) {
                case storage:
                    MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findById(MongoUtils.toObjectId(tagBindingParam.getId()), field);
                    Update update = getUpdate(allTags, metadataInstancesDto.getListtags(), false);
                    metadataInstancesService.updateById(MongoUtils.toObjectId(tagBindingParam.getId()), update, user);
                    break;
                case job:
                    TaskCollectionObjDto taskDto = taskService.findById(MongoUtils.toObjectId(tagBindingParam.getId()), field);
                    Update updateJob = getUpdate(allTags, taskDto.getListtags(), false);
                    taskService.updateById(MongoUtils.toObjectId(tagBindingParam.getId()), updateJob, user);
                    break;
                case api:
                    ModulesDto modulesDto = modulesService.findById(MongoUtils.toObjectId(tagBindingParam.getId()), field);
                    Update updateModules = getUpdate(allTags, modulesDto.getListtags(), false);
                    modulesService.updateById(MongoUtils.toObjectId(tagBindingParam.getId()), updateModules, user);
                    break;
                default:
                    break;
            }
        }

    }

    public void addListTags(List<TagBindingParam> tagBindingParams,  List<String> tagIds, UserDetail user, boolean add) {
        Criteria criteriaTags = Criteria.where("_id").in(tagIds);
        Query query = new Query(criteriaTags);
        List<MetadataDefinitionDto> all = metadataDefinitionService.findAll(query);
        List<Tag> allTags = all.stream().map(s -> new Tag(s.getId().toHexString(), s.getValue())).collect(Collectors.toList());


        for (TagBindingParam tagBindingParam : tagBindingParams) {

            com.tapdata.tm.base.dto.Field field = new com.tapdata.tm.base.dto.Field();
            field.put("listtags", true);
            switch (tagBindingParam.getObjCategory()) {
                case storage:
                    MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findById(MongoUtils.toObjectId(tagBindingParam.getId()), field);
                    Update update = getUpdate(allTags, metadataInstancesDto.getListtags(), add);
                    metadataInstancesService.updateById(MongoUtils.toObjectId(tagBindingParam.getId()), update, user);
                    break;
                case job:
                    TaskCollectionObjDto taskDto = taskService.findById(MongoUtils.toObjectId(tagBindingParam.getId()), field);
                    Update updateJob = getUpdate(allTags, taskDto.getListtags(), add);
                    taskService.updateById(MongoUtils.toObjectId(tagBindingParam.getId()), updateJob, user);
                    break;
                case api:
                    ModulesDto modulesDto = modulesService.findById(MongoUtils.toObjectId(tagBindingParam.getId()), field);
                    Update updateModules = getUpdate(allTags, modulesDto.getListtags(), add);
                    modulesService.updateById(MongoUtils.toObjectId(tagBindingParam.getId()), updateModules, user);
                    break;
                default:
                    break;
            }
        }
    }

    @NotNull
    private static Update getUpdate(List<Tag> allTags, List<Tag> listtags, boolean add) {
        if (listtags == null) {
            listtags = new ArrayList<>();
        }
        for (Tag allTag : allTags) {
            if (add) {
                if (!listtags.contains(allTag)) {
                    listtags.add(allTag);
                }
            } else {
                listtags.remove(allTag);
            }
        }
        return Update.update("listtags", listtags);
    }

    @Override
    public void addObjCount(List<MetadataDefinitionDto> tagDtos, UserDetail user) {
        Query query = new Query();
        query.fields().include("_id", "parent_id", "item_type", "linkId");
        List<MetadataDefinitionDto> allDto = metadataDefinitionService.findAllDto(new Query(), user);
        Map<String, List<MetadataDefinitionDto>> parentMap = allDto.stream().filter(s->StringUtils.isNotBlank(s.getParent_id()))
                .collect(Collectors.groupingBy(MetadataDefinitionDto::getParent_id));

        Map<ObjectId, MetadataDefinitionDto> metadataDefinitionDtoMap = allDto.stream().collect(Collectors.toMap(BaseDto::getId, s -> s));
        Criteria criteria1 = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true)
                .and("meta_type").is("table")
                .and("source._id").ne(null);
        MatchOperation match = Aggregation.match(criteria1);
        GroupOperation g = Aggregation.group("source._id").count().as("count");


        Aggregation aggregation = Aggregation.newAggregation(match, g);
        AggregationResults<GroupMetadata> metadataInstances = taskRepository.getMongoOperations().aggregate(aggregation, "MetadataInstances", GroupMetadata.class);
        List<GroupMetadata> mappedResults = metadataInstances.getMappedResults();

        final Map<String, Long> connectMap;
        if (CollectionUtils.isNotEmpty(mappedResults)) {
            connectMap = mappedResults.stream().collect(Collectors.toMap(GroupMetadata::get_id, GroupMetadata::getCount, (m1, m2) -> m1));
        } else {
            connectMap = new HashMap<>();
        }


        Criteria criteriaSyncTask = Criteria.where("is_deleted").ne(true)
                .and("syncType").is(TaskDto.SYNC_TYPE_SYNC)
                .and("agentId").exists(true);
        boolean isRoot = user.isRoot();
        if (!isRoot) {
            criteriaSyncTask.and("user_id").is(user.getUserId());
        }
        MatchOperation matchTask = Aggregation.match(criteriaSyncTask);
        GroupOperation gTask = Aggregation.group("user_id").count().as("count");


        Aggregation aggregationTask = Aggregation.newAggregation(matchTask, gTask);
        AggregationResults<GroupMetadata> tasks = taskRepository.getMongoOperations().aggregate(aggregationTask, "TaskCollectionObj", GroupMetadata.class);
        List<GroupMetadata> TaskMappedResults = tasks.getMappedResults();

        final Map<String, Long> syncTaskMap;
        if (CollectionUtils.isNotEmpty(TaskMappedResults)) {
            syncTaskMap = TaskMappedResults.stream().collect(Collectors.toMap(GroupMetadata::get_id, GroupMetadata::getCount, (m1, m2) -> m1));
        } else {
            syncTaskMap = new HashMap<>();
        }


        Criteria criteriaMigrateTask = Criteria.where("is_deleted").ne(true)
                .and("syncType").is(TaskDto.SYNC_TYPE_MIGRATE)
                .and("agentId").exists(true);
        if (!isRoot) {
            criteriaMigrateTask.and("user_id").is(user.getUserId());
        }
        MatchOperation matchMigrateTask = Aggregation.match(criteriaMigrateTask);
        GroupOperation gMigrateTask = Aggregation.group("user_id").count().as("count");


        Aggregation aggregationMigrateTask = Aggregation.newAggregation(matchMigrateTask, gMigrateTask);
        AggregationResults<GroupMetadata> migrateTasks = taskRepository.getMongoOperations().aggregate(aggregationMigrateTask, "TaskCollectionObj", GroupMetadata.class);
        List<GroupMetadata> TaskMigrateMappedResults = migrateTasks.getMappedResults();

        final Map<String, Long> migrateTaskMap;
        if (CollectionUtils.isNotEmpty(TaskMappedResults)) {
            migrateTaskMap = TaskMigrateMappedResults.stream().collect(Collectors.toMap(GroupMetadata::get_id, GroupMetadata::getCount, (m1, m2) -> m1));
        } else {
            migrateTaskMap = new HashMap<>();
        }


        tagDtos.parallelStream().forEach(tagDto -> {
                    try {
                        long count = 0;
                        MetadataDefinitionDto definitionDto = metadataDefinitionDtoMap.get(tagDto.getId());
                        List<String> itemTypes = definitionDto.getItemType();
                        boolean isDefault = itemTypes.contains("default");

                        List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(null, definitionDto, parentMap);

                        if (!isDefault) {
                            Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                                    .and("taskId").exists(false)
                                    .and("is_deleted").ne(true)
                                    .and("meta_type").is("table");

                            Criteria criteriaTask1 = Criteria.where("is_deleted").ne(true)
                                    .and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)
                                    .and("agentId").exists(true);

                            Criteria apiCriteria = Criteria.where("status").is("active");



                            List<String> tagIds = andChild.stream().map(t->t.getId().toHexString()).collect(Collectors.toList());

                            criteria.and("listtags.id").in(tagIds);
                            criteriaTask1.and("listtags.id").in(tagIds);
                            apiCriteria.and("listtags.id").in(tagIds);
                            long count1 = metadataInstancesService.count(new Query(criteria), user);
                            long count2 = taskRepository.count(new Query(criteriaTask1), user);
                            long count3 = modulesService.count(new Query(apiCriteria), user);
                            count = count1 + count2 + count3;
                        } else {
                            if (!tagDto.getValue().equals("Root")) {
                                DataObjCategoryEnum defaultObjEnum = getDefaultObjEnum(metadataDefinitionDtoMap, tagDto.getId());
                                switch (defaultObjEnum) {
                                    case storage:
                                        List<String> linkIds = andChild.stream().map(MetadataDefinitionDto::getLinkId).filter(Objects::nonNull).collect(Collectors.toList());
                                        for (String linkId : linkIds) {
                                            count += connectMap.getOrDefault(linkId, 0L);
                                        }
                                        break;
                                    case job:
                                        for (MetadataDefinitionDto metadataDefinitionDto : andChild) {
                                            if (metadataDefinitionDto.getValue().equals(TaskDto.SYNC_TYPE_MIGRATE)) {
                                                count += migrateTaskMap.getOrDefault(tagDto.getUserId(), 0L);
                                            }
                                            if (metadataDefinitionDto.getValue().equals(TaskDto.SYNC_TYPE_SYNC)) {
                                                count += syncTaskMap.getOrDefault(tagDto.getUserId(), 0L);
                                            }
                                        }
                                        break;
                                    case api:
                                        Criteria apiCriteria = Criteria.where("status").is("active").and("is_deleted").ne(true);
                                        if (isRoot) {
                                            apiCriteria.and("user_id").is(tagDto.getUserId());
                                        }
                                        count = modulesService.count(new Query(apiCriteria), user);
                                        break;
                                }
                            } else {
                                List<String> linkIds = andChild.stream().map(MetadataDefinitionDto::getLinkId).filter(Objects::nonNull).collect(Collectors.toList());
                                for (String linkId : linkIds) {
                                    count += connectMap.getOrDefault(linkId, 0L);
                                }
                                for (MetadataDefinitionDto metadataDefinitionDto : andChild) {
                                    if (metadataDefinitionDto.getValue().equals(TaskDto.SYNC_TYPE_MIGRATE)) {
                                        count += migrateTaskMap.getOrDefault(tagDto.getUserId(), 0L);
                                    }
                                    if (metadataDefinitionDto.getValue().equals(TaskDto.SYNC_TYPE_SYNC)) {
                                        count += syncTaskMap.getOrDefault(tagDto.getUserId(), 0L);
                                    }
                                }
                                Criteria apiCriteria = Criteria.where("status").is("active").and("is_deleted").ne(true);;
                                count += modulesService.count(new Query(apiCriteria), user);
                            }


                        }

                        tagDto.setObjCount(count);

                    } catch (Exception e) {
                        log.warn("count stat failed, value = {}", tagDto.getValue());
                    }
                }
        );
    }


    private DataObjCategoryEnum getDefaultObjEnum(Map<ObjectId, MetadataDefinitionDto> metadataDefinitionDtoMap, ObjectId id) {
        if (metadataDefinitionDtoMap == null) {
            metadataDefinitionDtoMap = new HashMap<>();
        }
        MetadataDefinitionDto tagDto = metadataDefinitionDtoMap.get(id);
        if (tagDto == null) {
            tagDto = metadataDefinitionService.findById(id);
        }
        List<String> itemType = tagDto.getItemType();
        if (itemType.contains("storage")) {
            return DataObjCategoryEnum.storage;
        } else if (itemType.contains("job")) {
            return DataObjCategoryEnum.job;
        } else if (itemType.contains("apis")) {
            return DataObjCategoryEnum.api;
        } else {
            String parent_id = tagDto.getParent_id();
            if (parent_id == null) {
                return DataObjCategoryEnum.storage;
            }
            return getDefaultObjEnum(metadataDefinitionDtoMap, MongoUtils.toObjectId(parent_id));
        }
    }


    private String getConnectInfo(SourceDto source, String name) {
        if (source == null) {
            return null;
        }

        StringBuilder ipAndPort = new StringBuilder();



        Object config = source.getConfig();
        Map config1 = (Map) config;
        Object isUri = config1.get("isUri");
        if (source.getDatabase_type().toLowerCase(Locale.ROOT).contains("mongo") && isUri != null && (boolean) isUri) {
            String uri1 = (String) config1.get("uri");
            if (StringUtils.isNotBlank(uri1)) {
                ConnectionString connectionString = new ConnectionString(uri1);
                List<String> hosts = connectionString.getHosts();
                if (CollectionUtils.isNotEmpty(hosts)) {
                    for (String host : hosts) {
                        ipAndPort.append(host).append(";");
                    }
                    ipAndPort = new StringBuilder(ipAndPort.substring(0, ipAndPort.length() -1));
                }
            }
        } else if (source.getDatabase_type().toLowerCase(Locale.ROOT).contains("activemq")) {
            Object brokerURL = config1.get("brokerURL");
            if (brokerURL instanceof String) {
                String ipPort = ((String) brokerURL).substring(6);
                ipAndPort.append(ipPort);
            }


        }  else if (source.getDatabase_type().toLowerCase(Locale.ROOT).contains("kafka")) {
            Object nameSrvAddr = config1.get("nameSrvAddr");
            if (nameSrvAddr instanceof String) {
                ipAndPort.append(nameSrvAddr);
            }


        } else {
            Object host = config1.get("host");
            Object port = config1.get("port");
            Object database = config1.get("database");
            if (host == null) {
                host = config1.get("mqHost");
            }
            if (port == null) {
                port = config1.get("mqPort");
            }
            if (StringUtils.isNotBlank((String)host)) {
                ipAndPort = new StringBuilder(host.toString());
                if (port != null) {
                    ipAndPort.append(":").append(port);
                }
            } else {
                ipAndPort = new StringBuilder();
            }

            if (database != null) {
                ipAndPort.append("/").append(database);
            }
        }

        if (StringUtils.isNotBlank(name)) {
            return ipAndPort + "/" + name;
        } else {
            return ipAndPort.toString();
        }
    }

    public String tapTypeString(String tapType) {
        Map map = JsonUtil.parseJson(tapType, Map.class);
        byte type = ((Double)map.get("type")).byteValue();
        switch (type) {
            case TapType.TYPE_ARRAY:
                return "数组";
            case TapType.TYPE_BINARY:
                return "字节数组";
            case TapType.TYPE_BOOLEAN:
                return "布尔值";
            case TapType.TYPE_DATE:
                return "日期";
            case TapType.TYPE_DATETIME:
                return "日期时间";
            case TapType.TYPE_MAP:
                return "映射";
            case TapType.TYPE_NUMBER:
                return "数值";
            case TapType.TYPE_STRING:
                return "字符串";
            case TapType.TYPE_TIME:
                return "时间";
            case TapType.TYPE_YEAR:
                return "日期（年）";
            default:
                return "未知";
        }
    }

    private DataDiscoveryDto convertToDataDiscovery(UnionQueryResult unionQueryResult) {
        DataDiscoveryDto dataDiscoveryDto = new DataDiscoveryDto();
        dataDiscoveryDto.setId(unionQueryResult.get_id().toHexString());
        List listtagsOld = unionQueryResult.getListtags();
        if (StringUtils.isNotBlank(unionQueryResult.getMeta_type())) {
            dataDiscoveryDto.setCategory(DataObjCategoryEnum.storage);
            dataDiscoveryDto.setType(unionQueryResult.getMeta_type());
            dataDiscoveryDto.setSourceCategory(DataSourceCategoryEnum.connection);
            dataDiscoveryDto.setSourceType(unionQueryResult.getSource() != null ? unionQueryResult.getSource().getDatabase_type() : null);
            dataDiscoveryDto.setSourceInfo(getConnectInfo(unionQueryResult.getSource(), unionQueryResult.getOriginal_name()));
            dataDiscoveryDto.setName(unionQueryResult.getOriginal_name());
        } else if (StringUtils.isNotBlank(unionQueryResult.getSyncType())) {
            dataDiscoveryDto.setCategory(DataObjCategoryEnum.job);
            dataDiscoveryDto.setType(unionQueryResult.getSyncType());
            dataDiscoveryDto.setSourceCategory(DataSourceCategoryEnum.pipe);
            dataDiscoveryDto.setSourceType(unionQueryResult.getAgentId());
            // TODO 查询woker表  得到引擎的地址跟端口
            dataDiscoveryDto.setSourceInfo(unionQueryResult.getSourceInfo());
            dataDiscoveryDto.setName(unionQueryResult.getName());
        } else {
            dataDiscoveryDto.setCategory(DataObjCategoryEnum.api);
            dataDiscoveryDto.setType(unionQueryResult.getApiType());
            dataDiscoveryDto.setSourceCategory(DataSourceCategoryEnum.server);
            dataDiscoveryDto.setName(unionQueryResult.getName());
            dataDiscoveryDto.setSourceInfo(unionQueryResult.getSourceInfo());
            dataDiscoveryDto.setSourceType("API Server");
        }

        if (listtagsOld != null) {
            String json = JsonUtil.toJsonUseJackson(listtagsOld);
            List<Tag> tags = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<Tag>>() {
            });
            dataDiscoveryDto.setListtags(tags);
        }
        List<Tag> listtags = dataDiscoveryDto.getListtags();
        if (CollectionUtils.isNotEmpty(listtags)) {
            List<ObjectId> ids = listtags.stream().map(t->MongoUtils.toObjectId(t.getId())).collect(Collectors.toList());
            List<MetadataDefinitionDto> andParents = metadataDefinitionService.findAndParent(null, ids);
            List<Tag> allTags = andParents.stream().map(s -> new Tag(s.getId().toHexString(), s.getValue())).collect(Collectors.toList());
            dataDiscoveryDto.setAllTags(allTags);
        }
        return dataDiscoveryDto;
    }


    private DataDirectoryDto convertToDataDirectory(UnionQueryResult unionQueryResult) {
        DataDirectoryDto dataDirectoryDto = new DataDirectoryDto();
        dataDirectoryDto.setId(unionQueryResult.get_id().toHexString());

        if (StringUtils.isNotBlank(unionQueryResult.getMeta_type())) {
            dataDirectoryDto.setType(unionQueryResult.getMeta_type());
            dataDirectoryDto.setSourceType(unionQueryResult.getSource() == null ? null : unionQueryResult.getSource().getDatabase_type());
            dataDirectoryDto.setName(unionQueryResult.getOriginal_name());
            dataDirectoryDto.setDesc(unionQueryResult.getComment());
            dataDirectoryDto.setCategory(DataObjCategoryEnum.storage);
            dataDirectoryDto.setListtags(unionQueryResult.getListtags());
        } else if (StringUtils.isNotBlank(unionQueryResult.getSyncType())) {
            dataDirectoryDto.setType(unionQueryResult.getSyncType());
            dataDirectoryDto.setSourceType(unionQueryResult.getAgentId());
            dataDirectoryDto.setName(unionQueryResult.getName());
            dataDirectoryDto.setListtags(unionQueryResult.getListtags());
            dataDirectoryDto.setDesc(unionQueryResult.getDesc());
            dataDirectoryDto.setCategory(DataObjCategoryEnum.job);


        } else {
            dataDirectoryDto.setType(unionQueryResult.getApiType());
            //dataDirectoryDto.setSourceType(unionQueryResult.getAgentId());
            dataDirectoryDto.setName(unionQueryResult.getName());
            dataDirectoryDto.setListtags(unionQueryResult.getListtags());
            dataDirectoryDto.setDesc(unionQueryResult.getDesc());
            dataDirectoryDto.setCategory(DataObjCategoryEnum.api);

        }

        List listtagsOld = unionQueryResult.getListtags();
        if (listtagsOld != null) {
            String json = JsonUtil.toJsonUseJackson(listtagsOld);
            List<Tag> tags = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<Tag>>() {
            });
            dataDirectoryDto.setListtags(tags);
        }
        List<Tag> listtags = dataDirectoryDto.getListtags();
        if (CollectionUtils.isNotEmpty(listtags)) {
            List<ObjectId> ids = listtags.stream().map(t->MongoUtils.toObjectId(t.getId())).collect(Collectors.toList());
            List<MetadataDefinitionDto> andParents = metadataDefinitionService.findAndParent(null, ids);
            List<Tag> allTags = andParents.stream().map(s -> new Tag(s.getId().toHexString(), s.getValue())).collect(Collectors.toList());
            dataDirectoryDto.setAllTags(allTags);
        }
        return dataDirectoryDto;
    }
    @Data
    private static class GroupMetadata{
        private String _id;
        private long count;
    }
}


