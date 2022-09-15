package com.tapdata.tm.discovery.service;

import com.mongodb.ConnectionString;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.*;
import com.tapdata.tm.metaData.repository.MetaDataRepository;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.entity.MetadataInstancesEntity;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.entity.schema.type.*;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
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
@Setter(onMethod_ = {@Autowired})
public class DiscoveryServiceImpl implements DiscoveryService {

    private MetadataInstancesService metadataInstancesService;

    private MetadataInstancesRepository metaDataRepository;
    private TaskRepository taskRepository;

    private MetadataDefinitionService metadataDefinitionService;

    /**
     * 查询对象概览列表
     *
     * @param param
     * @return
     */

    @Override
    public Page<DataDiscoveryDto> find(DiscoveryQueryParam param, UserDetail user) {
        if (param.getPage() == null) {
            param.setPage(1);
        }

        if (param.getPageSize() == null) {
            param.setPageSize(20);
        }

        Page<DataDiscoveryDto> page = new Page<>();
        page.setItems(Lists.of());
        page.setTotal(0);


        if (StringUtils.isNotBlank(param.getCategory())) {
            if (!param.getCategory().equals(DataObjCategoryEnum.storage.name())) {
                return page;
            }
        }
        if (StringUtils.isNotBlank(param.getSourceCategory())) {
            if (!param.getSourceCategory().equals(DataSourceCategoryEnum.connection.name())) {
                return page;
            }
        }

        Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true);
        if (StringUtils.isNotBlank(param.getType())) {
            criteria.and("meta_type").is(param.getType());
        } else {
            criteria.and("meta_type").ne("database");
        }
        if (StringUtils.isNotBlank(param.getSourceType())) {
            criteria.and("source.database_type").is(param.getSourceType());
        }



        if (StringUtils.isNotBlank(param.getQueryKey())) {
            String queryKey = param.getQueryKey();
            queryKey = MongoUtils.replaceLike(queryKey);
            criteria.orOperator(
                    Criteria.where("originalName").regex(queryKey,"i"),
                    Criteria.where("name").regex(queryKey,"i"),
                    Criteria.where("comment").regex(queryKey,"i"),
                    Criteria.where("source.database_name").regex(queryKey,"i"),
                    Criteria.where("source.name").regex(queryKey,"i"),
                    Criteria.where("alias_name").regex(queryKey,"i"));
        }

        if (StringUtils.isNotBlank(param.getTagId())) {
            List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.of(MongoUtils.toObjectId(param.getTagId())));
            List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());
            criteria.and("listtags.id").in(tagIds);
        }



//        if (StringUtils.isNotBlank(param.getItemType())) {
//            List<String> types = new ArrayList<>();
//            if (ItemTypeEnum.resource.name().equals(param.getItemType())) {
//                types.add(DataObjCategoryEnum.table.name());
//                types.add(DataObjCategoryEnum.api.name());
//            } else {
//                types.add(DataObjCategoryEnum.job.name());
//            }
//
//            if (StringUtils.isNotBlank(param.getCategory())) {
//                if (!types.contains(param.getCategory())) {
//                    return page;
//                } else {
//                    types.clear();
//                }
//            }
//        }

        Query query = new Query(criteria);
        query.with(Sort.by(Sort.Direction.DESC, "createTime"));

        long count = metadataInstancesService.count(query, user);

        query.skip((long) (param.getPage() - 1) * param.getPageSize());
        query.limit(param.getPageSize());
        List<MetadataInstancesDto> allDto = metadataInstancesService.findAllDto(query, user);

        List<DataDiscoveryDto> items = new ArrayList<>();
        for (MetadataInstancesDto metadataInstancesDto : allDto) {
            DataDiscoveryDto dto = new DataDiscoveryDto();
            //dto.setRowNum();
            SourceDto source = metadataInstancesDto.getSource();
            if (source != null) {
                dto.setSourceType(source.getDatabase_type());
            }
            dto.setId(metadataInstancesDto.getId().toHexString());
            dto.setCategory(DataObjCategoryEnum.storage);
            dto.setType(metadataInstancesDto.getMetaType());
            dto.setName(metadataInstancesDto.getOriginalName());
            dto.setSourceCategory(DataSourceCategoryEnum.connection);
            dto.setSourceType(metadataInstancesDto.getSource() == null ? null : metadataInstancesDto.getSource().getDatabase_type());
            dto.setSourceInfo(getConnectInfo(metadataInstancesDto));
            //dto.setSourceInfo();
            //dto.setName();
            //dto.setBusinessName();
            //dto.setBusinessDesc();
            dto.setListtags(metadataInstancesDto.getListtags());
            List<Tag> listtags = dto.getListtags();
            if (CollectionUtils.isNotEmpty(listtags)) {
                List<ObjectId> ids = listtags.stream().map(Tag::getId).collect(Collectors.toList());
                List<MetadataDefinitionDto> andParents = metadataDefinitionService.findAndParent(null, ids);
                List<Tag> allTags = andParents.stream().map(s -> new Tag(s.getId(), s.getValue())).collect(Collectors.toList());
                dto.setAllTags(allTags);
            }

            items.add(dto);
        }

        page.setItems(items);
        page.setTotal(count);
        return page;
    }

    public Page<DataDiscoveryDto> find1(DiscoveryQueryParam param, UserDetail user) {
        if (param.getPage() == null) {
            param.setPage(1);
        }

        if (param.getPageSize() == null) {
            param.setPageSize(20);
        }

        Page<DataDiscoveryDto> page = new Page<>();
        page.setItems(Lists.of());
        page.setTotal(0);
        if (StringUtils.isNotBlank(param.getCategory())) {
            if (!param.getCategory().equals(DataObjCategoryEnum.storage.name())) {
                return page;
            }
        }
        if (StringUtils.isNotBlank(param.getSourceCategory())) {
            if (!param.getCategory().equals(DataSourceCategoryEnum.connection.name())) {
                return page;
            }
        }
        Criteria taskCriteria = Criteria.where("is_deleted").ne(true);

        Criteria metadataCriteria = Criteria.where("type1").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true);
        if (StringUtils.isNotBlank(param.getType())) {
            metadataCriteria.and("type").is(param.getType());
            taskCriteria.and("type").is(param.getType());
        } else {
            taskCriteria.and("type").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC);
        }
        if (StringUtils.isNotBlank(param.getSourceType())) {
            metadataCriteria.and("sourceType").is(param.getSourceType());
            taskCriteria.and("sourceType").is(param.getSourceType());
        } else {

            taskCriteria.and("sourceType").exists(true);
        }



        if (StringUtils.isNotBlank(param.getQueryKey())) {
            metadataCriteria.orOperator(
                    Criteria.where("original_name").regex(param.getQueryKey()),
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("comment").regex(param.getQueryKey()),
                    Criteria.where("source.database_name").regex(param.getQueryKey()),
                    Criteria.where("alias_name").regex(param.getQueryKey()));

            taskCriteria.orOperator(
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("desc").regex(param.getQueryKey()));
        }

        if (StringUtils.isNotBlank(param.getTagId())) {
            List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.of(MongoUtils.toObjectId(param.getTagId())));
            List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());
            metadataCriteria.and("listtags.id").in(tagIds);
            taskCriteria.and("listtags.id").in(tagIds);
        }

        UnionWithOperation taskUnion = UnionWithOperation.unionWith("Task")
                .pipeline(
                        Aggregation.project("createTime", "_id", "listtags")
                                .and("syncType").as("type")
                                .and("name").as("name")
                                .and("agentId").as("sourceType")
                                .and("pipe").as("sourceCategory")
                                .and("calculate").as("category"),
                        Aggregation.match(taskCriteria)
                );

        UnionWithOperation metadataUnion = UnionWithOperation.unionWith("MetadataInstances")
                .pipeline(
                        Aggregation.project("createTime", "_id", "listtags")
                                .and("meta_type").as("type")
                                .and("original_name").as("name")
                                .and("source.database_type").as("sourceType")
                                .and("connection").as("sourceCategory")
                                .and("storage").as("category")
                                .and("sourceType").as("type1"),
                        Aggregation.match(metadataCriteria)
                );

        LimitOperation limitOperation = Aggregation.limit(param.getPageSize());
        SkipOperation skipOperation = Aggregation.skip((long) (param.getPage() - 1) * param.getPageSize());
        SortOperation sortOperation = Aggregation.sort(Sort.Direction.DESC, "createTime");
        Aggregation aggregation = Aggregation.newAggregation(taskUnion, metadataUnion, skipOperation, limitOperation, sortOperation);
        long count1 = metadataInstancesService.count(new Query(metadataCriteria), user);
        long count2 = taskRepository.count(new Query(metadataCriteria), user);
        AggregationResults<DataDiscoveryDto> results = metaDataRepository.getMongoOperations().aggregate(aggregation, "sdfasdfsdfsadassdfsaf", DataDiscoveryDto.class);
        List<DataDiscoveryDto> dataDiscoveryDtos = results.getMappedResults();

        for (DataDiscoveryDto dataDiscoveryDto : dataDiscoveryDtos) {
            List<Tag> listtags = dataDiscoveryDto.getListtags();
            if (CollectionUtils.isNotEmpty(listtags)) {
                List<ObjectId> ids = listtags.stream().map(Tag::getId).collect(Collectors.toList());
                List<MetadataDefinitionDto> andParents = metadataDefinitionService.findAndParent(null, ids);
                List<Tag> allTags = andParents.stream().map(s -> new Tag(s.getId(), s.getValue())).collect(Collectors.toList());
                dataDiscoveryDto.setAllTags(allTags);
            }
        }

        page.setTotal(count1 + count2);
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
        MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findById(MongoUtils.toObjectId(id), user);
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
        dto.setId(metadataInstancesDto.getId().toHexString());
        dto.setName(metadataInstancesDto.getOriginalName());
        dto.setCategory(DataObjCategoryEnum.storage);
        dto.setType(metadataInstancesDto.getMetaType());
        dto.setSourceCategory(DataSourceCategoryEnum.connection);
        dto.setSourceInfo(getConnectInfo(metadataInstancesDto));
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
        page.setItems(Lists.of());
        page.setTotal(0);
        Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true)
                .and("meta_type").is("table");
        if (StringUtils.isNotBlank(param.getObjType())) {
           if (!param.getObjType().equals("table")) {
               return page;
            }
        }


        if (StringUtils.isNotBlank(param.getQueryKey())) {
            String queryKey = param.getQueryKey();
            queryKey = MongoUtils.replaceLike(queryKey);
            criteria.orOperator(
                    Criteria.where("originalName").regex(queryKey,"i"),
                    Criteria.where("name").regex(queryKey,"i"),
                    Criteria.where("alias_name").regex(queryKey,"i"));
        }




        if (StringUtils.isNotBlank(param.getTagId())) {
            MetadataDefinitionDto definitionDto = metadataDefinitionService.findById(MongoUtils.toObjectId(param.getTagId()));
            if (definitionDto != null) {
                List<String> itemTypes = definitionDto.getItemType();
                boolean isDefault = itemTypes.contains("default");
                List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.of(MongoUtils.toObjectId(param.getTagId())));
                if (!isDefault) {
                    List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());
                    criteria.and("listtags.id").in(tagIds);
                } else {
                    List<String> linkIds = andChild.stream().map(MetadataDefinitionDto::getLinkId).filter(Objects::nonNull).collect(Collectors.toList());
                    criteria.and("source._id").in(linkIds);
                }
            }
        }

        Query query = new Query(criteria);

        long count = metadataInstancesService.count(query, user);

        query.skip((long) (param.getPage() - 1) * param.getPageSize());
        query.limit(param.getPageSize());
        query.with(Sort.by(Sort.Direction.DESC, "createTime"));
        List<MetadataInstancesDto> allDto = metadataInstancesService.findAllDto(query, user);

        List<DataDirectoryDto> items = new ArrayList<>();
        for (MetadataInstancesDto metadataInstancesDto : allDto) {
            DataDirectoryDto dto = new DataDirectoryDto();

            dto.setId(metadataInstancesDto.getId().toHexString());
            dto.setName(metadataInstancesDto.getOriginalName());
            dto.setType(metadataInstancesDto.getMetaType());
            if (metadataInstancesDto.getSource() != null) {
                dto.setSourceType(metadataInstancesDto.getSource().getDatabase_type());
            }
            dto.setDesc(metadataInstancesDto.getComment());
            List<Tag> listtags = dto.getListtags();
            if (CollectionUtils.isNotEmpty(listtags)) {
                List<ObjectId> ids = listtags.stream().map(Tag::getId).collect(Collectors.toList());
                List<MetadataDefinitionDto> andParents = metadataDefinitionService.findAndParent(null, ids);
                List<Tag> allTags = andParents.stream().map(s -> new Tag(s.getId(), s.getValue())).collect(Collectors.toList());
                dto.setAllTags(allTags);
            }
            items.add(dto);
        }

        page.setItems(items);
        page.setTotal(count);
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
        List<Tag> allTags = all.stream().map(s -> new Tag(s.getId(), s.getValue())).collect(Collectors.toList());


        for (TagBindingParam tagBindingParam : tagBindingParams) {
            com.tapdata.tm.base.dto.Field field = new com.tapdata.tm.base.dto.Field();
            field.put("listtags", true);
            MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findById(MongoUtils.toObjectId(tagBindingParam.getId()), field);
            List<Tag> listtags = metadataInstancesDto.getListtags();
            if (listtags == null) {
                listtags = new ArrayList<>();
            }
            for (Tag allTag : allTags) {
                listtags.remove(allTag);
            }
            switch (tagBindingParam.getObjCategory()) {
                case storage:
                    Update update = Update.update("listtags", listtags);
                    metadataInstancesService.updateById(MongoUtils.toObjectId(tagBindingParam.getId()), update, user);
                    break;
                case job:
                    break;
                case api:
                    break;
                default:
                    break;
            }
        }



    }

    public void addListTags(List<TagBindingParam> tagBindingParams,  List<String> tagIds, UserDetail user) {
        Criteria criteriaTags = Criteria.where("_id").in(tagIds);
        Query query = new Query(criteriaTags);
        List<MetadataDefinitionDto> all = metadataDefinitionService.findAll(query);
        List<Tag> allTags = all.stream().map(s -> new Tag(s.getId(), s.getValue())).collect(Collectors.toList());


        for (TagBindingParam tagBindingParam : tagBindingParams) {
            com.tapdata.tm.base.dto.Field field = new com.tapdata.tm.base.dto.Field();
            field.put("listtags", true);
            MetadataInstancesDto metadataInstancesDto = metadataInstancesService.findById(MongoUtils.toObjectId(tagBindingParam.getId()), field);
            List<Tag> listtags = metadataInstancesDto.getListtags();
            if (listtags == null) {
                listtags = new ArrayList<>();
            }
            for (Tag allTag : allTags) {
                if (!listtags.contains(allTag)) {
                    listtags.add(allTag);
                }
            }
            switch (tagBindingParam.getObjCategory()) {
                case storage:
                    Update update = Update.update("listtags", listtags);
                    metadataInstancesService.updateById(MongoUtils.toObjectId(tagBindingParam.getId()), update, user);
                    break;
                case job:
                    break;
                case api:
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void addObjCount(List<MetadataDefinitionDto> tagDtos, UserDetail user) {
        Query query = new Query();
        query.fields().include("_id", "parent_id", "item_type");
        List<MetadataDefinitionDto> allDto = metadataDefinitionService.findAllDto(new Query(), user);
        Map<String, List<MetadataDefinitionDto>> parentMap = allDto.stream().filter(s->StringUtils.isNotBlank(s.getParent_id()))
                .collect(Collectors.groupingBy(MetadataDefinitionDto::getParent_id));

        Map<ObjectId, MetadataDefinitionDto> metadataDefinitionDtoMap = allDto.stream().collect(Collectors.toMap(BaseDto::getId, s -> s));
        tagDtos.parallelStream().forEach(tagDto -> {
                    Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                            .and("taskId").exists(false)
                            .and("is_deleted").ne(true)
                            .and("meta_type").is("table");

                    Criteria criteriaTask = Criteria.where("is_deleted").ne(true)
                            .and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)
                            .and("agentId").exists(true);

                    MetadataDefinitionDto definitionDto = metadataDefinitionDtoMap.get(tagDto.getId());
                    List<String> itemTypes = definitionDto.getItemType();

                    List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(null, tagDto, parentMap);
                    boolean isDefault = itemTypes.contains("default");
                    if (!isDefault) {
                        List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());

                        criteria.and("listtags.id").in(tagIds);
                        criteriaTask.and("listtags.id").in(tagIds);
                    } else {
                        List<String> linkIds = andChild.stream().map(MetadataDefinitionDto::getLinkId).filter(Objects::nonNull).collect(Collectors.toList());
                        criteria.and("source._id").in(linkIds);
                    }

                    long count = metadataInstancesService.count(new Query(criteria), user);
                    long count1 = 0;//taskRepository.count(new Query(criteria), user);
                    tagDto.setObjCount(count1 + count);
                }
        );
    }


    private String getConnectInfo(MetadataInstancesDto metadataInstancesDto) {
        SourceDto source = metadataInstancesDto.getSource();
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

        return ipAndPort + "/" + metadataInstancesDto.getOriginalName();
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
}
