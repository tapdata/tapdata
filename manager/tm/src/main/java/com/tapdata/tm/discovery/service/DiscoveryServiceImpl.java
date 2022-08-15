package com.tapdata.tm.discovery.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.*;
import com.tapdata.tm.metaData.repository.MetaDataRepository;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
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
            if (!param.getCategory().equals(DataSourceCategoryEnum.connection.name())) {
                return page;
            }
        }

        Criteria criteria = Criteria.where("sourceType").is(SourceTypeEnum.SOURCE.name())
                .and("taskId").exists(false)
                .and("is_deleted").ne(true);
        if (StringUtils.isNotBlank(param.getType())) {
            criteria.and("meta_type").is(param.getType());
        }
        if (StringUtils.isNotBlank(param.getSourceType())) {
            criteria.and("source.database_type").is(param.getSourceType());
        }



        if (StringUtils.isNotBlank(param.getQueryKey())) {
            criteria.orOperator(
                    Criteria.where("originalName").regex(param.getQueryKey()),
                    Criteria.where("name").regex(param.getQueryKey()),
                    Criteria.where("comment").regex(param.getQueryKey()),
                    Criteria.where("source.database_name").regex(param.getQueryKey()),
                    Criteria.where("alias_name").regex(param.getQueryKey()));
        }

        if (StringUtils.isNotBlank(param.getTagId())) {
            List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.of(MongoUtils.toObjectId(param.getTagId())));
            List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());
            criteria.and("listtags.id").in(tagIds);
        }

        Query query = new Query(criteria);

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
            dto.setName(metadataInstancesDto.getName());
            dto.setSourceCategory(DataSourceCategoryEnum.connection);
            dto.setId(metadataInstancesDto.getId().toHexString());
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
        dto.setCategory(DataObjCategoryEnum.storage);
        dto.setType(metadataInstancesDto.getMetaType());
        dto.setSourceCategory(DataSourceCategoryEnum.connection);
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
                discoveryFieldDto.setForeignKey(field.getForeignKey());

                discoveryFieldDto.setIndex(indexNames.contains(field.getFieldName()));

                if (field.getIsNullable() != null && field.getIsNullable() instanceof String) {
                    discoveryFieldDto.setNotNull("YES".equals(field.getIsNullable()));
                } else if (!(field.getIsNullable() instanceof Boolean)) {
                    discoveryFieldDto.setNotNull(!(boolean)field.getIsNullable());
                }
                discoveryFieldDto.setDefaultValue(field.getDefaultValue());
                //discoveryFieldDto.setBusinessName();
                //discoveryFieldDto.setBusinessType();
                //discoveryFieldDto.setBusinessDesc();

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
                default:
                    break;
            }
        }
        return returnMap;
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
                .and("is_deleted").ne(true);
        if (StringUtils.isNotBlank(param.getSourceType())) {
            criteria.and("source.database_type").is(param.getSourceType());
        }

        if (StringUtils.isNotBlank(param.getQueryKey())) {
            Criteria.where("name").regex(param.getQueryKey());
        }

        if (StringUtils.isNotBlank(param.getTagId())) {
            List<MetadataDefinitionDto> andChild = metadataDefinitionService.findAndChild(Lists.of(MongoUtils.toObjectId(param.getTagId())));
            List<ObjectId> tagIds = andChild.stream().map(BaseDto::getId).collect(Collectors.toList());
            criteria.and("listtags.id").in(tagIds);
        }

        Query query = new Query(criteria);

        long count = metadataInstancesService.count(query, user);

        query.skip((long) (param.getPage() - 1) * param.getPageSize());
        query.limit(param.getPageSize());
        List<MetadataInstancesDto> allDto = metadataInstancesService.findAllDto(query, user);

        List<DataDirectoryDto> items = new ArrayList<>();
        for (MetadataInstancesDto metadataInstancesDto : allDto) {
            DataDirectoryDto dto = new DataDirectoryDto();

            dto.setId(metadataInstancesDto.getId().toHexString());
            dto.setName(metadataInstancesDto.getName());
            dto.setType(metadataInstancesDto.getMetaType());
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
                .and("is_deleted").ne(true);
        Query query = new Query(criteria);
        query.fields().include("meta_type");
        List<String> objTypes = metaDataRepository.findDistinct(query, "meta_type", user, String.class);
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
                .and("is_deleted").ne(true);
        Query query = new Query(criteria);
        query.fields().include("source.database_type");
        List<String> sourceTypes = metaDataRepository.findDistinct(query, "source.database_type", user, String.class);

        Criteria criteriaTask = Criteria.where("is_deleted").ne(true)
                .and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)
                .and("agentId").exists(true);
        query.fields().include("agentId");
        List<String> taskSourceTypes = taskRepository.findDistinct(query, "agentId", user, String.class);
        sourceTypes.addAll(taskSourceTypes);
        return sourceTypes;
    }
}
