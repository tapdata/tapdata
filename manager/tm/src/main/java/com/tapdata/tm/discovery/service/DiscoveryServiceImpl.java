package com.tapdata.tm.discovery.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.*;
import com.tapdata.tm.metaData.repository.MetaDataRepository;
import com.tapdata.tm.metadatainstance.repository.MetadataInstancesRepository;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
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
    /**
     * 查询对象概览列表
     *
     * @param param
     * @return
     */
    @Override
    public Page<DataDiscoveryDto> find(DiscoveryQueryParam param, UserDetail user) {
        return null;
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
        dto.setFieldNum(metadataInstancesDto.getFields().size());
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
        //dto.setName();
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
    public List<DataDirectoryDto> findDataDirectory(DirectoryQueryParam param, UserDetail user) {
        return null;
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
