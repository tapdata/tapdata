package com.tapdata.tm.discovery.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.*;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;

import java.util.List;
import java.util.Map;

public interface DiscoveryService {
    /**
     * 查询对象概览列表
     * @param param
     * @return
     */
    Page<DataDiscoveryDto> find(DiscoveryQueryParam param, UserDetail user);

    /**
     * 查询存储对象预览
     * @param id
     * @return
     */
    Page<Object> storagePreview(String id, UserDetail user);

    /**
     * 查询存储对象概览
     * @param id
     * @return
     */
    DiscoveryStorageOverviewDto storageOverview(String id, UserDetail user);
    DiscoveryTaskOverviewDto taskOverview(String id, UserDetail user);
    DiscoveryApiOverviewDto apiOverview(String id, UserDetail user);


    Map<ObjectFilterEnum, List<String>> filterList(List<ObjectFilterEnum> filterTypes, UserDetail user);

    Page<DataDirectoryDto> findDataDirectory(DirectoryQueryParam param, UserDetail user);

    void updateListTags(List<TagBindingParam> tagBindingParams, List<String> tagIds, UserDetail user);

    void addListTags(List<TagBindingParam> tagBindingParams, List<String> tagIds, UserDetail user, boolean add);

    void addObjCount(List<MetadataDefinitionDto> tagDtos, UserDetail user);
}
