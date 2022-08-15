package com.tapdata.tm.discovery.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.discovery.bean.*;

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


    Map<ObjectFilterEnum, List<String>> filterList(List<ObjectFilterEnum> filterTypes, UserDetail user);

    Page<DataDirectoryDto> findDataDirectory(DirectoryQueryParam param, UserDetail user);
}
