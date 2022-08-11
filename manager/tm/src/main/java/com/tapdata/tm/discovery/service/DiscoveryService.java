package com.tapdata.tm.discovery.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.discovery.bean.DataDiscoveryDto;
import com.tapdata.tm.discovery.bean.DiscoveryQueryParam;
import com.tapdata.tm.discovery.bean.DiscoveryStorageOverviewDto;
import com.tapdata.tm.discovery.bean.DiscoveryStoragePreviewDto;

public interface DiscoveryService {
    /**
     * 查询对象概览列表
     * @param param
     * @return
     */
    Page<DataDiscoveryDto> find(DiscoveryQueryParam param);

    /**
     * 查询存储对象预览
     * @param id
     * @return
     */
    Page<Object> storagePreview(String id);

    /**
     * 查询存储对象概览
     * @param id
     * @return
     */
    DiscoveryStorageOverviewDto storageOverview(String id);
}
