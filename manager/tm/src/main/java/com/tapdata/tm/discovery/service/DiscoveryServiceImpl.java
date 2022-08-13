package com.tapdata.tm.discovery.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.discovery.bean.*;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DiscoveryServiceImpl implements DiscoveryService {
    /**
     * 查询对象概览列表
     *
     * @param param
     * @return
     */
    @Override
    public Page<DataDiscoveryDto> find(DiscoveryQueryParam param) {
        return null;
    }

    /**
     * 查询存储对象预览
     *
     * @param id
     * @return
     */
    @Override
    public Page<Object> storagePreview(String id) {
        return null;
    }

    /**
     * 查询存储对象概览
     *
     * @param id
     * @return
     */
    @Override
    public DiscoveryStorageOverviewDto storageOverview(String id) {
        return null;
    }

    @Override
    public Map<ObjectFilterEnum, List<String>> filterList(List<ObjectFilterEnum> filterTypes) {
        return null;
    }

    @Override
    public List<DataDirectoryDto> findDataDirectory(DirectoryQueryParam param) {
        return null;
    }
}
