package com.tapdata.tm.shareCdcTableMapping.service;

import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.shareCdcTableMapping.ShareCdcTableMappingDto;
import com.tapdata.tm.shareCdcTableMapping.entity.ShareCdcTableMappingEntity;
import com.tapdata.tm.shareCdcTableMapping.repository.ShareCdcTableMappingRepository;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface ShareCdcTableMappingService extends IBaseService<ShareCdcTableMappingDto, ShareCdcTableMappingEntity, ObjectId, ShareCdcTableMappingRepository> {
    String SHARE_CDC_KEY_PREFIX = "ExternalStorage_SHARE_CDC_";

    static String genExternalStorageTableName(String connId, String connNamespaceStr, String tableName) {
        String name = SHARE_CDC_KEY_PREFIX;
        if (null != connNamespaceStr) {
            name += String.join("_", connId, String.join(".", connNamespaceStr, tableName)).hashCode();
        } else {
            name += String.join("_", connId, tableName).hashCode();
        }
        return name;
    }

    void genShareCdcTableMappingsByLogCollectorTask(TaskDto logCollectorTask, boolean newTask, UserDetail user);

    Map<String, List<String>> getConnId2TableNames(TaskDto taskDto);

    void setTaskService(com.tapdata.tm.task.service.TaskService taskService);

    void setDataSourceService(DataSourceService dataSourceService);
}
