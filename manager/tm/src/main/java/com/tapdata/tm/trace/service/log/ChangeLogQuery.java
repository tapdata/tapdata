package com.tapdata.tm.trace.service.log;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.commons.trace.ChangeLogCriteria;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.shareCdcTableMapping.entity.ShareCdcTableMappingEntity;
import com.tapdata.tm.shareCdcTableMapping.repository.ShareCdcTableMappingRepository;
import com.tapdata.tm.trace.dto.ChangeLog;
import com.tapdata.tm.trace.param.ChangeLogParam;
import com.tapdata.tm.trace.service.data.TraceDataQueryRpcAdapter;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.MongoUtils;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/27 17:14 Create
 * @description
 */
@Service
@Slf4j
public class ChangeLogQuery {
    @Resource(name = "dataSourceRepository")
    DataSourceRepository dataSourceRepository;
    @Resource(name = "shareCdcTableMappingRepository")
    ShareCdcTableMappingRepository shareCdcTableMappingRepository;
    @Resource(name = "traceDataQueryRpcAdapter")
    TraceDataQueryRpcAdapter traceDataQueryRpcAdapter;
    @Resource(name = "externalStorageServiceImpl")
    ExternalStorageService externalStorageService;


    public ChangeLog query(ChangeLogParam param, UserDetail user) {
        String connectionId = param.getConnectionId();
        if (StringUtils.isBlank(connectionId)) {
            throw new BizException("schema.reload.connectionId");
        }
        String tableName = param.getTable();
        if (StringUtils.isBlank(tableName)) {
            throw new BizException("schema.reload.tableName");
        }
        if (null == param.getStartTime()) {
            throw new BizException("data.trace.log.sTime");
        }
        if (null == param.getEndTime()) {
            throw new BizException("data.trace.log.eTime");
        }
        if (param.getEndTime() - param.getStartTime() > 7*24*60*60*1000) {
            throw new BizException("data.trace.log.time.too.large", 7);
        }
        String shareCDCExternalStorageId = findShareCDCExternalStorageId(connectionId, user);
        if (StringUtils.isBlank(shareCDCExternalStorageId)) {
           return ChangeLog.from(param, null);
        }
        ExternalStorageDto storageDto = externalStorageService.findById(MongoUtils.toObjectId(shareCDCExternalStorageId));
        if (null == storageDto) {
            return ChangeLog.from(param, null);
        }
        String type = storageDto.getType();
        if (!ExternalStorageType.supported(type)) {
            return ChangeLog.from(param, null).msg(MessageUtil.getMessage("data.trace.log.supported", type));
        }

        String tableRingBufferId = findTableRingBufferId(connectionId, tableName, user);
        if (StringUtils.isBlank(tableRingBufferId)) {
            return ChangeLog.from(param, null);
        }
        ChangeLogCriteria criteria = new ChangeLogCriteria();
        criteria.setRingBuffer(tableRingBufferId);
        criteria.setConnectionId(connectionId);
        criteria.setTableName(tableName);
        List<Map<String,Object>> filters =  Optional.ofNullable(param.getQueryConditions()).orElse(new ArrayList<>()).stream().filter(Objects::nonNull).filter(MapUtils::isNotEmpty).toList();
        if(filters.isEmpty()){
            return ChangeLog.from(param, null);
        }
        criteria.setFilters(filters);
        criteria.setExternalStorageId(shareCDCExternalStorageId);
        criteria.setStartTime(param.getStartTime());
        criteria.setEndTime(param.getEndTime());
        criteria.setLimit(param.getLimit());
        criteria.setKey(param.getLastKey());
        return ChangeLog.from(param, traceDataQueryRpcAdapter.queryChangeLog(criteria));
    }

    String findShareCDCExternalStorageId(String connectionId, UserDetail user) {
        ObjectId objectId = MongoUtils.toObjectId(connectionId);
        if (objectId == null) {
            throw new BizException("schema.reload.connectionId.invalid", connectionId);
        }
        Criteria criteriaConnection = Criteria.where("_id").is(objectId)
                .and("shareCdcEnable").is(true);
        Query queryConnection = new Query(criteriaConnection);
        queryConnection.fields().include("shareCDCExternalStorageId");
        DataSourceEntity dataSourceEntity = dataSourceRepository.findOne(queryConnection, user).orElse(null);
        if (dataSourceEntity == null) {
            return null;
        }
        return dataSourceEntity.getShareCDCExternalStorageId();
    }

    String findTableRingBufferId(String connectionId, String tableName, UserDetail user) {
        Criteria criteriaConnection = Criteria.where("connectionId").is(connectionId)
                .and("tableName").is(tableName);
        Query queryConnection = new Query(criteriaConnection);
        queryConnection.fields().include("externalStorageTableName");
        ShareCdcTableMappingEntity dataSourceEntity = shareCdcTableMappingRepository.findOne(queryConnection, user).orElse(null);
        if (dataSourceEntity == null) {
            return null;
        }
        return dataSourceEntity.getExternalStorageTableName();
    }
}
