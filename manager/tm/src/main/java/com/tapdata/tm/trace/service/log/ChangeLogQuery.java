package com.tapdata.tm.trace.service.log;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.trace.ChangeLogCriteria;
import com.tapdata.tm.commons.trace.ChangeLogData;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.shareCdcTableMapping.entity.ShareCdcTableMappingEntity;
import com.tapdata.tm.shareCdcTableMapping.repository.ShareCdcTableMappingRepository;
import com.tapdata.tm.trace.dto.ChangeLog;
import com.tapdata.tm.trace.param.ChangeLogParam;
import com.tapdata.tm.trace.service.data.TraceDataQueryRpcAdapter;
import com.tapdata.tm.utils.MongoUtils;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Optional;

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
        String shareCDCExternalStorageId = findShareCDCExternalStorageId(connectionId);
        if (StringUtils.isBlank(shareCDCExternalStorageId)) {
           return ChangeLog.from(param, null);
        }
        String tableRingBufferId = findTableRingBufferId(connectionId, tableName);
        if (StringUtils.isBlank(tableRingBufferId)) {
            return ChangeLog.from(param, null);
        }
        ChangeLogCriteria criteria = new ChangeLogCriteria();
        criteria.setRingBuffer(tableRingBufferId);
        criteria.setConnectionId(connectionId);
        criteria.setTableName(tableName);
        criteria.setFilters(Optional.ofNullable(param.getQueryConditions()).orElse(new ArrayList<>()));
        criteria.setExternalStorageId(shareCDCExternalStorageId);
        criteria.setStartTime(param.getStartTime());
        criteria.setEndTime(param.getEndTime());
        criteria.setLimit(param.getLimit());
        criteria.setKey(param.getLastKey());
        //@todo
        return ChangeLog.from(param, traceDataQueryRpcAdapter.queryChangeLog(criteria));
    }

    String findShareCDCExternalStorageId(String connectionId) {
        Criteria criteriaConnection = Criteria.where("_id").is(MongoUtils.toObjectId(connectionId))
                .and("shareCdcEnable").is(true);
        Query queryConnection = new Query(criteriaConnection);
        queryConnection.fields().include("shareCDCExternalStorageId");
        DataSourceEntity dataSourceEntity = dataSourceRepository.findOne(queryConnection).orElse(null);
        if (dataSourceEntity == null) {
            return null;
        }
        return dataSourceEntity.getShareCDCExternalStorageId();
    }

    String findTableRingBufferId(String connectionId, String tableName) {
        Criteria criteriaConnection = Criteria.where("connectionId").is(connectionId)
                .and("tableName").is(tableName);
        Query queryConnection = new Query(criteriaConnection);
        queryConnection.fields().include("externalStorageTableName");
        ShareCdcTableMappingEntity dataSourceEntity = shareCdcTableMappingRepository.findOne(queryConnection).orElse(null);
        if (dataSourceEntity == null) {
            return null;
        }
        return dataSourceEntity.getExternalStorageTableName();
    }
}
