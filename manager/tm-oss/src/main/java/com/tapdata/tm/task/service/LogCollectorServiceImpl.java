package com.tapdata.tm.task.service;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.mongodb.ConnectionString;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.externalStorage.vo.ExternalStorageVo;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.shareCdcTableMapping.ShareCdcTableMappingDto;
import com.tapdata.tm.shareCdcTableMapping.service.ShareCdcTableMappingService;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import com.tapdata.tm.shareCdcTableMetrics.service.ShareCdcTableMetricsService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.param.TableLogCollectorParam;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.UUIDUtil;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author: Zed
 * @Date: 2022/2/15
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class LogCollectorServiceImpl implements LogCollectorService {
    @Override
    public Page<LogCollectorVo> find(Filter filter, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<TaskDto> findSyncTaskById(TaskDto taskDto, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<LogCollectorVo> findByTaskId(String taskId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<LogCollectorVo> findBySubTaskId(String taskId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<LogCollectorVo> findByConnectionName(String name, String connectionName, UserDetail user, int skip, int limit, List<String> sort) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public boolean checkCondition(UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void update(LogCollectorEditVo logCollectorEditVo, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public LogCollectorDetailVo findDetail(String id, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public LogSystemConfigDto findSystemConfig(UserDetail loginUser) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void updateSystemConfig(LogSystemConfigDto logSystemConfigDto, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Boolean checkUpdateConfig(UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Boolean checkUpdateConfig(String connectionId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<Map<String, String>> findTableNames(String taskId, int skip, int limit, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<Map<String, String>> findCallTableNames(String taskId, String callSubId, int skip, int limit, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void logCollector(UserDetail user, TaskDto oldTaskDto) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void startConnHeartbeat(UserDetail user, TaskDto taskDto) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void endConnHeartbeat(UserDetail user, TaskDto taskDto) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void cancelMerge(String taskId, String connectionId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<ShareCdcTableInfo> tableInfos(String taskId, String connectionId, String keyword, Integer page, Integer size, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public Page<ShareCdcTableInfo> excludeTableInfos(String taskId, String connectionId, String keyword, Integer page, Integer size, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void configTables(String taskId, List<TableLogCollectorParam> params, String type, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<ShareCdcConnectionInfo> getConnectionIds(String taskId, UserDetail user) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void clear() {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void removeTask() {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
