package com.tapdata.tm.monitor.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.AggregateMeasurementParam;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.param.SyncStatusStatisticsParam;
import com.tapdata.tm.monitor.vo.SyncStatusStatisticsVo;
import com.tapdata.tm.monitor.vo.TaskMetricsTrendVo;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import com.tapdata.tm.task.bean.TableStatusInfoDto;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;

public interface MeasurementServiceV2 {
    List<MeasurementEntity> find(Query query);

    void addAgentMeasurement(List<SampleRequest> samples);

    Object getSamples(MeasurementQueryParam measurementQueryParam);

    Map<String, Sample> getDifferenceSamples(MeasurementQueryParam.MeasurementQuerySample querySample, long start, long end);

    void aggregateMeasurement(AggregateMeasurementParam param);

    void aggregateMeasurementByGranularity(Map<String, String> queryTags, long start, long end, String granularity);

    void deleteTaskMeasurement(String taskId);

    Long[] countEventByTaskRecord(String taskId, String taskRecordId);

    List<String> findRunTable(String taskId, String taskRecordId);

    Page<TableSyncStaticVo> querySyncStatic(TableSyncStaticDto dto, UserDetail userDetail);

    List<SyncStatusStatisticsVo> queryTableSyncStatusStatistics(SyncStatusStatisticsParam param);

    MeasurementEntity findLastMinuteByTaskId(String taskId);

    Map<String, Sample> findLastMinuteSamplesByTaskIds(List<String> taskIds);

    TaskMetricsTrendVo aggregateTaskMetricsByTaskIds(List<String> taskIds, long startAt, long endAt);

    void queryTableMeasurement(String taskId, TableStatusInfoDto tableStatusInfoDto);

    void cleanRemovedTableMeasurement(String taskId, String taskRecordId, String tableName);
}
