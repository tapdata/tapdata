package com.tapdata.tm.task.service.chart;

import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.task.bean.Chart6Vo;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.utils.NumberUtil;
import io.tapdata.common.sample.request.Sample;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class ChartViewService {
    private TaskService taskService;
    private MeasurementServiceV2 measurementServiceV2;

    public List<TaskDto> getViewTaskDtoByUser(UserDetail user) {
        return DataPermissionMenuEnums.MigrateTack.checkAndSetFilter(user, DataPermissionActionEnums.View, () -> getViewTaskDto(user));
    }

    protected List<TaskDto> getViewTaskDto(UserDetail user) {
        Criteria criteria = new Criteria()
                .and(TaskServiceImpl.IS_DELETED).ne(true)
                .and(TaskServiceImpl.SYNC_TYPE).in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)
                .and(TaskServiceImpl.STATUS).nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED)
                //共享缓存的任务设计的有点问题
                .and("shareCache").ne(true);
        Query query = Query.query(criteria);
        query.fields().include(TaskServiceImpl.SYNC_TYPE, TaskServiceImpl.STATUS, "statuses");
        return taskService.findAllDto(query, user);
    }

    public Chart6Vo transmissionOverviewChartData(UserDetail user) {
        List<TaskDto> allDto = getViewTaskDtoByUser(user);
        return transmissionOverviewChartData(allDto);
    }

    public Chart6Vo transmissionOverviewChartData(List<TaskDto> allDto) {
        List<String> ids = allDto.stream()
                .filter(Objects::nonNull)
                .map(a -> a.getId().toHexString())
                .collect(Collectors.toList());

        List<MeasurementEntity> allMeasurements = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(ids)) {
            ids.forEach(id -> {
                MeasurementEntity measurement = measurementServiceV2.findLastMinuteByTaskId(id);
                if (measurement != null) {
                    allMeasurements.add(measurement);
                }
            });
        }

        BigInteger output = BigInteger.ZERO;
        BigInteger input = BigInteger.ZERO;
        BigInteger insert = BigInteger.ZERO;
        BigInteger update = BigInteger.ZERO;
        BigInteger delete = BigInteger.ZERO;

        for (MeasurementEntity allMeasurement : allMeasurements) {
            if (allMeasurement == null) {
                continue;
            }
            List<Sample> samples = allMeasurement.getSamples();
            if (CollectionUtils.isNotEmpty(samples)) {
                Optional<Sample> max = samples.stream().max(Comparator.comparing(Sample::getDate));
                if (max.isPresent()) {
                    Sample sample = max.get();
                    Map<String, Number> vs = sample.getVs();
                    BigInteger inputDdlTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_INPUT_DDL_TOTAL));
                    BigInteger inputInsertTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_INPUT_INSERT_TOTAL));
                    BigInteger inputUpdateTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_INPUT_UPDATE_TOTAL));
                    BigInteger inputDeleteTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_INPUT_DELETE_TOTAL));
                    BigInteger inputOthersTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_INPUT_OTHERS_TOTAL));

                    BigInteger outputDdlTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_OUTPUT_DDL_TOTAL));
                    BigInteger outputInsertTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_OUTPUT_INSERT_TOTAL));
                    BigInteger outputUpdateTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_OUTPUT_UPDATE_TOTAL));
                    BigInteger outputDeleteTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_OUTPUT_DELETE_TOTAL));
                    BigInteger outputOthersTotal = NumberUtil.parseDataTotal(vs.get(MetricCons.SS.VS.F_OUTPUT_OTHERS_TOTAL));
                    output = output.add(outputDdlTotal);
                    output = output.add(outputInsertTotal);
                    output = output.add(outputUpdateTotal);
                    output = output.add(outputDeleteTotal);
                    output = output.add(outputOthersTotal);

                    input = input.add(inputDdlTotal);
                    input = input.add(inputInsertTotal);
                    input = input.add(inputUpdateTotal);
                    input = input.add(inputDeleteTotal);
                    input = input.add(inputOthersTotal);

                    insert = insert.add(inputInsertTotal);
                    update = update.add(inputUpdateTotal);
                    delete = delete.add(inputDeleteTotal);

                }
            }
        }
        return Chart6Vo.builder().outputTotal(output)
                .inputTotal(input)
                .insertedTotal(insert)
                .updatedTotal(update)
                .deletedTotal(delete)
                .build();
    }
}
