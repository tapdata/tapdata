package com.tapdata.tm.inspect.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tapdata.manager.common.utils.IOUtils;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.entity.InspectDetailsEntity;
import com.tapdata.tm.inspect.repository.InspectDetailsRepository;
import com.tapdata.tm.inspect.vo.FailTableAndRowsVo;
import com.tapdata.tm.monitoringlogs.param.MonitoringLogExportParam;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipOutputStream;

public abstract class InspectDetailsService extends BaseService<InspectDetailsDto, InspectDetailsEntity, ObjectId, InspectDetailsRepository> {
    public InspectDetailsService(@NonNull InspectDetailsRepository repository) {
        super(repository, InspectDetailsDto.class, InspectDetailsEntity.class);
    }

    public FailTableAndRowsVo totalFailTableAndRows(String inspectResultId) {
        AggregationResults<FailTableAndRowsVo> results = repository.aggregate(Aggregation.newAggregation(
            Aggregation.match(Criteria.where("inspectResultId").is(inspectResultId).and("source").exists(true))
            , Aggregation.group("taskId").count().as("rows")
            , Aggregation.group().count().as("tableTotals").sum("rows").as("rowsTotals")
        ), FailTableAndRowsVo.class);
        List<FailTableAndRowsVo> mappedResults = results.getMappedResults();
        if (!mappedResults.isEmpty()) {
            return mappedResults.get(0);
        }
        return null;
    }


    public void export(InspectDetailsDto inspectDetails, ZipOutputStream stream, UserDetail userDetail, InspectResultService inspectResultService)
            throws IOException {
        JSONArray jsonArray = new JSONArray();
        Map<String, JSONObject> map = new HashMap<>();
        InspectResultDto inspectResultDto = inspectResultService.findById(new ObjectId(inspectDetails.getInspectResultId()), userDetail);
        List<Stats> stats = inspectResultDto.getStats();
        Query query = Query.query(Criteria.where("inspectResultId").is(inspectDetails.getInspectResultId()));
        Sort sort = Sort.by("createTime").descending();
        query.with(sort);
        List<InspectDetailsDto> inspectDetailsDto = findAllDto(query, userDetail);
        inspectDetailsDto.forEach(inspectDetailsDtoTmp -> {
            JSONObject jsonObject = new JSONObject();
            Stats statsTmp = null;
            String taskId = inspectDetailsDtoTmp.getTaskId();
            for (int index = 0; index < stats.size(); index++) {
                statsTmp = stats.get(index);
                if (taskId.equals(statsTmp.getTaskId())) {
                    break;
                }
            }
            if (!"failed".equals(statsTmp.getResult())) {
                return;
            }
            JSONObject jsonObjectTmp = new JSONObject();
            jsonObjectTmp.put("source", inspectDetailsDtoTmp.getSource());
            jsonObjectTmp.put("target", inspectDetailsDtoTmp.getTarget());
            if (!map.containsKey(taskId)) {
                map.put(taskId, jsonObject);

                Source source = statsTmp.getSource();
                jsonObject.put("sourceTableName", source.getTable() + "/" + source.getConnectionName()
                        + "(Row:" + statsTmp.getSourceTotal() + ")");
                Source target = statsTmp.getTarget();
                jsonObject.put("targetTableName", target.getTable() + "/" + target.getConnectionName()
                        + "(Row:" + statsTmp.getTargetTotal() + ")");
                jsonObject.put("checkResult", "failed");
                long count = statsTmp.getTargetTotal() - statsTmp.getSourceTotal();
                if (count < 0) {
                    jsonObject.put("targetCountLess", Math.abs(count));
                } else {
                    jsonObject.put("targetCountMore", count);
                }

                jsonObject.put("tableDiffCount", statsTmp.getSourceOnly() + statsTmp.getTargetOnly() +
                        statsTmp.getRowFailed());
                JSONArray jsonArrayTmp = new JSONArray();
                jsonArrayTmp.add(jsonObjectTmp);
                jsonObject.put("data", jsonArrayTmp);
                jsonArray.add(jsonObject);
            } else {
                jsonObject = map.get(taskId);
                JSONArray jsonArrayData = (JSONArray) jsonObject.get("data");
                jsonArrayData.add(jsonObjectTmp);
            }
        });
        stream.write(jsonArray.toString().getBytes());
    }




}
