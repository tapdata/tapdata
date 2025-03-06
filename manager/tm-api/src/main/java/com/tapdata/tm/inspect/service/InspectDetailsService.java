package com.tapdata.tm.inspect.service;

import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.entity.InspectDetailsEntity;
import com.tapdata.tm.inspect.repository.InspectDetailsRepository;
import com.tapdata.tm.inspect.vo.FailTableAndRowsVo;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
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
        InspectResultDto inspectResultDto = inspectResultService.findById(new ObjectId(inspectDetails.getInspectResultId()));
        if (null == inspectResultDto) {
            return;
        }
        List<Stats> stats = inspectResultDto.getStats();
        Query query = Query.query(Criteria.where("inspectResultId").is(inspectDetails.getInspectResultId()));
        Sort sort = Sort.by("createTime").descending();
        query.with(sort);
        List<InspectDetailsDto> inspectDetailsDto = findAll(query);
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
            if(!inspectDetails.getFullField())compareDifferenceFields(inspectDetailsDtoTmp,inspectResultDto);
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
        stream.write(JSONObject.toJSONString(jsonArray, SerializerFeature.WriteMapNullValue).getBytes());
    }

    protected void compareDifferenceFields(InspectDetailsDto inspectDetailsDtoTmp,InspectResultDto inspectResultDto){
        Map<String,Object> source = inspectDetailsDtoTmp.getSource();
        Map<String,Object> target = inspectDetailsDtoTmp.getTarget();
        Map<String,Object> resultSource = new HashMap<>();
        Map<String,Object> resultTarget = new HashMap<>();
        String message = inspectDetailsDtoTmp.getMessage();
        if (StringUtils.isBlank(message)) {
            return;
        }
        if (message.contains("Different fields")) {
            List<String> diffFields = Arrays.stream(message.split(":")[1].split(",")).collect(Collectors.toList());
            diffFields.forEach(diffField -> {
                resultSource.put(diffField, source.get(diffField));
                resultTarget.put(diffField, target.get(diffField));
            });
        } else if (message.contains("Different index")) {
            List<String> diffFiledIndexs = Arrays.stream(message.split(":")[1].split(",")).collect(Collectors.toList());
            List<Stats> statsList = inspectResultDto.getStats();
            if(CollectionUtil.isNotEmpty(statsList)){
                Stats stats = statsList.stream().filter(s -> s.getTaskId().equals(inspectDetailsDtoTmp.getTaskId())).findFirst().orElse(null);
                if(stats != null){
                    List<String> sourceColumns = stats.getSource().getColumns();
                    List<String> targetColumns = stats.getTarget().getColumns();
                    if(CollectionUtil.isNotEmpty(sourceColumns) && CollectionUtil.isNotEmpty(targetColumns)){
                        diffFiledIndexs.forEach(diffFiledIndex -> {
                            String sourceColumn = sourceColumns.get(Integer.parseInt(diffFiledIndex));
                            String targetColumn = targetColumns.get(Integer.parseInt(diffFiledIndex));
                            resultSource.put(sourceColumn, source.get(sourceColumn));
                            resultTarget.put(targetColumn, target.get(targetColumn));
                        });
                    }
                }
            }
        }
        inspectDetailsDtoTmp.setSource(resultSource);
        inspectDetailsDtoTmp.setTarget(resultTarget);
    }





}
