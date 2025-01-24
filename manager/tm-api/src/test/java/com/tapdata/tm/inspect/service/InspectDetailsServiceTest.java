package com.tapdata.tm.inspect.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;

import java.io.IOException;
import java.util.*;
import java.util.zip.ZipOutputStream;

import static org.mockito.Mockito.*;

public class InspectDetailsServiceTest {

    @Nested
    class ExportInspectDetails{

        @Test
        void testExport() throws IOException {
            InspectDetailsService inspectDetailsService = mock(InspectDetailsService.class);
            InspectDetailsDto inspectDetails = new InspectDetailsDto();
            String inspectResultId = ObjectId.get().toHexString();
            inspectDetails.setInspectResultId(inspectResultId);
            ZipOutputStream zipOutputStream = mock(ZipOutputStream.class);
            UserDetail userDetail = mock(UserDetail.class);
            InspectResultService inspectResultService = mock(InspectResultService.class);
            InspectResultDto inspectResultDto = new InspectResultDto();
            List<Stats> stats = new ArrayList<>();
            String taskId = ObjectId.get().toHexString();
            Stats stats2 = new Stats();
            stats2.setTaskId(ObjectId.get().toHexString());
            stats.add(stats2);
            Stats stats1= new Stats();
            stats1.setTaskId(taskId);
            stats1.setResult("failed");
            Source source = new Source();
            source.setConnectionName("mysql");
            source.setTable("test");
            Source target = new Source();
            target.setConnectionName("mysql");
            target.setTable("check_test");
            stats1.setSource(source);
            stats1.setTarget(target);
            long targetTotal = 2l;
            stats1.setTargetTotal(targetTotal);
            long sourceTotal = 3l;
            stats1.setSourceTotal(sourceTotal);
            stats1.setSourceOnly(0l);
            stats1.setTargetOnly(1l);
            stats1.setRowFailed(0l);
            stats.add(stats1);

            inspectResultDto.setStats(stats);
            when(inspectResultService.findById(new ObjectId(inspectResultId))).thenReturn(inspectResultDto);
            org.springframework.data.mongodb.core.query.Query query = org.springframework.data.mongodb.core.query.Query.query(Criteria.where("inspectResultId").is(inspectDetails.getInspectResultId()));
            Sort sort = Sort.by("createTime").descending();
            query.with(sort);
            List<InspectDetailsDto> inspectDetailsDto = new ArrayList<>();
            InspectDetailsDto inspectDetailsDto1 = new InspectDetailsDto();
            inspectDetailsDto1.setTaskId(taskId);
            inspectDetailsDto.add(inspectDetailsDto1);
            inspectDetailsDto.add(inspectDetailsDto1);
            when(inspectDetailsService.findAllDto(query,userDetail)).thenReturn(inspectDetailsDto);
            doCallRealMethod().when(inspectDetailsService).export(inspectDetails, zipOutputStream, userDetail, inspectResultService);
            inspectDetailsService.export(inspectDetails, zipOutputStream, userDetail, inspectResultService);

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("sourceTableName", source.getTable() + "/" + source.getConnectionName()
                    + "(Row:" + sourceTotal + ")");
            jsonObject.put("targetTableName", target.getTable() + "/" + target.getConnectionName()
                    + "(Row:" + targetTotal + ")");
            jsonObject.put("checkResult", "failed");
            long count = targetTotal - sourceTotal;
            if (count < 0) {
                jsonObject.put("targetCountLess", Math.abs(count));
            } else {
                jsonObject.put("targetCountMore", count);
            }

            jsonObject.put("tableDiffCount", 1);
            JSONArray jsonArrayTmp = new JSONArray();
            JSONObject jsonObjectTmp = new JSONObject();
            jsonObjectTmp.put("source", inspectDetailsDto1.getSource());
            jsonObjectTmp.put("target", inspectDetailsDto1.getTarget());
            jsonArrayTmp.add(jsonObjectTmp);
            JSONObject jsonObjectTmp1 = new JSONObject();
            jsonObjectTmp1.put("source", inspectDetailsDto1.getSource());
            jsonObjectTmp1.put("target", inspectDetailsDto1.getTarget());
            jsonArrayTmp.add(jsonObjectTmp1);
            jsonObject.put("data", jsonArrayTmp);
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(jsonObject);



            verify(zipOutputStream,times(1)).write(JSONObject.toJSONString(jsonArray, SerializerFeature.WriteMapNullValue).getBytes());

        }


        @Test
        void testExport_fullFiled_false() throws IOException {
            InspectDetailsService inspectDetailsService = mock(InspectDetailsService.class);
            InspectDetailsDto inspectDetails = new InspectDetailsDto();
            String inspectResultId = ObjectId.get().toHexString();
            inspectDetails.setInspectResultId(inspectResultId);
            inspectDetails.setFullField(false);
            ZipOutputStream zipOutputStream = mock(ZipOutputStream.class);
            UserDetail userDetail = mock(UserDetail.class);
            InspectResultService inspectResultService = mock(InspectResultService.class);
            InspectResultDto inspectResultDto = new InspectResultDto();
            List<Stats> stats = new ArrayList<>();
            String taskId = ObjectId.get().toHexString();
            Stats stats2 = new Stats();
            stats2.setTaskId(ObjectId.get().toHexString());
            stats.add(stats2);
            Stats stats1= new Stats();
            stats1.setTaskId(taskId);
            stats1.setResult("failed");
            Source source = new Source();
            source.setConnectionName("mysql");
            source.setTable("test");
            Source target = new Source();
            target.setConnectionName("mysql");
            target.setTable("check_test");
            stats1.setSource(source);
            stats1.setTarget(target);
            long targetTotal = 2l;
            stats1.setTargetTotal(targetTotal);
            long sourceTotal = 3l;
            stats1.setSourceTotal(sourceTotal);
            stats1.setSourceOnly(0l);
            stats1.setTargetOnly(1l);
            stats1.setRowFailed(0l);
            stats.add(stats1);

            inspectResultDto.setStats(stats);
            when(inspectResultService.findById(new ObjectId(inspectResultId))).thenReturn(inspectResultDto);
            org.springframework.data.mongodb.core.query.Query query = org.springframework.data.mongodb.core.query.Query.query(Criteria.where("inspectResultId").is(inspectDetails.getInspectResultId()));
            Sort sort = Sort.by("createTime").descending();
            query.with(sort);
            List<InspectDetailsDto> inspectDetailsDto = new ArrayList<>();
            InspectDetailsDto inspectDetailsDto1 = new InspectDetailsDto();
            Map<String,Object> source1 = new HashMap<>();
            source1.put("id",1);
            source1.put("text",null);
            Map<String,Object> target1 = new HashMap<>();
            target1.put("id",1);
            target1.put("name","test1");
            target1.put("text",null);
            inspectDetailsDto1.setSource(source1);
            inspectDetailsDto1.setTarget(target1);
            inspectDetailsDto1.setTaskId(taskId);
            inspectDetailsDto1.setMessage("Different fields:name");
            inspectDetailsDto.add(inspectDetailsDto1);
            when(inspectDetailsService.findAllDto(query,userDetail)).thenReturn(inspectDetailsDto);
            doCallRealMethod().when(inspectDetailsService).compareDifferenceFields(any(),any());
            doCallRealMethod().when(inspectDetailsService).export(inspectDetails, zipOutputStream, userDetail, inspectResultService);
            inspectDetailsService.export(inspectDetails, zipOutputStream, userDetail, inspectResultService);

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("sourceTableName", source.getTable() + "/" + source.getConnectionName()
                    + "(Row:" + sourceTotal + ")");
            jsonObject.put("targetTableName", target.getTable() + "/" + target.getConnectionName()
                    + "(Row:" + targetTotal + ")");
            jsonObject.put("checkResult", "failed");
            long count = targetTotal - sourceTotal;
            if (count < 0) {
                jsonObject.put("targetCountLess", Math.abs(count));
            } else {
                jsonObject.put("targetCountMore", count);
            }

            jsonObject.put("tableDiffCount", 1);
            JSONArray jsonArrayTmp = new JSONArray();
            JSONObject jsonObjectTmp = new JSONObject();
            Map<String,Object> resultSource = new HashMap<>();
            resultSource.put("name",null);
            Map<String,Object> resultTarget = new HashMap<>();
            resultTarget.put("name","test1");
            jsonObjectTmp.put("source", resultSource);
            jsonObjectTmp.put("target", resultTarget);
            jsonArrayTmp.add(jsonObjectTmp);
            jsonObject.put("data", jsonArrayTmp);
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(jsonObject);



            verify(zipOutputStream,times(1)).write(JSONObject.toJSONString(jsonArray, SerializerFeature.WriteMapNullValue).getBytes());

        }


        @Test
        void testExport_fullFiled_false_index() throws IOException {
            InspectDetailsService inspectDetailsService = mock(InspectDetailsService.class);
            InspectDetailsDto inspectDetails = new InspectDetailsDto();
            String inspectResultId = ObjectId.get().toHexString();
            inspectDetails.setInspectResultId(inspectResultId);
            inspectDetails.setFullField(false);
            ZipOutputStream zipOutputStream = mock(ZipOutputStream.class);
            UserDetail userDetail = mock(UserDetail.class);
            InspectResultService inspectResultService = mock(InspectResultService.class);
            InspectResultDto inspectResultDto = new InspectResultDto();
            List<Stats> stats = new ArrayList<>();
            String taskId = ObjectId.get().toHexString();
            Stats stats2 = new Stats();
            stats2.setTaskId(ObjectId.get().toHexString());
            stats.add(stats2);
            Stats stats1= new Stats();
            stats1.setTaskId(taskId);
            stats1.setResult("failed");
            Source source = new Source();
            source.setConnectionName("mysql");
            source.setTable("test");
            Source target = new Source();
            source.setColumns(Arrays.asList("id","name","text"));
            target.setColumns(Arrays.asList("id","name","text"));
            target.setConnectionName("mysql");
            target.setTable("check_test");
            stats1.setSource(source);
            stats1.setTarget(target);
            stats1.setTaskId("test");
            long targetTotal = 2l;
            stats1.setTargetTotal(targetTotal);
            long sourceTotal = 3l;
            stats1.setSourceTotal(sourceTotal);
            stats1.setSourceOnly(0l);
            stats1.setTargetOnly(1l);
            stats1.setRowFailed(0l);
            stats.add(stats1);
            inspectResultDto.setStats(stats);
            when(inspectResultService.findById(new ObjectId(inspectResultId))).thenReturn(inspectResultDto);
            org.springframework.data.mongodb.core.query.Query query = org.springframework.data.mongodb.core.query.Query.query(Criteria.where("inspectResultId").is(inspectDetails.getInspectResultId()));
            Sort sort = Sort.by("createTime").descending();
            query.with(sort);
            List<InspectDetailsDto> inspectDetailsDto = new ArrayList<>();
            InspectDetailsDto inspectDetailsDto1 = new InspectDetailsDto();
            Map<String,Object> source1 = new HashMap<>();
            source1.put("id",1);
            source1.put("text",null);
            Map<String,Object> target1 = new HashMap<>();
            target1.put("id",1);
            target1.put("name","test1");
            target1.put("text",null);
            inspectDetailsDto1.setSource(source1);
            inspectDetailsDto1.setTarget(target1);
            inspectDetailsDto1.setTaskId(taskId);
            inspectDetailsDto1.setMessage("Different index:1");
            inspectDetailsDto1.setTaskId("test");
            inspectDetailsDto.add(inspectDetailsDto1);
            when(inspectDetailsService.findAllDto(query,userDetail)).thenReturn(inspectDetailsDto);
            doCallRealMethod().when(inspectDetailsService).compareDifferenceFields(any(),any());
            doCallRealMethod().when(inspectDetailsService).export(inspectDetails, zipOutputStream, userDetail, inspectResultService);
            inspectDetailsService.export(inspectDetails, zipOutputStream, userDetail, inspectResultService);

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("sourceTableName", source.getTable() + "/" + source.getConnectionName()
                    + "(Row:" + sourceTotal + ")");
            jsonObject.put("targetTableName", target.getTable() + "/" + target.getConnectionName()
                    + "(Row:" + targetTotal + ")");
            jsonObject.put("checkResult", "failed");
            long count = targetTotal - sourceTotal;
            if (count < 0) {
                jsonObject.put("targetCountLess", Math.abs(count));
            } else {
                jsonObject.put("targetCountMore", count);
            }

            jsonObject.put("tableDiffCount", 1);
            JSONArray jsonArrayTmp = new JSONArray();
            JSONObject jsonObjectTmp = new JSONObject();
            Map<String,Object> resultSource = new HashMap<>();
            resultSource.put("name",null);
            Map<String,Object> resultTarget = new HashMap<>();
            resultTarget.put("name","test1");
            jsonObjectTmp.put("source", resultSource);
            jsonObjectTmp.put("target", resultTarget);
            jsonArrayTmp.add(jsonObjectTmp);
            jsonObject.put("data", jsonArrayTmp);
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(jsonObject);



            verify(zipOutputStream,times(1)).write(JSONObject.toJSONString(jsonArray, SerializerFeature.WriteMapNullValue).getBytes());

        }
        @Test
        void testExportWhenDetailIsNull() throws IOException {
            InspectDetailsService inspectDetailsService = mock(InspectDetailsService.class);
            InspectDetailsDto inspectDetails = new InspectDetailsDto();
            String inspectResultId = ObjectId.get().toHexString();
            inspectDetails.setInspectResultId(inspectResultId);
            inspectDetails.setFullField(false);
            ZipOutputStream zipOutputStream = mock(ZipOutputStream.class);
            UserDetail userDetail = mock(UserDetail.class);
            InspectResultService inspectResultService = mock(InspectResultService.class);
            when(inspectResultService.findById(new ObjectId(inspectResultId))).thenReturn(null);
            doCallRealMethod().when(inspectDetailsService).export(inspectDetails, zipOutputStream, userDetail, inspectResultService);
            inspectDetailsService.export(inspectDetails, zipOutputStream, userDetail, inspectResultService);
            verify(zipOutputStream,times(0)).write(JSONObject.toJSONString(anyList(), SerializerFeature.WriteMapNullValue).getBytes());
        }

        @Test
        void testExport_fullFiled_false_message_null() {
            InspectDetailsService inspectDetailsService = mock(InspectDetailsService.class);
            InspectDetailsDto inspectDetailsDto = mock(InspectDetailsDto.class);
            InspectResultDto inspectResultDto = mock(InspectResultDto.class);
            doCallRealMethod().when(inspectDetailsService).compareDifferenceFields(inspectDetailsDto, inspectResultDto);
            inspectDetailsService.compareDifferenceFields(inspectDetailsDto, inspectResultDto);
            verify(inspectDetailsDto, new Times(0)).setSource(anyMap());
            verify(inspectDetailsDto, new Times(0)).setTarget(anyMap());
        }

    }

}
