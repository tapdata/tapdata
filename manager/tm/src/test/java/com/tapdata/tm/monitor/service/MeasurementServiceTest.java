//package com.tapdata.tm.monitor.service;
//
//import com.mongodb.client.result.DeleteResult;
//import com.mongodb.client.result.UpdateResult;
//import com.tapdata.manager.common.utils.JsonUtil;
//import com.tapdata.tm.BaseJunit;
//import com.tapdata.tm.monitor.constant.Granularity;
//import com.tapdata.tm.monitor.constant.TableNameEnum;
//import com.tapdata.tm.monitor.entity.MeasurementEntity;
//import com.tapdata.tm.utils.TimeUtil;
//import io.tapdata.common.sample.request.Sample;
//import io.tapdata.common.sample.request.Statistic;
//import org.apache.commons.collections.CollectionUtils;
//import org.apache.commons.collections.map.HashedMap;
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.domain.Sort;
//import org.springframework.data.mongodb.core.MongoOperations;
//import org.springframework.data.mongodb.core.aggregation.*;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.data.mongodb.core.query.Update;
//
//import java.util.*;
//import java.util.stream.Collectors;
//
//import static org.junit.jupiter.api.Assertions.*;
//import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
//import static org.springframework.data.mongodb.core.aggregation.ArrayOperators.Filter.filter;
//import static org.springframework.data.mongodb.core.aggregation.ComparisonOperators.valueOf;
//
//class MeasurementServiceTest extends BaseJunit {
//
//    @Autowired
//    MeasurementService measurementService;
//
//    @Autowired
//    MongoOperations mongoOperations;
//
//    @Test
//    void addWebVisitTotalCount() {
//        Date date = new Date(1645066800000L);
//        System.out.println(date);
//    }
//
//    @Test
//    void addErrorVisitTotalCount() {
//        printResult(measurementService.queryTransmitTotal(getUser()));
//    }
//
//    @Test
//    void generateMinuteInHourPoint() {
//        measurementService.generateMinuteInHourPoint("AgentMeasurement");
//    }
//
//
//    @Test
//    public void deleteAll() {
//        mongoOperations.remove(new Query(Criteria.where("isdee").ne(false)), MeasurementEntity.class, TableNameEnum.AgentMeasurement.getValue());
//    }
//
//    @Test
//    public void addAgentStatistics() {
//        Query query = Query.query(Criteria.where("grnty").is("minute")).limit(5);
//        List<MeasurementEntity> measurementEntityList = mongoOperations.find(query, MeasurementEntity.class, TableNameEnum.AgentMeasurement.getValue());
//
//        printResult(measurementEntityList.stream().findFirst());
//    }
//
//    @Test
//    public void queryMeasurement2() {
//        Map<String, String> tags = new HashedMap();
//        tags.put("subTaskId", "6204c706a2546216a3974625");
//        tags.put("type", "subTask");
//        Long start = 1644336000000L;
//        Long end = 1644508800000L;
////        List<MeasurementEntity> measurementEntityList = measurementService.queryMeasurement(tags, start, end, null, Granularity.GRANULARITY_MINUTE, null, TableNameEnum.AgentEnvironment);
////        System.out.println("total :" + measurementEntityList.size());
//    }
//
//    @Test
//    public void queryMeasurement() {
//        /*
//         * {"tags.subTaskId":"6204c706a2546216a3974625","tags.type":"subTask"}
//         * */
//        Map<String, String> tags = new HashedMap();
//        tags.put("subTaskId", "6204c706a2546216a3974625");
//        tags.put("type", "subTask");
//
//        Criteria criteria = new Criteria();
//        criteria.and(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MONTH);
//        for (Map.Entry<String, String> entry : tags.entrySet()) {
//            criteria.and(MeasurementEntity.FIELD_TAGS + "." + entry.getKey()).is(entry.getValue());
//        }
//        List<String> fields = new ArrayList<>();
//        Query query = new Query();
//        criteria.and("first").gt(new Date(1644336000000L));  //2022-02-10 10:10:59
//        criteria.and("last").lt(new Date(1644508800000L));  //2022-02-10 10:10:59
//        query.addCriteria(criteria);
//
//        fields.add("inputQps");
//        fields.add("outputQPS");
//        if (CollectionUtils.isNotEmpty(fields)) {
//            String[] fieldArray = new String[fields.size()];
//            for (int i = 0; i < fields.size(); i++) {
//                fieldArray[i] = "ss.vs." + fields.get(i);
//            }
//            query.fields().include(fieldArray).include("ss.date");
//        }
//        List<MeasurementEntity> measurementEntityList = mongoOperations.find(query, MeasurementEntity.class, TableNameEnum.AgentMeasurement.getValue());
//        System.out.println("总记录数：" + measurementEntityList.size());
//        printResult(measurementEntityList);
//    }
//
//
//    @Test
//    public void streamTest() {
//        MeasurementEntity newone = new MeasurementEntity();
//        List<Sample> sampleList = new ArrayList<>();
//        Map vs = new HashMap();
//        vs.put("aa", 1);
//
//        Sample sample = new Sample();
//        sample.setVs(vs);
//
//        Sample sample2 = new Sample();
//        Map vs2 = new HashMap();
//        vs2.put("aa", 1);
//        vs2.put("ab", 122);
//        sample2.setVs(vs2);
//
//        Sample sample3 = new Sample();
//        Map vs3 = new HashMap();
//        vs3.put("aa", 1);
//        vs3.put("ab", 2);
//        vs3.put("a21b", 2);
//        sample3.setVs(vs3);
//
//        sampleList.add(sample);
//        sampleList.add(sample2);
//        sampleList.add(sample3);
//
//        newone.setSamples(sampleList);
//
//        newone.getSamples().stream().map(Sample::initVsValue).collect(Collectors.toList());
//
//
//        System.out.println(newone);
//        printResult(newone);
//
//    }
//
//    @Test
//    public void queryArrayTest() {
//        Criteria criteria = Criteria.where("tags.type").is("subTask")
//                .and("tags.subTaskId").is("620db3fb8cc2af789704d8ac")
//                .and("grnty").is("minute");
//        MatchOperation matchStage = Aggregation.match(criteria);
//
//        ProjectionOperation project = project().andInclude("_id", "ss", "date", "grnty");
//        Aggregation aggregation = newAggregation(matchStage,
//                sort(Sort.Direction.DESC, "date"),
//                project.and("tags,type").as("node1").and(filter("ss")
//                                .as("item")
//                                .by(valueOf(
//                                        "item.date")
//                                        .greaterThanValue(new Date(1645081920000L).getTime())))
//                        .as("parts"));
//        List results = mongoOperations.aggregate(aggregation, TableNameEnum.AgentMeasurement.getValue(), MeasurementEntity.class).getMappedResults();
//        printResult("记录数" + results.size());
//        printResult(results);
//    }
//
//
//    @Test
//    public void deleteSubTaskMeasurement() {
//        String subTaskId = "6201dc172872a61c7809cf5f";
//        measurementService.deleteSubTaskMeasurement(subTaskId);
//    }
//
//}