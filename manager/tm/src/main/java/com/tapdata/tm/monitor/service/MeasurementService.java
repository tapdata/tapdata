package com.tapdata.tm.monitor.service;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Maps;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.monitor.constant.Granularity;
import com.tapdata.tm.monitor.constant.TableNameEnum;
import com.tapdata.tm.monitor.dto.TransmitTotalVo;
import com.tapdata.tm.monitor.entity.AgentEnvironmentEntity;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.vo.GetMeasurementVo;
import com.tapdata.tm.monitor.vo.GetStaticVo;
import com.tapdata.tm.task.entity.SubTaskEntity;
import com.tapdata.tm.task.repository.SubTaskRepository;
import com.tapdata.tm.utils.BeanUtil;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.TimeUtil;
import io.tapdata.common.sample.request.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MeasurementService {
    private final Integer MAX_RETURN_DOC = 5000;

    private Long timeGap = 0l;

    @Autowired
    MessageQueueService messageQueueService;

    @Autowired
    private MongoTemplate mongoOperations;


    @Autowired
    private SubTaskRepository subTaskRepository;


    private static final Long ONE_HOUR = 60 * 60 * 1000L;
    private static final Long TWELVE_HOUR = 12 * ONE_HOUR;
    private static final Long THIRTY_DAY = 2 * TWELVE_HOUR * 30;
    private static final Long TWENTY_FOUR_MONTH = 24 * THIRTY_DAY;



  /*  public void addBulkSampleRequest(BulkSampleRequest bulkSampleRequest) {
        addBulkSampleRequest(bulkSampleRequest, MeasurementEntity.GRANULARITY_MINUTE);
    }*/

    public void addAgentMeasurement(List<SampleRequest> samples) {
        addBulkAgentMeasurement(samples, TableNameEnum.AgentMeasurement, Granularity.GRANULARITY_MINUTE);
    }

    public void addAgentStatistics(List<StatisticRequest> statisticRequestList) {
        addAgentStatistics(statisticRequestList, TableNameEnum.AgentStatistics);
    }

    /**
     * promethius  来pull的时候返回上一分钟所有的分钟数据（核心是返回分钟里的所有秒点）
     */
    @Deprecated
    public List getPromethius(GetMeasurementParam getMeasurementParam) {
        String tableName = "AgentMeasurement";
        Map<String, String> tags = getMeasurementParam.getTags();
        String granularity = (null == getMeasurementParam.getGranularity()) ? MeasurementEntity.GRANULARITY_MINUTE : getMeasurementParam.getGranularity();
        Criteria criteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(granularity);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            criteria.and(MeasurementEntity.FIELD_TAGS + "." + entry.getKey()).is(entry.getValue());
        }
        Query query = Query.query(criteria);
        List<MeasurementEntity> measurementEntityList = mongoOperations.find(query, MeasurementEntity.class, tableName);

        List<GetMeasurementVo> measurementVoList = BeanUtil.deepCloneList(measurementEntityList, GetMeasurementVo.class);
        return measurementVoList;

    }


    @Deprecated
    public List query(GetMeasurementParam getMeasurementParam) {
        Map<String, String> tags = getMeasurementParam.getTags();
        String granularity = (null == getMeasurementParam.getGranularity()) ? MeasurementEntity.GRANULARITY_MINUTE : getMeasurementParam.getGranularity();
        Criteria criteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(granularity);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            criteria.and(MeasurementEntity.FIELD_TAGS + "." + entry.getKey()).is(entry.getValue());
        }
        Query query = Query.query(criteria);
        List<MeasurementEntity> measurementEntityList = mongoOperations.find(query, MeasurementEntity.class, TableNameEnum.getTableName(getMeasurementParam.getMeasurement()));

        List<GetMeasurementVo> measurementVoList = BeanUtil.deepCloneList(measurementEntityList, GetMeasurementVo.class);
        return measurementVoList;

    }

    /*  endTime - startTime <= 1h(1 * 60 * 60s) --> minute, second point, max 60 * 12 = 720 period 5s
     * // <= 12h(12 * 60 * 60s) --> hour, minute point, max 12 * 60 = 720 period 1m
     * // <= 30d(30 * 24 * 60 * 60s) --> day, hour point, max 24 * 30 = 720 period 1h
     * // <= 24m+ --> month, day point, max 30 * 24 = 720 period 1d
     */
    private List<Date> parseGuanluaryAndDateList(QuerySampleParam querySampleParam) {
        Long startMillSeconds = querySampleParam.getStart();
        Long endMillSeconds = querySampleParam.getEnd();
        long nowMillSeconds;
        if (null == endMillSeconds) {
            nowMillSeconds = new Date().getTime();
        } else {
            nowMillSeconds = endMillSeconds;
        }


        long millSecondsOneSecond = 1000L;
        String guaranty = querySampleParam.getGuanluary() == null ? Granularity.GRANULARITY_MINUTE : querySampleParam.getGuanluary();
        switch (guaranty) {
            case Granularity.GRANULARITY_HOUR:
                timeGap = millSecondsOneSecond * 60;
                break;
            case Granularity.GRANULARITY_DAY:
                timeGap = millSecondsOneSecond * 60 * 60;
                break;
            case Granularity.GRANULARITY_MONTH:
                timeGap = millSecondsOneSecond * 60 * 60 * 24;
                break;
            default:
                timeGap = millSecondsOneSecond * 5;
                break;
        }
        querySampleParam.setGuanluary(guaranty);
        List<Date> dateList = new ArrayList<>();

        while (Objects.nonNull(startMillSeconds) && startMillSeconds <= nowMillSeconds) {
            dateList.add(new Date(startMillSeconds));
            startMillSeconds = startMillSeconds + timeGap;
        }

        return dateList;
    }


    /**
     * // endTime - startTime <= 1h(1 * 60 * 60s) --> minute, second point, max 60 * 12 = 720 period 5s
     * // <= 12h(12 * 60 * 60s) --> hour, minute point, max 12 * 60 = 720 period 1m
     * // <= 30d(30 * 24 * 60 * 60s) --> day, hour point, max 24 * 30 = 720 period 1h
     * // <= 24m+ --> month, day point, max 30 * 24 = 720 period 1d
     *
     * @param querySampleParam
     * @return
     */
    public List<Map<String, List<Number>>> querySample(QuerySampleParam querySampleParam) {
        List<Map<String, List<Number>>> vss = new ArrayList<>();

        List<String> fields = querySampleParam.getFields();

        List<Date> dateList = parseGuanluaryAndDateList(querySampleParam);

        List<MeasurementEntity> measurementEntityList = queryMeasurement(querySampleParam.getTags(), querySampleParam.getStart(), querySampleParam.getEnd(), fields, querySampleParam.getGuanluary(), querySampleParam.getType(), querySampleParam.getLimit(), TableNameEnum.AgentMeasurement);

        Map<Date, Sample> dateToSample = new HashMap<>();
        for (MeasurementEntity measurementEntity : measurementEntityList) {
            List<Sample> sampleList = measurementEntity.getSamples();
            dateToSample.putAll(sampleList.stream().collect(Collectors.toMap(Sample::getDate, Function.identity(), (v1, v2) -> v1)));
        }
        List<Sample> sampleList = setPointToClosestValue(dateList, dateToSample, fields);
        // 倒序，为了兼容前端
        List<Sample> reversedSampleList = ListUtil.reverse(sampleList);

        Map<String, List<Number>> sampleMap = new HashedMap();
        for (Sample sample : reversedSampleList) {
            if (null != sample) {
                Map<String, Number> vs = sample.getVs();
                initSampelFileds(vs, sampleMap);
                List<String> hitKey = new ArrayList();
                for (String key : vs.keySet()) {
                    hitKey.add(key);
                    Number value = vs.get(key);
                    //四舍五入
                    sampleMap.get(key).add(Math.round(value.longValue()));
                }
                getNotHitLists(hitKey, sampleMap);
            }
        }

        // 倒序，为了兼容前端
        List<Date> reversedDateList = ListUtil.reverse(dateList);
        sampleMap.put("time", reversedDateList.stream().map(Date::getTime).collect(Collectors.toList()));
        vss.add(sampleMap);
        return vss;
    }


    public List<Map<String, List<Number>>> queryHeadAndTail(QuerySampleParam querySampleParam) {
        List<Map<String, List<Number>>> vss = new ArrayList<>();

        List<String> fields = querySampleParam.getFields();
        List<MeasurementEntity> measurementEntityList = queryMeasurement(querySampleParam.getTags(), querySampleParam.getStart(), querySampleParam.getEnd(), fields, querySampleParam.getGuanluary(), querySampleParam.getType(), querySampleParam.getLimit(), TableNameEnum.AgentMeasurement);
        List<Number> dateList = Lists.newArrayList();
        Map<String, List<Number>> sampleMap = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(measurementEntityList)) {
            for (MeasurementEntity measurementEntity : measurementEntityList) {
                List<Sample> sampleList = measurementEntity.getSamples();
                List<Date> dates = sampleList.stream().map(Sample::getDate).collect(Collectors.toList());
                dateList.addAll(dates.stream().map(Date::getTime).collect(Collectors.toList()));
                for (Sample sample : sampleList) {
                    Map<String, Number> vs = sample.getVs();
                    initSampelFileds(vs, sampleMap);
                    List<String> hitKey = new ArrayList<String>();
                    for (String key : vs.keySet()) {
                        hitKey.add(key);
                        sampleMap.get(key).add(vs.get(key));
                    }
                    getNotHitLists(hitKey, sampleMap);
                }
            }
            sampleMap.put("time", dateList);
        }
        vss.add(sampleMap);
        return vss;
    }


    private List<Date> getAllKeysAsList(Map<Date, Sample> map) {
        List<Date> keyList = new ArrayList<>();
        Set<Date> sampleDateKeySet = map.keySet();
        sampleDateKeySet.forEach(set -> {
            keyList.add(set);
        });
        return keyList;
    }

    private List<Sample> setPointToClosestValue(List<Date> dateList, Map<Date, Sample> dateToSample, List<String> fields) {
        List<Sample> result = new ArrayList();
        if (dateToSample.size() > 0) {
            List<Date> sampleDateList = getAllKeysAsList(dateToSample);
            Collections.sort(sampleDateList, (o1, o2) -> {
                return o1.compareTo(o2);//升序，前边加负号变为降序
            });

            int sampleIndex = 0;
            if (CollectionUtils.isEmpty(dateList)) {
                result.addAll(dateToSample.values());
            } else {
                for (int i = 0; i < dateList.size(); i++) {
                    Date date = dateList.get(i);
                    Sample sample = null;

                    for (int j = sampleIndex; j < sampleDateList.size(); j++) {
                        Date sampleDate = sampleDateList.get(j);
                        if (DateUtil.between(date, sampleDate, DateUnit.MS) <= timeGap) {
                            sample = dateToSample.get(sampleDate);
                            sampleIndex = j + 1;
                            break;
                        }

                    }
                    if (null == sample) {
                        sample = new Sample();
                        Map<String, Number> vs = new HashMap<>();
                        for (String s : fields) {
                            vs.put(s, 0L);
                        }
                        sample.setVs(vs);
                    }
                    result.add(sample);
                }
            }
        } else {
            for (Date date : dateList) {
                Sample emptySample = new Sample();
                emptySample.setVs(new HashMap<>());
                fields.forEach(field -> {
                    emptySample.getVs().put(field, 0L);
                });
                result.add(emptySample);
            }
        }
        return result;
    }


    private void getNotHitLists(List<String> hitKey, Map<String, List<Number>> sampleMap) {
        sampleMap.forEach((key, notHitList) -> {
            if (CollectionUtils.isNotEmpty(hitKey) && !hitKey.contains(key)) {
                notHitList.add(0);
            }
        });
    }


    public List queryStatistics(List<QueryStisticsParam> queryMeasurementParam) {
        List<Map<String, Number>> vss = new ArrayList<>();

        for (QueryStisticsParam sample : queryMeasurementParam) {
            List<MeasurementEntity> measurementEntityLis = queryMeasurement(sample.getTags(), null, null, sample.getFields(), null, null, null, TableNameEnum.AgentStatistics);
            if (CollectionUtils.isEmpty(measurementEntityLis)) {
                continue;
            }
            Map<Date, List<MeasurementEntity>> dateToMeasurementList = measurementEntityLis.stream().collect(Collectors.groupingBy(MeasurementEntity::getDate));
            dateToMeasurementList.forEach((date, singleList) -> {
                List<Map<String, Number>> statisticsList = singleList.stream().map(MeasurementEntity::getStatistics).collect(Collectors.toList());
                vss.addAll(statisticsList);
            });

        }
        return vss;
    }


    public MeasurementEntity findByTaskIdAndNodeId(String taskId, String nodeId) {
        Query query = Query.query(Criteria.where("tags.nodeId").is(nodeId).and("tags.taskId").is(taskId));
        MeasurementEntity measurementEntity = mongoOperations.findOne(query, MeasurementEntity.class, TableNameEnum.AgentStatistics.getValue());
        return measurementEntity;
    }


    private Map<String, List<Number>> initSampelFileds(Map<String, Number> vs, Map<String, List<Number>> sampleMap) {
        Set<String> keySet = vs.keySet();

        Integer length = 0;
        for (String key : keySet) {
            List existedList = sampleMap.get(key);
            if (CollectionUtils.isNotEmpty(existedList)) {
                length = existedList.size();
                break;
            }
        }


        for (String key : keySet) {
            if (!sampleMap.containsKey(key)) {
                List<Number> newList = Lists.initWithNumber(length, 0);
                sampleMap.put(key, newList);
            }

        }
        return sampleMap;
    }

    private List<MeasurementEntity> queryMeasurement(Map<String, String> tags, Long start, Long end, List<String> fields, String granularity, String type, Long limit, TableNameEnum tableNameEnum) {
        Criteria criteria = new Criteria();
        Date startTime;
        Date endTime;

        if (granularity != null) {
            criteria.and(MeasurementEntity.FIELD_GRANULARITY).is(granularity);
        }

        for (Map.Entry<String, String> entry : tags.entrySet()) {
            criteria.and(MeasurementEntity.FIELD_TAGS + "." + entry.getKey()).is(entry.getValue());
        }

        Criteria criteriaStartTime = new Criteria();
        if (null != start) {
            startTime = new Date(start);
            criteriaStartTime.and("ss.date").gt(startTime);
        }


        Criteria criteriaEndTime = new Criteria();
        if (null != end) {
            endTime = new Date(end);
            criteriaEndTime.and("ss.date").lt(endTime);
        }
        criteria.andOperator(criteriaStartTime, criteriaEndTime);
        Query query = Query.query(criteria);
        query.with(Sort.by(MeasurementEntity.FIELD_DATE).descending());

        if (CollectionUtils.isNotEmpty(fields)) {
            String[] fieldArray = new String[fields.size()];
            if (tableNameEnum == TableNameEnum.AgentMeasurement) {
                for (int i = 0; i < fields.size(); i++) {
                    fieldArray[i] = "ss.vs." + fields.get(i);
                }
                query.fields().include(fieldArray).include("ss.date");
            } else if (tableNameEnum == TableNameEnum.AgentStatistics) {
                for (int i = 0; i < fields.size(); i++) {
                    fieldArray[i] = "statistics." + fields.get(i);
                }
                query.fields().include(fieldArray).include("date");
            } else {
                throw new RuntimeException("Wrong table name for measurement");
            }
        }
        //所有的查询，最大只会返回5000条数据
        if (null != limit && limit <= MAX_RETURN_DOC) {
            query.limit(limit.intValue());
        } else {
            query.limit(MAX_RETURN_DOC);
        }

        List<MeasurementEntity> measurementEntityList = mongoOperations.find(query, MeasurementEntity.class, tableNameEnum.getValue());

        if (CollectionUtils.isNotEmpty(measurementEntityList)) {
            correctHeadAndTailVs(measurementEntityList, start, end);

            if ("headAndTail".equals(type)) {
                parseHeadAndTail(measurementEntityList);
            }
        }
        return measurementEntityList;
    }


    public void addStatic(List<ProcessStaticParam> processStaticParamList, TableNameEnum tableNameEnum) {
        BulkOperations bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, AgentEnvironmentEntity.class);
        for (ProcessStaticParam param : processStaticParamList) {
            Query query = Query.query(Criteria.where("tags").is(param.getTags()));
            Update update = new Update().set(MeasurementEntity.FIELD_SAMPLES, param.getValues());
            bulkOperations.upsert(query, update);
        }
        BulkWriteResult bulkWriteResult = bulkOperations.execute();
        Integer insertCount = bulkWriteResult.getInsertedCount();
    }

    private void parseHeadAndTail(List<MeasurementEntity> measurementEntityList) {
        if (measurementEntityList.size() == 1) {
            MeasurementEntity newOne = BeanUtil.deepClone(measurementEntityList.get(0), MeasurementEntity.class);
            measurementEntityList.add(newOne);
        }

        measurementEntityList.subList(1, measurementEntityList.size() - 1).clear();

        MeasurementEntity tail = measurementEntityList.get(0);
        MeasurementEntity head = measurementEntityList.get(1);

        List<Sample> headSamples = head.getSamples();
        if (CollectionUtils.isNotEmpty(headSamples)) {
            if (headSamples.size() == 1) {
                Sample newone = BeanUtil.deepClone(headSamples.get(0), Sample.class);
                headSamples.add(newone);
            }
            headSamples.subList(0, headSamples.size() - 1).clear();
        }

        List<Sample> tailSamples = tail.getSamples();
        if (CollectionUtils.isNotEmpty(tailSamples)) {
            if (tailSamples.size() == 1) {
                Sample newone = BeanUtil.deepClone(tailSamples.get(0), Sample.class);
                tailSamples.add(newone);
            }
            tailSamples.subList(1, tailSamples.size()).clear();
        }
    }

    /**
     * 因为根据first和last 查询出来的document，vs会有不满足条件的的情况，需要对头和尾再进行过滤，
     * 此处可以再优化,直接从数据库查出来就过滤掉最好
     * inspectDtoList.stream().filter(m -> m.getName().equals("1to2-1asdddd")
     */
    private void correctHeadAndTailVs(List<MeasurementEntity> measurementEntityList, Long start, Long end) {
        if (null != end) {
            List<Sample> newList = new ArrayList<Sample>();
            MeasurementEntity head = measurementEntityList.get(0);
            newList.addAll(head.getSamples().stream().filter(sample -> (sample.getDate().before(new Date(end)))).collect(Collectors.toList()));
            head.setSamples(newList);
        }

        if (null != start) {
            List<Sample> newList = new ArrayList<Sample>();
            MeasurementEntity tail = measurementEntityList.get(measurementEntityList.size() - 1);
            newList.addAll(tail.getSamples().stream().filter(sample -> (sample.getDate().after(new Date(start)))).collect(Collectors.toList()));
            tail.setSamples(newList);
        }
    }


    /**
     * 根据tags查询
     *
     * @param processStaticParam
     */
    public List<GetStaticVo> getStatic(ProcessStaticParam processStaticParam, TableNameEnum tableNameEnum) {
        Map tags = processStaticParam.getTags();
        Query query = Query.query(Criteria.where("tags").is(tags));
        List<AgentEnvironmentEntity> processStaticMeasurementEntityList = mongoOperations.find(query, AgentEnvironmentEntity.class);
        List<GetStaticVo> getStaticVoList = BeanUtil.deepCloneList(processStaticMeasurementEntityList, GetStaticVo.class);
        return getStaticVoList;
    }


    private void addBulkAgentMeasurement(List<SampleRequest> sampleRequestList, TableNameEnum tableNameEnum, String granularity) {
        BulkOperations bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, MeasurementEntity.class, tableNameEnum.getValue());
        for (SampleRequest singleSampleRequest : sampleRequestList) {
            Criteria criteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(granularity);
            Date theDate = TimeUtil.cleanTimeAfterMinute(new Date());
            criteria.and(MeasurementEntity.FIELD_DATE).is(theDate);

            Map<String, String> tags = singleSampleRequest.getTags();
            if (null == tags || 0 == tags.size()) {
                continue;
            }

            for (Map.Entry<String, String> entry : tags.entrySet()) {
                criteria.and(MeasurementEntity.FIELD_TAGS + "." + entry.getKey()).is(entry.getValue());
            }
            Query query = Query.query(criteria);

            Map<String, Object> sampleMap = singleSampleRequest.getSample().toMap();
            Document upd = new Document();
            upd.put("$each", Collections.singletonList(sampleMap));
            upd.put("$slice", 200); //为了保护数组过长， 在出bug的情况下
            upd.put("$sort", new Document().append(Sample.FIELD_DATE, -1));

            Update update = new Update().push(MeasurementEntity.FIELD_SAMPLES, upd)
                    .min(MeasurementEntity.FIELD_FIRST, singleSampleRequest.getSample().getDate())
                    .max(MeasurementEntity.FIELD_LAST, singleSampleRequest.getSample().getDate());
            bulkOperations.upsert(query, update);
        }

        BulkWriteResult bulkWriteResult = bulkOperations.execute();
        log.info("add bulkWriteResult,{}", bulkWriteResult);
    }

    /**
     * agentEnvironment 表，一个tags,永远之哟一条数据
     *
     * @param statisticRequestList
     * @param tableNameEnum
     */
    private void addAgentStatistics(List<StatisticRequest> statisticRequestList, TableNameEnum tableNameEnum) {
        //todo 不要在for循环里  execute
        for (StatisticRequest singleStatistics : statisticRequestList) {
            Map<String, String> tags = singleStatistics.getTags();
            if (null == tags || 0 == tags.size()) {
                continue;
            }

            Criteria criteria = new Criteria();
            Update update = new Update();
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                criteria.and(MeasurementEntity.FIELD_TAGS + "." + entry.getKey()).is(entry.getValue());
            }

            Query query = Query.query(criteria);
            Statistic statistic = singleStatistics.getStatistic();
            update.setOnInsert("statistics", statistic.getValues());

            Date theDate = TimeUtil.cleanTimeAfterMinute(new Date());
            update.setOnInsert(MeasurementEntity.FIELD_DATE, theDate);
            UpdateResult updateResult = mongoOperations.upsert(query, update, tableNameEnum.getValue());

            Update incOrSetUpdate = new Update();
            List<String> incFields = statistic.getIncFields();
            for (Map.Entry<String, Number> singleVal : statistic.getValues().entrySet()) {
                String valKey = singleVal.getKey();
                if (incFields != null && incFields.contains(valKey)) {
                    incOrSetUpdate.inc("statistics." + valKey, singleVal.getValue());
                } else {
                    incOrSetUpdate.set("statistics." + valKey, singleVal.getValue());
                }
            }
            UpdateResult updateResult2 = mongoOperations.updateFirst(query, incOrSetUpdate, tableNameEnum.getValue());
        }
    }


    public Integer generateMinuteInHourPoint(String collectionName) {
        List<MeasurementEntity> tmMeasurementEntityList = findLastHourMinuteGranty(collectionName);
        Map tags = new HashMap();
        for (MeasurementEntity measurementEntity : tmMeasurementEntityList) {
            List<Sample> sampleList = new ArrayList();
            Sample sample = new Sample();
            sample.setDate(measurementEntity.getDate());
            sample.setVs(measurementEntity.averageValues());
            sampleList.add(sample);
            tags = measurementEntity.getTags();
            addBulkSampleRequest(sampleList, tags, collectionName, Granularity.GRANULARITY_HOUR);
        }

        return tmMeasurementEntityList.size();
    }


    public Integer generateHourInDayPoint(String collectionName) {
        List<MeasurementEntity> tmMeasurementEntityList = findLastHourInDayGranty(collectionName);
        Map tags = new HashMap();
        for (MeasurementEntity measurementEntity : tmMeasurementEntityList) {
            List<Sample> sampleList = new ArrayList();
            Sample sample = new Sample();
            sample.setDate(measurementEntity.getDate());
            sample.setVs(measurementEntity.averageValues());
            sampleList.add(sample);
            tags = measurementEntity.getTags();
            addBulkSampleRequest(sampleList, tags, collectionName, Granularity.GRANULARITY_DAY);
        }

        return tmMeasurementEntityList.size();
    }

    public Integer generateDayInMonthPoint(String collectionName) {
        List<MeasurementEntity> tmMeasurementEntityList = findLastDayInMonthGranty(collectionName);
        Map tags = new HashMap();
        for (MeasurementEntity measurementEntity : tmMeasurementEntityList) {
            List<Sample> sampleList = new ArrayList();
            Sample sample = new Sample();
            sample.setDate(measurementEntity.getDate());
            sample.setVs(measurementEntity.averageValues());
            sampleList.add(sample);
            tags = measurementEntity.getTags();
            addBulkSampleRequest(sampleList, tags, collectionName, Granularity.GRANULARITY_MONTH);
        }

        return tmMeasurementEntityList.size();
    }

    /**
     * 定时补充小时维度的秒点
     *
     * @param sampleList
     * @param tags
     * @param collectionName
     */
    private void addBulkSampleRequest(List<Sample> sampleList, Map<String, String> tags, String collectionName, String granularity) {
        BulkOperations bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, MeasurementEntity.class, collectionName);

        Date theDate = TimeUtil.cleanTimeAfterHour(new Date());
        for (Sample sample : sampleList) {
            Criteria criteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(granularity);
            criteria.and(MeasurementEntity.FIELD_DATE).is(theDate);
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                criteria.and(MeasurementEntity.FIELD_TAGS + "." + entry.getKey()).is(entry.getValue());
            }
            Query query = Query.query(criteria);

            Map<String, Object> sampleMap = sample.toMap();
            Document upd = new Document();
            upd.put("$each", Collections.singletonList(sampleMap));
            upd.put("$slice", 200); //为了保护数组过长， 在出bug的情况下
            upd.put("$sort", new Document().append(Sample.FIELD_DATE, -1));

            Update update = new Update().push(MeasurementEntity.FIELD_SAMPLES, upd)
                    .min(MeasurementEntity.FIELD_FIRST, sample.getDate())
                    .max(MeasurementEntity.FIELD_LAST, sample.getDate());
            bulkOperations.upsert(query, update);
        }
        BulkWriteResult bulkWriteResult = bulkOperations.execute();
    }

    private List<MeasurementEntity> findLastHourMinuteGranty(String collectionName) {
        List<MeasurementEntity> tmMeasurementEntityList = new ArrayList();
        Date thisHour = TimeUtil.cleanTimeAfterHour(new Date());
        Date lastHour = TimeUtil.cleanTimeAfterHour(DateUtil.offsetHour(thisHour, -1));
        Criteria criteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);
        criteria.and(MeasurementEntity.FIELD_DATE).gt(lastHour).andOperator(Criteria.where(MeasurementEntity.FIELD_DATE).lt(thisHour));
        Query query = Query.query(criteria);
        tmMeasurementEntityList = mongoOperations.find(query, MeasurementEntity.class, collectionName);
        return tmMeasurementEntityList;
    }

    private List<MeasurementEntity> findLastHourInDayGranty(String collectionName) {
        Date now = new Date();
        List<MeasurementEntity> tmMeasurementEntityList = new ArrayList();
        Date startOfDay = DateUtil.beginOfDay(now).toJdkDate();
        Date endOfDay = DateUtil.endOfDay(now).toJdkDate();
        Criteria criteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_HOUR);
        criteria.and(MeasurementEntity.FIELD_DATE).gt(startOfDay).andOperator(Criteria.where(MeasurementEntity.FIELD_DATE).lt(endOfDay));
        Query query = Query.query(criteria);
        tmMeasurementEntityList = mongoOperations.find(query, MeasurementEntity.class, collectionName);
        return tmMeasurementEntityList;
    }

    private List<MeasurementEntity> findLastDayInMonthGranty(String collectionName) {
        Date now = new Date();
        List<MeasurementEntity> tmMeasurementEntityList = new ArrayList();
        Date startOfMonth = DateUtil.beginOfMonth(now).toJdkDate();
        Date endOfMonth = DateUtil.endOfMonth(now).toJdkDate();
        Criteria criteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_DAY);
        criteria.and(MeasurementEntity.FIELD_DATE).gt(startOfMonth).andOperator(Criteria.where(MeasurementEntity.FIELD_DATE).lt(endOfMonth));
        Query query = Query.query(criteria);
        tmMeasurementEntityList = mongoOperations.find(query, MeasurementEntity.class, collectionName);
        return tmMeasurementEntityList;
    }


    /**
     * 要统计的几个数据
     * "inputTotal",
     * "outputTotal",
     * "insertedTotal",
     * "updatedTotal",
     * "deletedTotal"
     *
     * @param userDetail
     * @return
     */
    public TransmitTotalVo queryTransmitTotal(UserDetail userDetail) {
        Aggregation aggregation5 =
                Aggregation.newAggregation(
                        Aggregation.match(Criteria.where("tags.customerId").is("enterpriseId2")),
                        Aggregation.unwind("ss"),
                        Aggregation.group("tags.customerId")
                                .sum("$ss.vs.inputTotal").as("inputTotal")
                                .sum("$ss.vs.outputTotal").as("outputTotal")
                                .sum("$ss.vs.insertedTotal").as("insertedTotal")
                                .sum("$ss.vs.deletedTotal").as("deletedTotal")
                                .sum("$ss.vs.updatedTotal").as("updatedTotal"));
        AggregationResults<TransmitTotalVo> outputTypeCount5 =
                mongoOperations.aggregate(aggregation5, TableNameEnum.AgentStatistics.getValue(), TransmitTotalVo.class);

        TransmitTotalVo transmitTotalVo = new TransmitTotalVo();
        for (Iterator<TransmitTotalVo> iterator = outputTypeCount5.iterator(); iterator.hasNext(); ) {
            transmitTotalVo = iterator.next();
        }
        return transmitTotalVo;
    }

    /**
     * 任务重置，删除对应的指标
     *
     * @param subTaskId
     */
    public void deleteSubTaskMeasurement(String subTaskId) {
        if (StringUtils.isEmpty(subTaskId)) {
            return;
        }
        Query query = Query.query(Criteria.where("tags.subTaskId").is(subTaskId));
        DeleteResult deleteResult1 = mongoOperations.remove(query, MeasurementEntity.class, TableNameEnum.AgentMeasurement.getValue());
        DeleteResult deleteResult2 = mongoOperations.remove(query, MeasurementEntity.class, TableNameEnum.AgentStatistics.getValue());

        log.info(" subTaskId :{}  删除了 {} 条 和 {} 记录", subTaskId, JsonUtil.toJson(deleteResult1), JsonUtil.toJson(deleteResult2));
    }

    /**
     * 首页传输总览
     * 总输入 inputTotal
     * 总输出 outputTotal
     * 总插入 insertedTotal
     * 总更新 updatedTotal
     * 总删除 deletedTotal
     */
    public Map<String, Number> getTransmitTotal(UserDetail userDetail) {
        Map transmitTotalMap = new HashMap();
        Query querySubTask = Query.query(Criteria.where("user_id").is(userDetail.getUserId()).and("is_deleted").ne(true));
        querySubTask.fields().include("id");
        List<SubTaskEntity> subTaskDtos = subTaskRepository.findAll(querySubTask);
        List<String> subTaskIdList = subTaskDtos.stream().map(SubTaskEntity::getId).map(ObjectId::toString).collect(Collectors.toList());


        Query query = Query.query(Criteria.where("tags.type").is("subTask").and("tags.subTaskId").in(subTaskIdList));
        List<MeasurementEntity> measurementEntityList = mongoOperations.find(query, MeasurementEntity.class, TableNameEnum.AgentStatistics.getValue());
        List<Map<String, Number>> statisticsList = measurementEntityList.stream().map(MeasurementEntity::getStatistics).collect(Collectors.toList());

        Integer inputTotal = statisticsList.stream().filter(e -> e.get("inputTotal") != null && (e.get("inputTotal") instanceof Integer)).mapToInt(e -> Integer.parseInt(e.get("inputTotal").toString())).sum(); //求 inputTotal 的总数量
        Integer outputTotal = statisticsList.stream().filter(e -> e.get("outputTotal") != null&& (e.get("outputTotal") instanceof Integer)).mapToInt(e -> Integer.parseInt(e.get("outputTotal").toString())).sum(); //求 outputTotal 的总数量
        Integer insertedTotal = statisticsList.stream().filter(e -> e.get("insertedTotal") != null&& (e.get("insertedTotal") instanceof Integer)).mapToInt(e -> Integer.parseInt(e.get("insertedTotal").toString())).sum(); //求 insertedTotal 的总数量
        Integer updatedTotal = statisticsList.stream().filter(e -> e.get("updatedTotal") != null&& (e.get("updatedTotal") instanceof Integer)).mapToInt(e -> Integer.parseInt(e.get("updatedTotal").toString())).sum(); //求 updatedTotal 的总数量
        Integer deletedTotal = statisticsList.stream().filter(e -> e.get("deletedTotal") != null&& (e.get("deletedTotal") instanceof Integer)).mapToInt(e -> Integer.parseInt(e.get("deletedTotal").toString())).sum(); //求 deletedTotal 的总数量

        transmitTotalMap.put("inputTotal", inputTotal);
        transmitTotalMap.put("outputTotal", outputTotal);
        transmitTotalMap.put("insertedTotal", insertedTotal);
        transmitTotalMap.put("updatedTotal", updatedTotal);
        transmitTotalMap.put("deletedTotal", deletedTotal);
        return transmitTotalMap;
    }

    public MeasurementEntity findBySubTaskId(String subTaskId) {
        MeasurementEntity measurementEntity = new MeasurementEntity();
        Query query = Query.query(Criteria.where("tags.type").is("subTask").and("tags.subTaskId").is(subTaskId));
        measurementEntity = mongoOperations.findOne(query, MeasurementEntity.class, TableNameEnum.AgentStatistics.getValue());
        return measurementEntity;
    }


}
