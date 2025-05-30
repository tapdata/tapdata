package com.tapdata.tm.monitor.service;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.constant.Granularity;
import com.tapdata.tm.monitor.constant.KeyWords;
import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.AggregateMeasurementParam;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.param.SyncStatusStatisticsParam;
import com.tapdata.tm.monitor.vo.SyncStatusStatisticsVo;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import com.tapdata.tm.task.bean.TableStatusInfoDto;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.TimeUtil;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.Fields;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MeasurementServiceV2Impl implements MeasurementServiceV2 {
    public static final String REPLICATE_LAG = "replicateLag";
    public static final String TASK_ID = "taskId";
    public static final String TAGS_TASK_ID = "tags.taskId";
    public static final String TAGS_TASK_RECORD_ID = "tags.taskRecordId";
    public static final String TAGS_TYPE = "tags.type";
    public static final String TABLE = "table";
    public static final String TAGS_TABLE = "tags.table";
    private final MongoTemplate mongoOperations;
    private final MetadataInstancesService metadataInstancesService;
    private final TaskService taskService;
    private Map<String, Long> taskDelayTimeMap;

    public MeasurementServiceV2Impl(@Qualifier(value = "obsMongoTemplate") CompletableFuture<MongoTemplate> mongoTemplateCompletableFuture, MetadataInstancesService metadataInstancesService, TaskService taskService) throws ExecutionException, InterruptedException {
        this.mongoOperations = mongoTemplateCompletableFuture.get();
        this.metadataInstancesService = metadataInstancesService;
        this.taskService = taskService;
        this.taskDelayTimeMap = new HashMap<>();
    }

    @Override
    public List<MeasurementEntity> find(Query query) {
        return mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
    }

    @Override
    public void addAgentMeasurement(List<SampleRequest> samples) {
        addBulkAgentMeasurement(samples);
    }

    private void addBulkAgentMeasurement(List<SampleRequest> sampleRequestList) {
        BulkOperations bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
        DateTime date = DateUtil.date();
        for (SampleRequest singleSampleRequest : sampleRequestList) {
            Criteria criteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);

            Map<String, String> tags = singleSampleRequest.getTags();
            if (null == tags || 0 == tags.size()) {
                continue;
            }

            Date theDate = TimeUtil.cleanTimeAfterMinute(date);
            if (!TABLE.equals(tags.get("type"))) {
                criteria.and(MeasurementEntity.FIELD_DATE).is(theDate);
            }

            for (Map.Entry<String, String> entry : tags.entrySet()) {
                criteria.and(MeasurementEntity.FIELD_TAGS + "." + entry.getKey()).is(entry.getValue());
            }

            Date second = TimeUtil.cleanTimeAfterSecond(date);
            AtomicReference<Sample> requestSample = new AtomicReference<>(singleSampleRequest.getSample());

            Query query = Query.query(criteria);
            requestSample.get().setDate(second);

            Map<String, Object> sampleMap = requestSample.get().toMap();
            Document upd = new Document();
            upd.put("$each", Collections.singletonList(sampleMap));
            upd.put("$slice", 200); //为了保护数组过长， 在出bug的情况下
            upd.put("$sort", new Document().append(Sample.FIELD_DATE, -1));

            Update update = new Update()
                    .min(MeasurementEntity.FIELD_FIRST, requestSample.get().getDate())
                    .max(MeasurementEntity.FIELD_LAST, requestSample.get().getDate());
            if (TABLE.equals(tags.get("type"))) {
                update.set(MeasurementEntity.FIELD_SAMPLES, Collections.singletonList(sampleMap));
                update.set(MeasurementEntity.FIELD_DATE, theDate);
            } else {
                update.push(MeasurementEntity.FIELD_SAMPLES, upd);
            }

            bulkOperations.upsert(query, update);
            if ("task".equals(tags.get("type"))) {
                Map<String, Object> vs = (Map) sampleMap.get("vs");
                Object replicateLag = Optional.ofNullable(vs.get(REPLICATE_LAG)).orElse(0);
                Long taskDelayTime = taskDelayTimeMap.get(tags.get(TASK_ID));
                long delayTime = Long.parseLong(replicateLag.toString());
                if (null == taskDelayTime || taskDelayTime != delayTime){
                    taskDelayTimeMap.put(tags.get(TASK_ID), delayTime);
                    taskService.updateDelayTime(new ObjectId(tags.get(TASK_ID)), delayTime);
                }
            }
        }

        bulkOperations.execute();
    }

    private static final String TAG_FORMAT = String.format("%s.%%s", MeasurementEntity.FIELD_TAGS);
    private static final String FIELD_FORMAT = String.format("%s.%s.%%s",
            MeasurementEntity.FIELD_SAMPLES, Sample.FIELD_VALUES);
    private static final String INSTANT_PADDING_LEFT = "left";
    private static final String INSTANT_PADDING_RIGHT = "right";
    private static final String INSTANT_PADDING_LEFT_AND_RIGHT = "leftAndRight";


    @Override
    public Object getSamples(MeasurementQueryParam measurementQueryParam) {
        Map<String, Object> ret = new HashMap<>();
        Map<String, List<Map<String, Object>>> data = new HashMap<>();

        if (ObjectUtils.anyNull(measurementQueryParam.getStartAt(), measurementQueryParam.getEndAt())) {
            return ret;
        }

        boolean hasTimeline = false;
        List<Long> timeline = null;
        Long timelineInterval = null;
        long start = measurementQueryParam.getStartAt();
        long end = measurementQueryParam.getEndAt();
        for(String unique : measurementQueryParam.getSamples().keySet()) {
            data.putIfAbsent(unique, new ArrayList<>());
            List<Map<String, Object>> uniqueData = data.get(unique);
            MeasurementQueryParam.MeasurementQuerySample querySample = measurementQueryParam.getSamples().get(unique);
            switch (querySample.getType()) {
                case MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT:
                    Map<String, Sample> instantSamples = getInstantSamples(querySample, INSTANT_PADDING_LEFT_AND_RIGHT, start, end);
                    uniqueData.addAll(formatSingleSamples(instantSamples));
                    break;
                case MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_DIFFERENCE:
                    Map<String, Sample> diffSamples = getDifferenceSamples(querySample, start, end);
                    uniqueData.addAll(formatSingleSamples(diffSamples));
                    break;
                case MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS:
                    hasTimeline = true;
                    String granularity = Granularity.calculateReasonableGranularity(start, end);
                    timelineInterval = Granularity.getTimelineMillisInterval(granularity);
                    if (!granularity.equals(Granularity.GRANULARITY_MINUTE)) {
                        // t1 start      t2            t3           t4      end  t5
                        // |__#____*_*___|___*__*___*__|___*_____*__|____*___#___|
                        //         s1 s2     s3 s4  s5      s6   s7      s8

                        // move the start cursor to the former granularity section
                        if (start % timelineInterval != 0) {
                            start = ((start / timelineInterval) - 1) * timelineInterval;
                        }
                        // move the start cursor to the current granularity section
                        if (end % timelineInterval != 0) {
                            end = ((end / timelineInterval)) * timelineInterval;
                        }
                    }
                    timeline = getTimeline(start, end, timelineInterval);
                    Map<String, List<Sample>> continuousSamples = getContinuousSamples(querySample, start, end);

                    // calculate the last point value
                    if (!granularity.equals(Granularity.GRANULARITY_MINUTE)) {
                        String previousGranularity = Granularity.getPreviousLevelGranularity(granularity);
                        long current = ((System.currentTimeMillis() / timelineInterval)) * timelineInterval;
                        for(int idx = 1; idx < timeline.size(); ++idx) {
                            if (current > timeline.get(idx-1) && current <= timeline.get(idx)) {
                                long time = timeline.get(idx);
                                // get the sample data in [start, end)
                                Criteria criteria = Criteria.where(MeasurementEntity.FIELD_DATE).gte(new Date(time)).lt(new Date(time + timelineInterval));
                                criteria.and(MeasurementEntity.FIELD_GRANULARITY).is(previousGranularity);
                                for (Map.Entry<String, String> entry : querySample.getTags().entrySet()) {
                                    criteria.and(String.format(TAG_FORMAT, entry.getKey())).is(entry.getValue());
                                }

                                List<String> includedFields = new ArrayList<>();
                                includedFields.add(MeasurementEntity.FIELD_DATE);
                                includedFields.add(MeasurementEntity.FIELD_TAGS);
                                for (String field : querySample.getFields()) {
                                    includedFields.add(String.format(FIELD_FORMAT, field));
                                }

                                Query query = new Query(criteria);
                                query.fields().include(includedFields.toArray(new String[]{}));
                                query.with(Sort.by(MeasurementEntity.FIELD_DATE).ascending());

                                List<MeasurementEntity> measurementEntities = mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
                                for (MeasurementEntity entity : measurementEntities) {
                                    String hash = hashTag(entity.getTags());
                                    continuousSamples.putIfAbsent(hash, new ArrayList<>());
                                    Sample sample = new Sample();
                                    sample.setDate(new Date(timeline.get(idx)));
                                    sample.setVs(entity.averageValues());
                                    continuousSamples.get(hash).add(sample);
                                }
                                break;
                            }
                        }
                    }

                    List<Map<String, Object>> formatContinuousSamples = formatContinuousSamples(continuousSamples, timeline, timelineInterval);
                    uniqueData.addAll(formatContinuousSamples);
                    break;
            }
        }

        ret.put("samples", data);
        if (hasTimeline && null != timeline) {
            ret.put("time", timeline);
            ret.put("interval", timelineInterval);
        }

        return ret;
    }


    /**
     *  padding is a value to tolerance the data loss.
     *  diagram:
     *      #: time
     *      *: sample data point
     *   time1                                          time2
     *  |__#____*_*___|___*__*___*__|___*_____*__|____*___#___|
     *         s1 s2     s3 s4  s5      s6   s7      s8
     *  when query instant value of time1, but the first sample value is s1, here we use `padding=left` to tolerance
     *  the data loss of the left part, aka. we got s1 for the query.
     *  when query instant value of time2, but the last sample value is s8, here we use `padding=right` to tolerance
     *  the data loss of the right part, aka. we got s2 for the query.
     * @param querySample querySample
     * @param padding padding
     * @param start time
     * @param end time
     * @return Map
     */
    protected Map<String, Sample> getInstantSamples(MeasurementQueryParam.MeasurementQuerySample querySample, String padding, Long start, Long end) {
        List<String> fields = querySample.getFields();
        Map<String, Sample> data = new HashMap<>();
        if (!StringUtils.equalsAny(querySample.getType(),
                MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT,
                MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_DIFFERENCE)) {
            return data;
        }

        Criteria criteria = Criteria.where(MeasurementEntity.FIELD_DATE);

        boolean typeIsTask = false;
        boolean typeIsNode = false;
        boolean typeIsEngine = false;
        String taskId = "";
        String taskRecordId = "";
        for (Map.Entry<String, String> entry : querySample.getTags().entrySet()) {
            String format = String.format(TAG_FORMAT, entry.getKey());
            String value = entry.getValue();
            criteria.and(format).is(value);
            if (format.equals(TAGS_TYPE)) {
                if ("task".equals(value)) {
                    typeIsTask = true;
                } else if ("engine".equals(value)) {
                    typeIsEngine = true;
                }else if("node".equals(value)){
                    typeIsNode = true;
                }
            }
            if (format.equals(TAGS_TASK_ID)) {
                taskId = value;
            }
            if (format.equals(TAGS_TASK_RECORD_ID)) {
                taskRecordId = value;
            }
        }

			long fixTimes = 0;
			String granularity = Granularity.GRANULARITY_MINUTE;
        AtomicReference<Date> startDate = new AtomicReference<>();
        AtomicReference<Date>  endDate = new AtomicReference<>();
        if (start != null) {
					fixTimes = Granularity.calculateGranularityStart(granularity, start) - start;
					startDate.set(new Date(start + fixTimes));
        }
        if (end != null) {
					endDate.set(new Date(end - fixTimes));
        }

        if (typeIsTask && StringUtils.isNotBlank(taskId) && (ObjectUtils.anyNull(start, end) || Objects.equals(start, end))) {
            TaskDto taskDto = taskService.findByTaskId(new ObjectId(taskId), "scheduledTime","runningTime", "errorTime", "stopTime", "finishTime", "status");
            Optional.ofNullable(taskDto).ifPresent(task -> {
                LinkedList<Date> collect = Lists.newArrayList(task.getScheduledTime(), task.getRunningTime(), task.getErrorTime(), task.getStopTime(), task.getFinishTime()).stream()
                        .filter(Objects::nonNull)
                        .sorted()
                        .collect(Collectors.toCollection(LinkedList::new));

                if (start == null || Objects.equals(start, end)) {
                    startDate.set(TimeUtil.cleanTimeAfterMinute(collect.getFirst()));
                }
                if (end == null) {
                    endDate.set(TimeUtil.cleanTimeAfterMinute(collect.getLast()));
                }
            });
        }

        if (ObjectUtils.anyNull(startDate.get(), endDate.get())) {
            log.error("date value error!");
        }

        criteria = criteria.gte(startDate.get()).lte(endDate.get());
        criteria.and(MeasurementEntity.FIELD_GRANULARITY).is(granularity);
        SortOperation sort;
        long time;
        boolean asc;
        if (padding.equals(INSTANT_PADDING_LEFT)) {
            time = startDate.get().getTime();
            sort = Aggregation.sort(Sort.by(MeasurementEntity.FIELD_DATE).ascending());
            asc = true;
        } else {
            time = endDate.get().getTime();
            sort = Aggregation.sort(Sort.by(MeasurementEntity.FIELD_DATE).descending());
            asc = false;
        }

        MatchOperation match = Aggregation.match(criteria);
        LimitOperation limitOperation = Aggregation.limit(1L);
        AggregationOperation projectionOperation = new AggregationOperation() {
            @Override
            public Document toDocument(AggregationOperationContext context) {
                Document projectFields = new Document();
                projectFields.put(MeasurementEntity.FIELD_ID,"$"+MeasurementEntity.FIELD_TAGS);
                projectFields.put(MeasurementEntity.FIELD_DATE,1);
                projectFields.put(MeasurementEntity.FIELD_TAGS,1);
                projectFields.put(MeasurementEntity.FIELD_SAMPLES,1);
                return new Document("$project", projectFields);
            }
        };
        Aggregation aggregation;
        if (typeIsEngine) {
            LimitOperation limit = new LimitOperation(1L);
            aggregation = Aggregation.newAggregation( match, sort, limit, limitOperation, projectionOperation);
        } else {
            if(typeIsNode){
                //After sorting, group according to nodeId and get the first latest data.
                GroupOperation groupOperation = Aggregation.group("tags.nodeId").first("$$ROOT").as("firstRecord");;
                aggregation = Aggregation.newAggregation(match, sort,groupOperation,Aggregation.replaceRoot().withValueOf("$firstRecord") ,projectionOperation);
            }else {
                aggregation = Aggregation.newAggregation(match, sort, limitOperation, projectionOperation);
            }
        }
        aggregation.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        AggregationResults<MeasurementEntity> results = mongoOperations.aggregate(aggregation, MeasurementEntity.COLLECTION_NAME, MeasurementEntity.class);
        List<MeasurementEntity> entities = results.getMappedResults();

        parse2SampleData(entities, data, time, asc);

        for (String hash : data.keySet()) {
            Sample sample = data.get(hash);

            Map<String, Number> values = new HashMap<>();
            for (Map.Entry<String, Number> entry : sample.getVs().entrySet()) {
                if (fields.contains(entry.getKey())) {
                    values.put(entry.getKey(), entry.getValue());
                }
            }

            if (typeIsTask && StringUtils.isNotBlank(taskId) && StringUtils.isNotBlank(taskRecordId)) {
                values.put("lastFiveMinutesQps", getTaskLastFiveMinutesQps(querySample.getTags()));
            }

            if (typeIsTask && MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT.equals(querySample.getType())) {
                Number currentEventTimestamp = values.get("currentEventTimestamp");
                Number snapshotStartAt = values.get("snapshotStartAt");
                // 按照延迟逻辑,源端无事件时,应该为全量同步开始到现在的时间差
                if (Objects.isNull(currentEventTimestamp) && Objects.nonNull(snapshotStartAt)) {
                    Number maxRep = Math.abs(System.currentTimeMillis() - snapshotStartAt.longValue());
                    values.put(REPLICATE_LAG, maxRep);
                }

            }
            sample.setVs(values);
        }
        return data;
    }

    protected void parse2SampleData(List<MeasurementEntity> entities, Map<String, Sample> data, long time, boolean asc) {
        List<Sample> samples;
        for (MeasurementEntity entity : entities) {
            String hash = hashTag(entity.getTags());
            samples = entity.getSamples();
            if (asc) {
                for (int i = 0; i < samples.size(); i++) {
                    putSample(data, time, samples, i, hash);
                }
            } else {
                // 获取最新点的指标（倒序是因为任务在1秒内完成时 init 和 completed 指标同时间一样）
                for(int i = samples.size() - 1; i >= 0; i --) {
                    putSample(data, time, samples, i, hash);
                }
            }
        }
    }

    private static void putSample(Map<String, Sample> data, long time, List<Sample> samples, int i, String hash) {
        Sample sample = samples.get(i);
        if (!data.containsKey(hash)) {
            data.put(hash, sample);
        } else {
            long oldInterval = Math.abs(data.get(hash).getDate().getTime() - time);
            long newInterval = Math.abs(sample.getDate().getTime() - time);
            if (newInterval < oldInterval) {
                data.put(hash, sample);
            }
        }
    }

    protected Double getTaskLastFiveMinutesQps(Map<String, String> tags) {
        Criteria criteria = new Criteria();
        tags.forEach((k, v) -> {
            String format = String.format(TAG_FORMAT, k);
            criteria.and(format).is(v);
        });
        criteria.and(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);
        MatchOperation match = Aggregation.match(criteria);
        SortOperation sort = Aggregation.sort(Sort.by(Sort.Direction.DESC, "date"));
        LimitOperation limit = Aggregation.limit(5);
        UnwindOperation unwind = Aggregation.unwind("ss", false);
        GroupOperation group = Aggregation.group().avg("ss.vs.inputQps").as("qps");
        Aggregation aggregation = Aggregation.newAggregation(match, sort, limit, unwind, group);
        aggregation.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
        List<Map> mappedResults = mongoOperations.aggregate(aggregation, MeasurementEntity.COLLECTION_NAME, Map.class).getMappedResults();
        if (CollectionUtils.isNotEmpty(mappedResults)) {
            Map map = mappedResults.get(0);
            return Optional.ofNullable(map.get("qps")).map(v -> {
                try {
                    return Double.parseDouble(v.toString());
                } catch (NumberFormatException ignored) {
                    return null;
                }
            }).orElse(0D);
        }
        return 0D;
    }

    @Override
    public Map<String, Sample> getDifferenceSamples(MeasurementQueryParam.MeasurementQuerySample querySample, long start, long end) {
        long average = (start + end) / 2;

        Map<String, Sample> endSamples = getInstantSamples(querySample, INSTANT_PADDING_LEFT_AND_RIGHT, average , end);
        Map<String, Sample> startSamples = getInstantSamples(querySample, INSTANT_PADDING_LEFT, start , average);

        Map<String, Sample> data = new HashMap<>();
        for (String hash : endSamples.keySet()) {
            Sample startSample;
            if (startSamples.containsKey(hash)) {
                startSample = startSamples.get(hash);
            } else {
                startSample = new Sample();
            }

            Sample ret = new Sample();
            ret.setVs(new HashMap<>());
            Sample endSample = endSamples.get(hash);
            for (String key : endSample.getVs().keySet()) {
                Number startNum = startSample.getVs() == null ? 0 : startSample.getVs().getOrDefault(key, 0);
                Number endNum = endSample.getVs().get(key);
                Number diff;
                if (ObjectUtils.anyNull(startNum, endNum)) {
                    diff = 0;
                } else {
                    diff = endNum.doubleValue() - startNum.doubleValue();
                }
                ret.getVs().put(key, diff);
            }
            data.put(hash, ret);
        }

        return data;
    }

    private Map<String, List<Sample>> getContinuousSamples(MeasurementQueryParam.MeasurementQuerySample querySample, long start, long end) {
        Map<String, List<Sample>> data = new HashMap<>();
        if (!StringUtils.equalsAny(querySample.getType(),
                MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS)) {
            return data;
        }

        Criteria criteria = Criteria.where(String.format("%s.%s", MeasurementEntity.FIELD_SAMPLES, Sample.FIELD_DATE))
                .gte(new Date(start))
                .lte(new Date(end));
        criteria.and(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.calculateReasonableGranularity(start, end));
        for (Map.Entry<String, String> entry : querySample.getTags().entrySet()) {
            criteria.and(String.format(TAG_FORMAT, entry.getKey())).is(entry.getValue());
        }

        List<String> includedFields = new ArrayList<>();
        includedFields.add(MeasurementEntity.FIELD_TAGS);
        includedFields.add(String.format("%s.%s", MeasurementEntity.FIELD_SAMPLES, Sample.FIELD_DATE));
        for (String field : querySample.getFields()) {
            includedFields.add(String.format(FIELD_FORMAT, field));
        }

        Query query = new Query(criteria);
        query.fields().include(includedFields.toArray(new String[]{}));
        query.with(Sort.by(MeasurementEntity.FIELD_DATE).ascending());
        List<MeasurementEntity> entities = mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
        for (MeasurementEntity entity : entities) {
            String hash = hashTag(entity.getTags());
            if (!data.containsKey(hash)) {
                data.put(hash, new ArrayList<>());
            }
            data.get(hash).addAll(entity.getSamples());
        }

        return data;
    }

    private String hashTag(Map<String, String> tags) {
        StringBuilder sb = new StringBuilder();
        for(String key: tags.keySet().stream().sorted().collect(Collectors.toList())) {
            sb.append(String.format("%s:%s", key, tags.get(key)));
            sb.append(";");
        }

        return sb.toString();
    }

    private Map<String, String> reverseHashTag(String hash) {
        Map<String, String> tags = new HashMap<>();
        for(String pair : hash.split(";")) {
            String[] kv = pair.split(":");
            tags.put(kv[0], kv[1]);
        }

        return tags;
    }

    private List<Map<String, Object>> formatSingleSamples(Map<String, Sample> singleSamples) {
        List<Map<String, Object>> data = new ArrayList<>();
        for(Map.Entry<String, Sample> entry: singleSamples.entrySet()) {
            Map<String, String> tags = reverseHashTag(entry.getKey());
            Map<String, Object> values = new HashMap<>(entry.getValue().getVs());
            values.put("tags", tags);
            data.add(values);
        }

        return data;
    }

    private  List<Map<String, Object>> formatContinuousSamples(Map<String, List<Sample>> continuousSamples, List<Long> timeline, long interval) {
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map.Entry<String, List<Sample>> entry : continuousSamples.entrySet()) {
            Map<String, String> tags = reverseHashTag(entry.getKey());
            Map<String, Number[]> values = new HashMap<>();

            List<Sample> samples = entry.getValue().stream().sorted(Comparator.comparing(Sample::getDate)).collect(Collectors.toList());

            int timeLineIdx = 0;
            int sampleIdx1 = 0;
            while (timeLineIdx < timeline.size() && sampleIdx1 < samples.size()) {
                Sample sample1 = samples.get(sampleIdx1);
                long time1 = sample1.getDate().getTime();
                long gap1 = Math.abs(timeline.get(timeLineIdx) - time1);

                if (gap1 > interval / 2) {
                    if (timeline.get(timeLineIdx) - time1 > 0) {
                        //    s1          s2          s3
                        //    \/          \/          \/
                        // |___*_______|___*_______|___*_______|
                        //             t1          t2          t3
                        // the s1 is left of the ranging area of t1, so s1 is dropped, but we still have to find the
                        // data for t1, so we move the pointer of s to s2 while keeping the pointer of t unchanged.
                        sampleIdx1 += 1;
                    } else {
                        //         s1          s2          s3
                        //         \/          \/          \/
                        // |________*__|________*__|________*__|
                        // t1         t2         t3           t4
                        // the s1 is right of the ranging area of t1, so the t1 does not have a value(aka. null), so
                        // we move the pointer of t to t2 while keeping the pointer of s unchanged.
                        timeLineIdx += 1;
                    }
                    continue;
                }

                //         s1
                //         \/
                // |________*__|__________|__________|
                //             t1          t2          t3
                // the s1 is in the ranging area of t1, so set data of s1 to t1 temporarily.
                for(String key : sample1.getVs().keySet()) {
                    values.putIfAbsent(key, new Number[timeline.size()]);
                    values.get(key)[timeLineIdx] = sample1.getVs().get(key);
                }

                //         s1   s2
                //         \/   \/
                // |________*__|_*_________|__________|
                //             t1          t2         t3
                // s2 may have the shorter distance to t1, we never know, so we should iterate the rest data
                // of s until get out the ranging area of t1.
                boolean skipSet = false;
                int sampleIdx2 = sampleIdx1;
                while(sampleIdx2 < samples.size() - 1) {
                    sampleIdx2 += 1;
                    Sample sample2 = samples.get(sampleIdx2);
                    long time2 = sample2.getDate().getTime();
                    long gap2 = Math.abs(timeline.get(timeLineIdx) - time2);
                    // pointer is out of the t1 ranging area, break the loop
                    if (gap2 > interval / 2) {
                        break;
                    }

                    // the shortest distance data is found, move pointer of s out of the t1 ranging area
                    if (skipSet || gap1 < gap2) {
                        continue;
                    }

                    // set the new value into array, only if the gap is smaller than the former one
                    for(String key : sample2.getVs().keySet()) {
                        values.putIfAbsent(key, new Number[timeline.size()]);
                        values.get(key)[timeLineIdx] = sample2.getVs().get(key);
                    }
                    // got a new value, use the new gap to compare
                    gap1 = gap2;

                    //         s1s2s3 s4 s5
                    //         \/\/\/ \/ \/
                    // |________*_*_*|_*__*_______|__________|
                    //              t1           t2         t3
                    // skip the set only if the pointer of s reaches s4 since the gap is
                    // surely getting bigger.
                    if (timeline.get(timeLineIdx) - time2 < 0) {
                        skipSet = true;
                    }

                }
                timeLineIdx += 1;
                sampleIdx1 = sampleIdx2;
            }

            // 补充null数据，取null上一个点的数据
            for (Map.Entry<String, Number[]> e : values.entrySet()) {
                Number[] v = e.getValue();
                Number last = null;
                for (int i = 0; i < v.length; i++) {
                    if (i == 0) {
                        continue;
                    }

                    if (Objects.isNull(v[i])) {
                        if (Objects.nonNull(last)) {
                            v[i] = last;
                        }
                    } else {
                        last = v[i];
                    }
                }
            }

            Map<String, Object> single = new HashMap<>(values);
            single.put("tags", tags);
            data.add(single);
        }

        return data;
    }

    private List<Long> getTimeline(long start, long end, long interval) {
        // get the time trail with same interval
        List<Long> timeline = new ArrayList<>();
        while (start < end) {
            timeline.add(end);
            end -= interval;
        }

        return timeline.stream().sorted().collect(Collectors.toList());
    }


    @Override
    public void aggregateMeasurement(AggregateMeasurementParam param) {
        if (!param.isGranularityValid()) {
            throw new RuntimeException("invalid value for granularity: " + param.getGranularity());
        }

        if (!param.isStartEndValid()) {
            String msg = "invalid value for start or end, start: %s, end: %s;";
            throw new RuntimeException(String.format(msg, param.getStart(), param.getEnd()));
        }

        for (String granularity : param.getGranularity()) {
            aggregateMeasurementByGranularity(param.getTags(), param.getStart(), param.getEnd(), granularity);
        }
    }


    @Override
    public void aggregateMeasurementByGranularity(Map<String, String> queryTags, long start, long end, String granularity) {
        long interval = Granularity.getGranularityMillisInterval(granularity);
        String nextGranularity = Granularity.getNextLevelGranularity(granularity);

        //      start                    end
        // |______*____|__________|_______*___|
        // t1         t2         t3         t4
        // here we should use t2 as start and t3 as stop.

        // move the start cursor to the next granularity section
        if (start % interval != 0) {
            start = ((start / interval) + 1) * interval;
        }
        // move the start cursor to the former granularity section
        if (end % interval != 0) {
            end = ((end / interval) - 1) * interval;
        }
        // |______*____|_____*_____|__________|
        // t1         t2         t3         t4
        // skip the aggregate since sections are not fully completed
        if (end - start < interval) {
            return;
        }

        // get the sample data in [start, end)
        Criteria criteria = Criteria.where(MeasurementEntity.FIELD_DATE).gte(new Date(start)).lt(new Date(end));
        criteria.and(MeasurementEntity.FIELD_GRANULARITY).is(granularity);
        for (Map.Entry<String, String> entry : queryTags.entrySet()) {
            criteria.and(String.format(TAG_FORMAT, entry.getKey())).is(entry.getValue());
        }

        List<String> includedFields = new ArrayList<>();
        includedFields.add(MeasurementEntity.FIELD_DATE);
        includedFields.add(MeasurementEntity.FIELD_TAGS);
        includedFields.add(MeasurementEntity.FIELD_SAMPLES);

        Query query = new Query(criteria);
        query.fields().include(includedFields.toArray(new String[]{}));
        query.with(Sort.by(MeasurementEntity.FIELD_DATE).ascending());

        Map<String, List<MeasurementEntity>> tagEntities = new HashMap<>();
        for (MeasurementEntity entity : mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME)) {
            String hash = hashTag(entity.getTags());
            tagEntities.putIfAbsent(hash, new ArrayList<>());
            tagEntities.get(hash).add(entity);
        }

        // does not have samples to be aggregated
        if (tagEntities.isEmpty()) {
            return;
        }

        BulkOperations bulkOperations = null;
        for (Map.Entry<String, List<MeasurementEntity>> entry : tagEntities.entrySet()) {
            int idx = 0;

            for (int cnt = 1; cnt <= (end - start) / interval; ++cnt) {
                long innerStart = start + (cnt - 1) * interval;
                long innerEnd = innerStart + interval;

                Long first = null, last = null;
                List<MeasurementEntity> entities = entry.getValue();
                List<Map<String, Object>> samples = new ArrayList<>();
                // the last value should not be included, it should be [start, end)
                for (long time = innerStart; time < innerEnd && idx < entry.getValue().size(); time += interval) {
                    MeasurementEntity entity = entities.get(idx);
                    if (entity.getDate().getTime() > time) {
                        continue;
                    }

                    if (null == first || first > time) {
                        first = time;
                    }
                    if (null == last || last < time) {
                        last = time;
                    }

                    Sample nextGranularitySample = new Sample();
                    nextGranularitySample.setDate(new Date(time));
                    nextGranularitySample.setVs(entity.averageValues());
                    samples.add(nextGranularitySample.toMap());

                    idx += 1;
                }

                if (samples.isEmpty()) {
                    continue;
                }

                Date nextGranularityDate = Granularity.calculateGranularityDate(nextGranularity, new Date(innerStart));

                Criteria upsertCriteria = Criteria.where(MeasurementEntity.FIELD_GRANULARITY).is(nextGranularity);
                upsertCriteria.and(MeasurementEntity.FIELD_TAGS).is(reverseHashTag(entry.getKey()));
                upsertCriteria.and(MeasurementEntity.FIELD_DATE).is(nextGranularityDate);

                Document ss = new Document();
                ss.put("$each", samples);
                // TODO(dexter): find a more elegant way to de-duplicate when value with same date arrives
                // each time we call the function, the inner doc is hard to de-duplicate, here we add
                // $slice=200 to protect the document from being to huge, the duplicate data will not
                // affect the data since the generated value will always be the same.
                ss.put("$slice", 200);
                ss.put("$sort", new Document().append(Sample.FIELD_DATE, -1));
                Update update = new Update().push(MeasurementEntity.FIELD_SAMPLES, ss)
                        .min(MeasurementEntity.FIELD_FIRST, new Date(first))
                        .max(MeasurementEntity.FIELD_LAST, new Date(last));

                if (null == bulkOperations) {
                    bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
                }
                bulkOperations.upsert(new Query(upsertCriteria), update);
            }
        }

        if (null != bulkOperations) {
            bulkOperations.execute();
        }
    }

    @Override
    public void deleteTaskMeasurement(String taskId) {
        if (StringUtils.isEmpty(taskId)) {
            return;
        }
        Query query = Query.query(Criteria.where(TAGS_TASK_ID).is(taskId));
        DeleteResult result = mongoOperations.remove(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);

        log.info(" taskId :{}  删除了 {} 条记录", taskId, JsonUtil.toJson(result));
    }

    @Override
    public Long[] countEventByTaskRecord(String taskId, String taskRecordId) {
        Query query = new Query(Criteria.where(TAGS_TASK_ID).is(taskId)
                .and(TAGS_TASK_RECORD_ID).is(taskRecordId)
                .and(TAGS_TYPE).is("task")
                .and(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)
                .and(MeasurementEntity.FIELD_DATE).lte(new Date()));
        query.with(Sort.by(MeasurementEntity.FIELD_DATE).descending());
        MeasurementEntity measurementEntity = mongoOperations.findOne(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
        if (null == measurementEntity || null == measurementEntity.getSamples() || measurementEntity.getSamples().isEmpty()) {
            return null;
        }

        Map<String, Number> vs = measurementEntity.getSamples().get(0).getVs();
        // inputInsertTotal + inputUpdateTotal + inputDeleteTotal + inputDdlTotal + inputOthersTotal
        AtomicReference<Long> inputTotal = new AtomicReference<>(0L);
        AtomicReference<Long> outputTotal = new AtomicReference<>(0L);
        List<String> calList = Lists.of("inputDdlTotal",
                "inputDeleteTotal",
                "inputInsertTotal",
                "inputOthersTotal",
                "inputUpdateTotal",
                "outputDdlTotal",
                "outputDeleteTotal",
                "outputInsertTotal",
                "outputOthersTotal",
                "outputUpdateTotal");
        vs.forEach((k, v) -> {
            if (calList.contains(k)) {
                Long value = Objects.nonNull(v) ? v.longValue() : 0;
                if (StringUtils.startsWith(k, "input")) {
                    inputTotal.updateAndGet(v1 -> v1 + value);
                } else if (StringUtils.startsWith(k, "output")) {
                    outputTotal.updateAndGet(v1 -> v1 + value);
                }
            }
        });

        return new Long[]{inputTotal.get(), outputTotal.get()};
    }


    @Override
    public List<String> findRunTable(String taskId, String taskRecordId) {
        List<String> runTables = new ArrayList<>();

        if (StringUtils.isBlank(taskRecordId)) {
            return runTables;
        }

        Criteria criteria = Criteria.where(TAGS_TASK_ID).is(taskId)
                .and(TAGS_TASK_RECORD_ID).is(taskRecordId)
                .and(TAGS_TYPE).is(TABLE)
                .and(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);

        Query query = new Query(criteria);
        query.fields().include(TAGS_TABLE);
        List<MeasurementEntity> measurementEntities = mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
        if (CollectionUtils.isNotEmpty(measurementEntities)) {
            for (MeasurementEntity measurementEntity : measurementEntities) {
                Map<String, String> tags = measurementEntity.getTags();
                if (tags != null && tags.get(TABLE) != null) {
                    runTables.add(tags.get(TABLE));
                }
            }
        }

        return runTables;
    }

    @Override
    public Page<TableSyncStaticVo> querySyncStatic(TableSyncStaticDto dto, UserDetail userDetail) {
        String taskRecordId = dto.getTaskRecordId();

        Query taskQuery = new Query(Criteria.where("taskRecordId").is(taskRecordId));
        TaskDto taskDto = taskService.findOne(taskQuery, userDetail);
        if (taskDto == null) {
            return new Page<>(0, Lists.of());
        }

        boolean hasTableRenameNode = false;
        if (CollectionUtils.isNotEmpty(taskDto.getDag().getNodes())) {
            hasTableRenameNode = taskDto.getDag().getNodes().stream().anyMatch(n -> n instanceof TableRenameProcessNode);
        }

        Criteria criteria = Criteria.where(TAGS_TASK_ID).is(taskDto.getId().toHexString())
                .and(TAGS_TASK_RECORD_ID).is(taskRecordId)
                .and(TAGS_TYPE).is(TABLE)
                .and(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);

        querySyncByTableName(dto,criteria);

        TmPageable tmPageable = new TmPageable();
        tmPageable.setPage(dto.getPage());
        tmPageable.setSize(dto.getSize());

        Query query = new Query(criteria);
        long count = mongoOperations.count(query, MeasurementEntity.COLLECTION_NAME);
        if (count == 0) {
            return new Page<>(0, Collections.emptyList());
        }

        query.with(Sort.by(Sort.Direction.DESC, "ss.vs.snapshotSyncRate"));
        query.with(tmPageable);
        List<MeasurementEntity> measurementEntities = mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);

        // get table map from task dag
        AtomicReference<Map<String, String>> tableNameMap = new AtomicReference<>();
        tableNameMap.set(new HashMap<>());
        if (hasTableRenameNode) {
            DatabaseNode targetNode = taskDto.getDag().getTargetNode().getLast();
            List<MetadataInstancesDto> metas = metadataInstancesService.findBySourceIdAndTableNameList(targetNode.getConnectionId(),
                    null, userDetail, taskDto.getId().toHexString());
            // filter by nodeId ,old data nodeId will null
            // get table origin name and target name
            tableNameMap.set(metas.stream()
                    .filter(meta -> Objects.nonNull(meta.getNodeId()) && meta.getNodeId().equals(targetNode.getId()))
                    .collect(Collectors.toMap(MetadataInstancesDto::getAncestorsName, MetadataInstancesDto::getName, (k1, k2) -> k2)));

            metas.stream()
                    .filter(meta -> StringUtils.isNotBlank(meta.getPartitionMasterTableId())
                            && meta.getPartitionInfo() != null
                            && meta.getPartitionInfo().getSubPartitionTableInfo() != null)
                    .forEach(meta -> {
                        meta.getPartitionInfo().getSubPartitionTableInfo().forEach(subPartitionTableInfo -> {
                            tableNameMap.updateAndGet((m) -> {
                                m.put(subPartitionTableInfo.getTableName(), meta.getName());
                                return m;
                            });
                        });
                    });

        }

        List<TableNode> collect = Lists.newArrayList();
        if (TaskDto.SYNC_TYPE_SYNC.equals(taskDto.getSyncType())) {
            collect = taskDto.getDag().getTargets().stream().map(n -> (TableNode) n).collect(Collectors.toList());
        }

        List<TableSyncStaticVo> result = new ArrayList<>();
        for (MeasurementEntity measurementEntity : measurementEntities) {
            String originTable = measurementEntity.getTags().get(TABLE);
            AtomicReference<String> originTableName = new AtomicReference<>();
            boolean finalHasTableRenameNode = hasTableRenameNode;
            List<TableNode> finalCollect = collect;
            FunctionUtils.isTureOrFalse(TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())).trueOrFalseHandle(
                    () -> FunctionUtils.isTureOrFalse(finalHasTableRenameNode).trueOrFalseHandle(
                            () -> originTableName.set(tableNameMap.get().get(originTable)),
                            () -> originTableName.set(originTable)),
                    () -> FunctionUtils.isTureOrFalse(CollectionUtils.isNotEmpty(finalCollect)).trueOrFalseHandle(
                            () -> originTableName.set(finalCollect.get(0).getTableName()),
                            () -> originTableName.set(originTable)
                    )
            );

            List<Sample> samples = measurementEntity.getSamples();
            if (CollectionUtils.isEmpty(samples)) {
                continue;
            }

            Map<String, Number> vs = samples.get(0).getVs();
            AtomicLong snapshotInsertRowTotal = new AtomicLong(0L);
            Optional.ofNullable(vs.getOrDefault("snapshotInsertRowTotal", 0)).ifPresent(number -> snapshotInsertRowTotal.set(number.longValue()));
            AtomicLong snapshotRowTotal = new AtomicLong(0L);
            Optional.ofNullable(vs.getOrDefault("snapshotRowTotal", 0)).ifPresent(number -> snapshotRowTotal.set(number.longValue()));

            Number snapshotSyncRate = vs.get("snapshotSyncRate");
            BigDecimal syncRate;
            if (snapshotRowTotal.get() == -1L && snapshotInsertRowTotal.get() > 0L) {
                syncRate = new BigDecimal(-1);
            } else if (Objects.nonNull(snapshotSyncRate)) {
                syncRate = BigDecimal.valueOf(snapshotSyncRate.doubleValue());
            } else if (snapshotRowTotal.get() != 0L) {
                syncRate = new BigDecimal(snapshotInsertRowTotal.get()).divide(new BigDecimal(snapshotRowTotal.get()), 2, RoundingMode.HALF_UP);
            } else {
                syncRate = BigDecimal.ZERO;
            }

            String fullSyncStatus;
            if (syncRate.compareTo(BigDecimal.ONE) >= 0) {
                fullSyncStatus = "DONE";
                syncRate = BigDecimal.ONE;
            } else if (syncRate.compareTo(BigDecimal.ZERO) == 0) {
                fullSyncStatus = "NOT_START";
            } else if (syncRate.intValue() == -1) {
                fullSyncStatus = "COUNTING";
            } else {
                fullSyncStatus = "ING";
            }

            TableSyncStaticVo vo = new TableSyncStaticVo();
            vo.setOriginTable(originTable);
            vo.setTargetTable(originTableName.get());
            vo.setFullSyncStatus(fullSyncStatus);
            if (syncRate.compareTo(BigDecimal.TEN) > 0) {
                log.warn("querySyncStatic table {} syncRate {} more than 100%", originTableName, syncRate);
                syncRate = new BigDecimal(1);
            }
            vo.setSyncRate(syncRate);

            result.add(vo);
        }
        fixSyncRate(result, taskDto);
        return new Page<>(count, result.stream()
                .sorted(Comparator.comparing(TableSyncStaticVo::getSyncRate).reversed())
                .collect(Collectors.toList()));
    }

    protected void fixSyncRate(List<TableSyncStaticVo> tableSyncStaticVos, TaskDto taskDto) {
        String status = taskDto.getStatus();
        String type = taskDto.getType();
        // task is initial_sync and status is complete, than fix sync rate, Do this for now, and then adjust accordingly
        if (KeyWords.INITIAL_SYNC.equalsIgnoreCase(type) && KeyWords.COMPLETE.equalsIgnoreCase(status)) {
            tableSyncStaticVos.forEach(t -> {
                t.setFullSyncStatus(KeyWords.DONE);
                t.setSyncRate(BigDecimal.ONE);
            });
        }
    }

    public void querySyncByTableName(TableSyncStaticDto dto, Criteria criteria){
        if(StringUtils.isNotBlank(dto.getTableName())){
            criteria.and(TAGS_TABLE).regex(dto.getTableName());
        }
    }

	@Override
    public List<SyncStatusStatisticsVo> queryTableSyncStatusStatistics(SyncStatusStatisticsParam param) {
		AggregationResults<SyncStatusStatisticsVo> aggregationResults = mongoOperations.aggregate(Aggregation.newAggregation(
			Aggregation.match(Criteria.where(TAGS_TASK_ID).is(param.getTaskId())
				.and(TAGS_TASK_RECORD_ID).is(param.getTaskRecordId())
				.and(TAGS_TYPE).is(TABLE)
				.and(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)),
			Aggregation.unwind("$ss"),
			Aggregation.project(Fields.from(
				Fields.field("syncTotal", "$ss.vs.snapshotInsertRowTotal"),
				Fields.field("dataTotal", "$ss.vs.snapshotRowTotal")
			)).and(AggregationExpression.from(MongoExpression.create("{$cond:[{$gte:['$ss.vs.snapshotSyncRate', 1]}, '**Done**', {$cond:[{$lte:['$ss.vs.snapshotSyncRate', 0]}, '**Wait**', '$tags.table']}]}")))
				.as("status"),
			Aggregation.group("status")
				.sum("syncTotal").as("syncTotal")
				.sum("dataTotal").as("dataTotal")
				.count().as("counts")
		), MeasurementEntity.COLLECTION_NAME, SyncStatusStatisticsVo.class);
		return aggregationResults.getMappedResults();
	}

    /**
     * 根据任务id查询得到最近的一条分种类型的统计信息
     * @param taskId 任务id
     * @return MeasurementEntity
     */
    @Override
    public MeasurementEntity findLastMinuteByTaskId(String taskId) {
        Criteria criteria = Criteria.where(TAGS_TASK_ID).is(taskId)
                .and("grnty").is("minute")
                .and(TAGS_TYPE).is("task");

        Query query = new Query(criteria);
        query.fields().include("ss", "tags");
        query.with(Sort.by("date").descending());
        return mongoOperations.findOne(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
    }


    @Override
    public void queryTableMeasurement(String taskId, TableStatusInfoDto tableStatusInfoDto) {
        Criteria criteria = Criteria.where(TAGS_TASK_ID).is(taskId)
                .and("grnty").is("minute")
                .and(TAGS_TYPE).is("task");
        Query query = new Query(criteria);
        query.fields().include("ss", "tags");
        query.with(Sort.by("last").descending());
        MeasurementEntity measurementEntity = mongoOperations.findOne(query, MeasurementEntity.class, "AgentMeasurementV2");
        if (measurementEntity == null) {
            return;
        }
        List<Sample> samples = measurementEntity.getSamples();
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(samples)) {
            Sample sample = samples.get(0);
            Long cdcDelayTime = null;
            Date lastData = null;
            if (sample.getVs().get(REPLICATE_LAG) != null) {
                cdcDelayTime = Long.valueOf(sample.getVs().get(REPLICATE_LAG).toString());
            }
            tableStatusInfoDto.setCdcDelayTime(cdcDelayTime);
            if (sample.getVs().get("currentEventTimestamp") != null) {
                long LastDataChangeTime = sample.getVs().get("currentEventTimestamp").longValue();
                if (LastDataChangeTime != 0) {
                    lastData = new Date(LastDataChangeTime);
                }
            }
            tableStatusInfoDto.setLastDataChangeTime(lastData);
        }

    }

    @Override
    public void cleanRemovedTableMeasurement(String taskId, String taskRecordId, String tableName) {
        Criteria criteria = Criteria.where(TAGS_TASK_ID).is(taskId)
                .and(TAGS_TASK_RECORD_ID).is(taskRecordId)
                .and(TAGS_TYPE).is(TABLE)
                .and(TAGS_TABLE).is(tableName)
                .and(MeasurementEntity.FIELD_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);

        Query query = new Query(criteria);
        mongoOperations.remove(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
    }
}
