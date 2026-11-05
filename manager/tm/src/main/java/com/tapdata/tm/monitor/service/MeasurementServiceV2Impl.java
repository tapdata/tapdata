package com.tapdata.tm.monitor.service;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.MongoBulkWriteException;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.constant.Granularity;
import com.tapdata.tm.monitor.constant.KeyWords;
import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.entity.TDigestEntity;
import com.tapdata.tm.monitor.param.AggregateMeasurementParam;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.param.SyncStatusStatisticsParam;
import com.tapdata.tm.monitor.vo.SyncStatusStatisticsVo;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import com.tapdata.tm.monitor.vo.TaskMetricsTrendVo;
import com.tapdata.tm.task.bean.TableStatusInfoDto;
import com.tapdata.tm.task.res.CpuMemoryService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.TimeUtil;
import java.util.Set;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Collection;
import java.util.Date;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
public class MeasurementServiceV2Impl implements MeasurementServiceV2 {
    private static final int TASK_ID_BATCH_SIZE = 100;
    private static final long LATEST_SAMPLE_LOOKBACK_MILLIS = 5L * 60L * 1000L;
    private static final int TASK_COUNTER_RECOVERY_SCHEMA_VERSION = 1;
    private static final Set<String> TASK_COUNTER_CORE_FIELDS = Set.of(
            MetricCons.SS.VS.F_INPUT_DDL_TOTAL,
            MetricCons.SS.VS.F_INPUT_INSERT_TOTAL,
            MetricCons.SS.VS.F_INPUT_UPDATE_TOTAL,
            MetricCons.SS.VS.F_INPUT_DELETE_TOTAL,
            MetricCons.SS.VS.F_INPUT_OTHERS_TOTAL,
            MetricCons.SS.VS.F_OUTPUT_DDL_TOTAL,
            MetricCons.SS.VS.F_OUTPUT_INSERT_TOTAL,
            MetricCons.SS.VS.F_OUTPUT_UPDATE_TOTAL,
            MetricCons.SS.VS.F_OUTPUT_DELETE_TOTAL,
            MetricCons.SS.VS.F_OUTPUT_OTHERS_TOTAL);

    public static final String SAMPLE_TYPE_TABLE = MetricCons.SampleType.TABLE.code();
    public static final String SAMPLE_TYPE_TASK = MetricCons.SampleType.TASK.code();

    public static final String FIELD_TAGS_TYPE = MetricCons.Tags.F_TYPE;
    public static final String FIELD_TAGS_TABLE = MetricCons.Tags.F_TABLE;
    public static final String FIELD_TAGS_TASK_ID = MetricCons.Tags.F_TASK_ID;
    public static final String FIELD_TAGS_TASK_RECORD_ID = MetricCons.Tags.F_TASK_RECORD_ID;
    public static final String FIELD_TAGS_PHYSICAL_NODE_ID = "physicalNodeId";
    public static final String FIELD_SS_VS_REPLICATE_LAG = MetricCons.SS.VS.F_REPLICATE_LAG;
    public static final String FIELD_SS_VS_CURR_EVENT_TS = MetricCons.SS.VS.F_CURR_EVENT_TS;
    public static final String FIELD_SS_VS_INPUT_QPS = MetricCons.SS.VS.F_INPUT_QPS;
    public static final String FIELD_SS_VS_OUTPUT_QPS = MetricCons.SS.VS.F_OUTPUT_QPS;

    public static final String PATH_TAGS_TASK_ID = MetricCons.Tags.path(FIELD_TAGS_TASK_ID);
    public static final String PATH_TAGS_TASK_RECORD_ID = MetricCons.Tags.path(FIELD_TAGS_TASK_RECORD_ID);
    public static final String PATH_TAGS_TYPE = MetricCons.Tags.path(FIELD_TAGS_TYPE);
    public static final String PATH_TAGS_NODE_ID = MetricCons.Tags.path(MetricCons.Tags.F_NODE_ID);
    public static final String PATH_TAGS_TABLE = MetricCons.Tags.path(FIELD_TAGS_TABLE);

    public static final String PATH_SS_DATE = MetricCons.SS.path(Sample.FIELD_DATE);
    public static final String PATH_SS_VS_INPUT_QPS = MetricCons.SS.VS.path(MetricCons.SS.VS.F_INPUT_QPS);
    public static final String PATH_SS_VS_SNAPSHOT_SYNC_RATE = MetricCons.SS.VS.path(MetricCons.SS.VS.F_SNAPSHOT_SYNC_RATE);

    private final MongoTemplate mongoOperations;
    private final MetadataInstancesService metadataInstancesService;
    private final TaskService taskService;
    private Map<String, Long> taskDelayTimeMap;
    @Autowired
    private CpuMemoryService cpuMemoryService;

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
        BulkOperations bulkOperations = null;
        List<WriteModel<Document>> attributedWrites = new ArrayList<>();
        DateTime date = DateUtil.date();
        Date receivedMinute = TimeUtil.cleanTimeAfterMinute(date);
        Date receivedSecond = TimeUtil.cleanTimeAfterSecond(date);
        for (SampleRequest singleSampleRequest : sampleRequestList) {
            Criteria criteria = Criteria.where(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);

            Map<String, String> tags = singleSampleRequest.getTags();
            if (null == tags || tags.isEmpty()) {
                continue;
            }

            AtomicReference<Sample> requestSample = new AtomicReference<>(singleSampleRequest.getSample());
            Sample sample = requestSample.get();
            boolean attributedTaskSample = SAMPLE_TYPE_TASK.equals(tags.get(FIELD_TAGS_TYPE))
                    && StringUtils.isNotBlank(tags.get(FIELD_TAGS_PHYSICAL_NODE_ID));
            if (attributedTaskSample && sample.getDate() == null) {
                log.warn("Skip task metric sample with physical node {} because its collection time is missing", tags.get(FIELD_TAGS_PHYSICAL_NODE_ID));
                continue;
            }
            if (attributedTaskSample && sample.getDate().after(date)) {
                log.warn("Skip task metric sample with physical node {} because its collection time is in the future", tags.get(FIELD_TAGS_PHYSICAL_NODE_ID));
                continue;
            }

            Date sampleDate = attributedTaskSample ? sample.getDate() : receivedSecond;
            Date documentDate = attributedTaskSample ? TimeUtil.cleanTimeAfterMinute(sampleDate) : receivedMinute;
            if (!SAMPLE_TYPE_TABLE.equals(tags.get(FIELD_TAGS_TYPE))) {
                criteria.and(MetricCons.F_DATE).is(documentDate);
            }

            addTags2Criteria(criteria, tags);

            Query query = Query.query(criteria);
            sample.setDate(sampleDate);
            if (SAMPLE_TYPE_TASK.equals(tags.get(FIELD_TAGS_TYPE))) {
                reuseShareCdcDelayIfNeed(tags, requestSample.get());
            }

            Map<String, Object> sampleMap = requestSample.get().toMap();
            if (attributedTaskSample) {
                String id = attributedTaskMinuteDocumentId(tags, documentDate);
                List<Bson> pipeline = attributedTaskMetricUpdatePipeline(
                        tags, documentDate, sampleMap, sampleDate, sampleDate.before(receivedMinute));
                attributedWrites.add(new UpdateOneModel<>(new Document("_id", id), pipeline,
                        new UpdateOptions().upsert(true)));
            } else {
                if (bulkOperations == null) {
                    bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED,
                            MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
                }
                addLegacySampleToBulk(bulkOperations, query, tags, sampleMap, requestSample.get(), receivedMinute);
            }
            if (SAMPLE_TYPE_TASK.equals(tags.get(FIELD_TAGS_TYPE))) {
                Map<String, Object> vs = (Map) sampleMap.get(Sample.FIELD_VALUES);
                Object replicateLag = vs.get(FIELD_SS_VS_REPLICATE_LAG);
                if(null != replicateLag) {
                    Long taskDelayTime = taskDelayTimeMap.get(tags.get(FIELD_TAGS_TASK_ID));
                    long delayTime = Long.parseLong(replicateLag.toString());
                    if (null == taskDelayTime || taskDelayTime != delayTime){
                        taskDelayTimeMap.put(tags.get(FIELD_TAGS_TASK_ID), delayTime);
                        taskService.updateDelayTime(new ObjectId(tags.get(FIELD_TAGS_TASK_ID)), delayTime);
                    }
                }
            }
        }

        if (bulkOperations != null) {
            bulkOperations.execute();
        }
        if (!attributedWrites.isEmpty()) {
            bulkWriteAttributedTaskMetrics(attributedWrites);
        }
    }

    private void bulkWriteAttributedTaskMetrics(List<WriteModel<Document>> attributedWrites) {
        MongoCollection<Document> collection = mongoOperations.getCollection(MeasurementEntity.COLLECTION_NAME);
        BulkWriteOptions options = new BulkWriteOptions().ordered(true);
        try {
            collection.bulkWrite(attributedWrites, options);
        } catch (MongoBulkWriteException e) {
            if (!isDuplicateKeyOnly(e)) {
                throw e;
            }
            // A concurrent first upsert can win the deterministic _id insert; replay is idempotent.
            collection.bulkWrite(attributedWrites, options);
        }
    }

    private boolean isDuplicateKeyOnly(MongoBulkWriteException exception) {
        return exception.getWriteErrors() != null && !exception.getWriteErrors().isEmpty()
                && exception.getWriteErrors().stream().allMatch(error -> error.getCode() == 11000
                || error.getCode() == 11001 || error.getCode() == 12582);
    }

    private void addLegacySampleToBulk(BulkOperations bulkOperations, Query query, Map<String, String> tags,
                                       Map<String, Object> sampleMap, Sample sample, Date receivedMinute) {
            Document upd = new Document();
            upd.put("$each", Collections.singletonList(sampleMap));
            upd.put("$slice", 200); //为了保护数组过长， 在出bug的情况下
            upd.put("$sort", new Document().append(Sample.FIELD_DATE, -1));

            Update update = new Update()
                    .min(MetricCons.F_FIRST, sample.getDate())
                    .max(MetricCons.F_LAST, sample.getDate());
            if (SAMPLE_TYPE_TABLE.equals(tags.get(FIELD_TAGS_TYPE))) {
                update.set(MetricCons.F_SAMPLES, Collections.singletonList(sampleMap));
                update.set(MetricCons.F_DATE, receivedMinute);
            } else {
                update.push(MetricCons.F_SAMPLES, upd);
            }

            bulkOperations.upsert(query, update);
    }

    static String attributedTaskMinuteDocumentId(Map<String, String> tags, Date documentDate) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(JsonUtil.toJson(new TreeMap<>(tags)).getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(Long.toString(documentDate.getTime()).getBytes(StandardCharsets.UTF_8));
            return "tapdata-task-metric-v2:" + HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    static List<Bson> attributedTaskMetricUpdatePipeline(Map<String, String> tags, Date documentDate,
                                                          Map<String, Object> sampleMap, Date sampleDate, boolean late) {
        Document existingSamples = new Document("$ifNull", Arrays.asList("$" + MetricCons.F_SAMPLES, Collections.emptyList()));
        Document newerSamples = filterSamplesByDate(existingSamples, sampleDate, "$gt");
        Document olderSamples = filterSamplesByDate(existingSamples, sampleDate, "$lt");
        Document retainedSamples = new Document("$slice", Arrays.asList(
                new Document("$concatArrays", Arrays.asList(newerSamples,
                        Collections.singletonList(new Document("$literal", sampleMap)), olderSamples)),
                200));

        Document first = new Document("$min", Arrays.asList(
                new Document("$ifNull", Arrays.asList("$" + MetricCons.F_FIRST, sampleDate)), sampleDate));
        Document last = new Document("$max", Arrays.asList(
                new Document("$ifNull", Arrays.asList("$" + MetricCons.F_LAST, sampleDate)), sampleDate));
        Document dirty = new Document("$or", Arrays.asList(
                new Document("$ifNull", Arrays.asList("$" + MeasurementEntity.FIELD_DASHBOARD_ROLLUP_DIRTY, false)), late));
        Document retainStage = new Document("$set", new Document(MetricCons.F_GRANULARITY,
                new Document("$ifNull", Arrays.asList("$" + MetricCons.F_GRANULARITY, Granularity.GRANULARITY_MINUTE)))
                .append(MetricCons.F_DATE, new Document("$ifNull", Arrays.asList("$" + MetricCons.F_DATE, documentDate)))
                .append(MetricCons.F_TAGS, new Document("$ifNull", Arrays.asList("$" + MetricCons.F_TAGS, new Document("$literal", tags))))
                .append(MetricCons.F_SAMPLES, retainedSamples)
                .append(MetricCons.F_FIRST, first)
                .append(MetricCons.F_LAST, last)
                .append(MeasurementEntity.FIELD_DASHBOARD_ROLLUP_DIRTY, dirty));
        Document summaryStage = new Document("$set",
                new Document(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_QPS_SUM, sumSampleMetric(MetricCons.SS.VS.F_OUTPUT_QPS))
                        .append(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_QPS_COUNT, countSampleMetric(MetricCons.SS.VS.F_OUTPUT_QPS))
                        .append(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_SIZE_QPS_SUM, sumSampleMetric(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS))
                        .append(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_SIZE_QPS_COUNT, countSampleMetric(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS)));
        return Arrays.asList(retainStage, summaryStage);
    }

    private static Document filterSamplesByDate(Document samples, Date sampleDate, String comparison) {
        return new Document("$filter", new Document("input", samples)
                .append("as", "existingSample")
                .append("cond", new Document(comparison, Arrays.asList("$$existingSample." + Sample.FIELD_DATE, sampleDate))));
    }

    private static Document sumSampleMetric(String metricName) {
        Document metricValue = new Document("$ifNull", Arrays.asList(
                "$$dashboardSample." + Sample.FIELD_VALUES + "." + metricName, 0));
        return new Document("$sum", new Document("$map", new Document("input", "$" + MetricCons.F_SAMPLES)
                .append("as", "dashboardSample")
                .append("in", metricValue)));
    }

    private static Document countSampleMetric(String metricName) {
        Document metricValue = new Document("$ifNull", Arrays.asList(
                "$$dashboardSample." + Sample.FIELD_VALUES + "." + metricName, null));
        Document nonNullValue = new Document("$ne", Arrays.asList(metricValue, null));
        return new Document("$size", new Document("$filter", new Document("input", "$" + MetricCons.F_SAMPLES)
                .append("as", "dashboardSample")
                .append("cond", nonNullValue)));
    }

    private void reuseShareCdcDelayIfNeed(Map<String, String> tags, Sample sample) {
        try {
            if (null == tags || null == sample || null == sample.getVs()) {
                return;
            }
            String taskId = tags.get(FIELD_TAGS_TASK_ID);
            if (StringUtils.isBlank(taskId)) {
                return;
            }
            Map<String, Number> vs = sample.getVs();
            Long replicateLag = parseLong(vs.get(FIELD_SS_VS_REPLICATE_LAG));
            Long currentEventTimestamp = parseLong(vs.get(FIELD_SS_VS_CURR_EVENT_TS));
            if (!isIdleTaskSample(vs)) {
                return;
            }

            TaskDto taskDto = taskService.findByTaskId(new ObjectId(taskId), "_id", "shareCdcEnable", "shareCdcTaskId", "syncType");
            if (!isShareCdcNormalTask(taskDto)) {
                return;
            }

            Long delayTime = findShareCdcDelaySnapshot(taskDto.getShareCdcTaskId(), currentEventTimestamp);
            if (null == delayTime || (null != replicateLag && delayTime >= replicateLag)) {
                return;
            }
            vs.put(FIELD_SS_VS_REPLICATE_LAG, delayTime);
        } catch (Exception e) {
            log.warn("Reuse share cdc delay failed, task tags: {}, error: {}", tags, e.getMessage());
        }
    }

    private boolean isShareCdcNormalTask(TaskDto taskDto) {
        return null != taskDto
                && Boolean.TRUE.equals(taskDto.getShareCdcEnable())
                && !TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(taskDto.getSyncType())
                && null != taskDto.getShareCdcTaskId()
                && !taskDto.getShareCdcTaskId().isEmpty();
    }

    private Long findShareCdcDelaySnapshot(Map<String, String> shareCdcTaskId, Long currentEventTimestamp) {
        long maxDelayTime = 0L;
        boolean found = false;
        for (String logCollectorTaskId : shareCdcTaskId.values()) {
            if (StringUtils.isBlank(logCollectorTaskId)) {
                return null;
            }
            TaskDto logCollectorTask = taskService.findByTaskId(new ObjectId(logCollectorTaskId), "_id", "status", FIELD_SS_VS_CURR_EVENT_TS, "delayTime");
            if (null == logCollectorTask
                    || !TaskDto.STATUS_RUNNING.equals(logCollectorTask.getStatus())
                    || null == logCollectorTask.getCurrentEventTimestamp()
                    || (null != currentEventTimestamp && logCollectorTask.getCurrentEventTimestamp() < currentEventTimestamp)) {
                return null;
            }
            maxDelayTime = Math.max(maxDelayTime, Math.max(0L, logCollectorTask.getDelayTime()));
            found = true;
        }
        return found ? maxDelayTime: null;
    }

    private boolean isIdleTaskSample(Map<String, Number> vs) {
        Double inputQps = parseDouble(vs.get(FIELD_SS_VS_INPUT_QPS));
        Double outputQps = parseDouble(vs.get(FIELD_SS_VS_OUTPUT_QPS));
        return null != inputQps && inputQps <= 0 && null != outputQps && outputQps <= 0;
    }

    private Double parseDouble(Object value) {
        if (null == value) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private Long parseLong(Object value) {
        if (null == value) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static final String INSTANT_PADDING_LEFT = "left";
    private static final String INSTANT_PADDING_RIGHT = "right";
    private static final String INSTANT_PADDING_LEFT_AND_RIGHT = "leftAndRight";


    @Override
    public Object getSamples(MeasurementQueryParam measurementQueryParam) {
        Map<String, Object> ret = new HashMap<>();
        Map<String, List<Map<String, Object>>> data = new HashMap<>();
        cpuMemoryService.ignoreMeasureInfoIfNeed(measurementQueryParam, ret);

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
                    Map<String, MeasurementEntity>  continuousSamples = getContinuousSamples(querySample, start, end, granularity);

                    // calculate the last point value
                    if (!granularity.equals(Granularity.GRANULARITY_MINUTE)) {
                        String previousGranularity = Granularity.getPreviousLevelGranularity(granularity);
                        long current = ((System.currentTimeMillis() / timelineInterval)) * timelineInterval;
                        for(int idx = 1; idx < timeline.size(); ++idx) {
                            if (current > timeline.get(idx-1) && current <= timeline.get(idx)) {
                                long time = timeline.get(idx);
                                // get the sample data in [start, end)
                                Criteria criteria = Criteria.where(MetricCons.F_DATE).gte(new Date(time)).lt(new Date(time + timelineInterval));
                                criteria.and(MetricCons.F_GRANULARITY).is(previousGranularity);
                                addTags2Criteria(criteria, querySample.getTags());

                                List<String> includedFields = new ArrayList<>();
                                includedFields.add(MetricCons.F_DATE);
                                includedFields.add(MetricCons.F_TAGS);
                                includedFields.add(MetricCons.F_GRANULARITY);
                                includedFields.add(MetricCons.F_DIGEST);
                                includedFields.add(MetricCons.SS.F_DATE);
                                for (String field : querySample.getFields()) {
                                    String keyPath = MetricCons.SS.VS.path(field);
                                    includedFields.add(keyPath);
                                }

                                Query query = new Query(criteria);
                                query.fields().include(includedFields.toArray(new String[]{}));
                                query.with(Sort.by(MetricCons.F_DATE).ascending());

                                List<MeasurementEntity> measurementEntities = mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
                                for (MeasurementEntity entity : measurementEntities) {
                                    String hash = hashTag(entity.getTags());
                                    continuousSamples.putIfAbsent(hash, new MeasurementEntity(new ArrayList<>(),new ArrayList<>()));
                                    Sample sample = new Sample();
                                    sample.setDate(new Date(timeline.get(idx)));
                                    sample.setVs(entity.averageValues());
                                    TDigestEntity tDigestEntity = new TDigestEntity();
                                    tDigestEntity.setDate(new Date(timeline.get(idx)));
                                    tDigestEntity.setDigest(entity.getDigestBytes());
                                    continuousSamples.get(hash).getSamples().add(sample);
                                    continuousSamples.get(hash).getDigest().add(tDigestEntity);
                                }
                                break;
                            }
                        }
                    }

                    List<Map<String, Object>> formatContinuousSamples = formatContinuousSamples(granularity,continuousSamples, timeline, timelineInterval);
                    uniqueData.addAll(formatContinuousSamples);
                    break;
            }
        }

        fillNodeCurrentEventTimestampIfNeed(data, measurementQueryParam);

        ret.put("samples", data);
        if (hasTimeline && null != timeline) {
            ret.put("time", timeline);
            ret.put("interval", timelineInterval);
        }

        return ret;
    }

    @Override
    public Object getSamples(MeasurementQueryParam measurementQueryParam, boolean taskCounterRecovery) {
        MeasurementQueryParam.MeasurementQuerySample recoverySample = taskCounterRecovery
                ? validTaskCounterRecoverySample(measurementQueryParam)
                : null;
        if (recoverySample == null) {
            return getSamples(measurementQueryParam);
        }
        return getTaskCounterRecoverySamples(measurementQueryParam, recoverySample);
    }

    private MeasurementQueryParam.MeasurementQuerySample validTaskCounterRecoverySample(
            MeasurementQueryParam queryParam) {
        if (queryParam == null || queryParam.getStartAt() == null || queryParam.getEndAt() == null
                || queryParam.getStartAt() > queryParam.getEndAt()
                || queryParam.getSamples() == null || queryParam.getSamples().size() != 1) {
            return null;
        }
        MeasurementQueryParam.MeasurementQuerySample sample = queryParam.getSamples().values().iterator().next();
        if (sample == null || !MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT
                .equals(sample.getType()) || sample.getTags() == null || CollectionUtils.isEmpty(sample.getFields())
                || !sample.getFields().containsAll(TASK_COUNTER_CORE_FIELDS)) {
            return null;
        }
        Map<String, String> tags = sample.getTags();
        if (!SAMPLE_TYPE_TASK.equals(tags.get(FIELD_TAGS_TYPE))
                || StringUtils.isBlank(tags.get(FIELD_TAGS_TASK_ID))
                || StringUtils.isBlank(tags.get(FIELD_TAGS_TASK_RECORD_ID))) {
            return null;
        }
        return sample;
    }

    private Object getTaskCounterRecoverySamples(MeasurementQueryParam queryParam,
                                                MeasurementQueryParam.MeasurementQuerySample querySample) {
        String unique = queryParam.getSamples().entrySet().stream()
                .filter(entry -> entry.getValue() == querySample)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow();
        Map<String, List<Map<String, Object>>> data = new HashMap<>();
        List<Map<String, Object>> resultSamples = new ArrayList<>();
        data.put(unique, resultSamples);
        Sample latest = findLatestTaskCounterSample(querySample.getTags(), queryParam.getStartAt(),
                queryParam.getEndAt(), querySample.getFields());
        if (latest != null) {
            validateTaskCounterCoreFields(latest, querySample.getTags());
            Map<String, Number> selectedValues = new HashMap<>();
            for (String field : querySample.getFields()) {
                if (latest.getVs().containsKey(field)) {
                    selectedValues.put(field, latest.getVs().get(field));
                }
            }
            Sample selected = new Sample();
            selected.setDate(latest.getDate());
            selected.setVs(selectedValues);
            resultSamples.addAll(formatSingleSamples(Map.of(hashTag(querySample.getTags()), selected)));
        }
        return Map.of("taskCounterRecoverySchemaVersion", TASK_COUNTER_RECOVERY_SCHEMA_VERSION,
                "samples", data);
    }

    private void validateTaskCounterCoreFields(Sample sample, Map<String, String> tags) {
        for (String field : TASK_COUNTER_CORE_FIELDS) {
            Number value = sample.getVs().get(field);
            if (value == null || !Double.isFinite(value.doubleValue())) {
                throw new IllegalStateException("Invalid task counter sample field " + field + " for taskId="
                        + tags.get(FIELD_TAGS_TASK_ID) + ", taskRecordId=" + tags.get(FIELD_TAGS_TASK_RECORD_ID));
            }
        }
    }

    private Sample findLatestTaskCounterSample(Map<String, String> tags, long startAt, long endAt,
                                               List<String> fields) {
        long effectiveEnd = Math.min(endAt, System.currentTimeMillis());
        if (startAt > effectiveEnd) {
            return null;
        }
        long minuteMillis = Granularity.getGranularityMillisInterval(Granularity.GRANULARITY_MINUTE);
        long firstMinute = startAt - startAt % minuteMillis;
        long lastMinute = effectiveEnd - effectiveEnd % minuteMillis;
        Criteria criteria = Criteria.where(PATH_TAGS_TYPE).is(SAMPLE_TYPE_TASK)
                .and(PATH_TAGS_TASK_ID).is(tags.get(FIELD_TAGS_TASK_ID))
                .and(PATH_TAGS_TASK_RECORD_ID).is(tags.get(FIELD_TAGS_TASK_RECORD_ID))
                .and(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)
                .and(MetricCons.F_DATE).gte(new Date(firstMinute)).lte(new Date(lastMinute));
        Query query = new Query(criteria);
        query.fields().include(MetricCons.F_DATE, MetricCons.F_TAGS, MetricCons.F_SAMPLES);
        query.with(Sort.by(Sort.Direction.DESC, MetricCons.F_DATE));

        Long latestMinute = null;
        long latestSampleTime = Long.MIN_VALUE;
        Sample latestSample = null;
        Set<Long> conflictingTimestamps = new HashSet<>();
        try (Stream<MeasurementEntity> stream = mongoOperations.stream(query, MeasurementEntity.class,
                MeasurementEntity.COLLECTION_NAME)) {
            Iterator<MeasurementEntity> iterator = stream.iterator();
            while (iterator.hasNext()) {
                MeasurementEntity entity = iterator.next();
                if (entity == null || entity.getDate() == null || CollectionUtils.isEmpty(entity.getSamples())) {
                    continue;
                }
                long documentMinute = entity.getDate().getTime();
                if (latestMinute != null && documentMinute < latestMinute) {
                    break;
                }
                for (Sample sample : entity.getSamples()) {
                    if (sample == null || sample.getDate() == null || sample.getVs() == null
                            || sample.getVs().isEmpty() || fields.stream().noneMatch(sample.getVs()::containsKey)) {
                        continue;
                    }
                    long sampleTime = sample.getDate().getTime();
                    if (sampleTime < startAt || sampleTime > effectiveEnd) {
                        continue;
                    }
                    if (latestMinute == null) {
                        latestMinute = documentMinute;
                    }
                    if (documentMinute != latestMinute) {
                        continue;
                    }
                    if (sampleTime > latestSampleTime) {
                        latestSampleTime = sampleTime;
                        latestSample = sample;
                    } else if (sampleTime == latestSampleTime
                            && !sameSampleValues(latestSample, sample, fields)) {
                        conflictingTimestamps.add(sampleTime);
                    }
                }
            }
        }
        if (conflictingTimestamps.contains(latestSampleTime)) {
            throw new IllegalStateException("Conflicting task counter samples at the same timestamp for taskId="
                    + tags.get(FIELD_TAGS_TASK_ID) + ", taskRecordId=" + tags.get(FIELD_TAGS_TASK_RECORD_ID)
                    + ", sampleDate=" + latestSampleTime);
        }
        return latestSample;
    }

    private boolean sameSampleValues(Sample left, Sample right, List<String> fields) {
        if (left == null || right == null) {
            return false;
        }
        for (String key : fields) {
            if (left.getVs().containsKey(key) != right.getVs().containsKey(key)) {
                return false;
            }
            if (!left.getVs().containsKey(key)) {
                continue;
            }
            Number leftValue = left.getVs().get(key);
            Number rightValue = right.getVs().get(key);
            if (leftValue == null || rightValue == null) {
                if (leftValue != rightValue) {
                    return false;
                }
            } else if (!sameNumberValue(leftValue, rightValue)) {
                return false;
            }
        }
        return true;
    }

    private boolean sameNumberValue(Number left, Number right) {
        try {
            return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString())) == 0;
        } catch (NumberFormatException e) {
            return Double.compare(left.doubleValue(), right.doubleValue()) == 0;
        }
    }

    private void fillNodeCurrentEventTimestampIfNeed(Map<String, List<Map<String, Object>>> data, MeasurementQueryParam measurementQueryParam) {
        if (data == null || data.isEmpty() || CollectionUtils.isNotEmpty(data.get("data")) || measurementQueryParam == null || measurementQueryParam.getSamples() == null) {
            return;
        }

        Map<String, TaskDto> taskNodeTsCache = new HashMap<>();
        for (String unique : measurementQueryParam.getSamples().keySet()) {
            MeasurementQueryParam.MeasurementQuerySample querySample = measurementQueryParam.getSamples().get(unique);
            if (querySample == null || querySample.getTags() == null) {
                continue;
            }

            String sampleTagsType = querySample.getTags().get(FIELD_TAGS_TYPE);
            boolean isNode = "node".equals(sampleTagsType);
            boolean isTask = "task".equals(sampleTagsType);

            if ((!isNode && !isTask) 
                    || !MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT.equals(querySample.getType())) {
                continue;
            }

            String taskId = querySample.getTags().get(FIELD_TAGS_TASK_ID);
            if (StringUtils.isBlank(taskId)) {
                continue;
            }

            TaskDto taskDto = getCachedTaskDto(taskNodeTsCache, taskId);
            if (taskDto == null) {
                continue;
            }

            List<Map<String, Object>> uniqueData = data.get(unique);
            if (uniqueData == null) {
                uniqueData = new ArrayList<>();
                data.put(unique, uniqueData);
            }

            Long taskSnapshotDoneAt = taskDto.getSnapshotDoneAt();
            String syncType = taskDto.getSyncType();

            Long taskSnapshotProgress = null;
            Object milestoneObj = taskDto.getAttrs().get("milestone");
            if (milestoneObj instanceof Map) {
                Map<String, Object> milestoneMap = (Map<String, Object>) milestoneObj;
                Object snapshotObj = milestoneMap.get("SNAPSHOT");
                if (snapshotObj instanceof Map) {
                    Map<String, Object> snapshotMap = (Map<String, Object>) snapshotObj;
                    Object progress = snapshotMap.get("progress");
                    if (progress instanceof Number) {
                        taskSnapshotProgress = ((Number)progress).longValue();
                    }
                }
            }

            if (isTask) {
                Long taskTs = taskDto.getCurrentEventTimestamp();
                Map<String, Object> foundSample = null;
                if (!uniqueData.isEmpty()) {
                    foundSample = uniqueData.get(0);
                }

                if (foundSample != null) {
                    Object currTsObj = foundSample.get(FIELD_SS_VS_CURR_EVENT_TS);
                    if (taskTs != null && (currTsObj == null || (currTsObj instanceof Number && ((Number) currTsObj).longValue() <= 0))) {
                        foundSample.put(FIELD_SS_VS_CURR_EVENT_TS, taskTs);
                    }

                    putSnapshotSamples(foundSample, taskSnapshotDoneAt);
                } else {
                    Map<String, Object> newSample = new HashMap<>();
                    Map<String, String> newTags = new HashMap<>(querySample.getTags());
                    newSample.put(MetricCons.F_TAGS, newTags);
                    if (taskTs != null) {
                        newSample.put(FIELD_SS_VS_CURR_EVENT_TS, taskTs);
                    }
                    if (taskSnapshotDoneAt != null) {
                        newSample.put(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT, taskSnapshotDoneAt);
                    }
                    if (taskSnapshotProgress != null) {
                        newSample.put(MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL, taskSnapshotProgress);
                    }
                    uniqueData.add(newSample);
                }
            } else {
                if (taskDto.getNodeCurrentEventTimestamp() == null || taskDto.getNodeCurrentEventTimestamp().isEmpty()) {
                    continue;
                }

                Map<String, Long> nodeTsMap = taskDto.getNodeCurrentEventTimestamp();
                String reqNodeId = querySample.getTags().get(MetricCons.Tags.F_NODE_ID);
                Set<String> targetNodeIds;
                if (StringUtils.isNotBlank(reqNodeId)) {
                    targetNodeIds = Collections.singleton(reqNodeId);
                } else {
                    targetNodeIds = nodeTsMap.keySet();
                }

                for (String nodeId : targetNodeIds) {
                    Long taskTs = nodeTsMap.get(nodeId);
                    if (taskTs == null || taskTs <= 0) {
                        continue;
                    }

                    Map<String, Object> foundSample = null;
                    for (Map<String, Object> sampleMap : uniqueData) {
                        Map<String, String> tags = (Map<String, String>) sampleMap.get(MetricCons.F_TAGS);
                        if (tags != null && nodeId.equals(tags.get(MetricCons.Tags.F_NODE_ID))) {
                            foundSample = sampleMap;
                            break;
                        }
                    }

                    if (foundSample != null) {
                        Object currTsObj = foundSample.get(FIELD_SS_VS_CURR_EVENT_TS);
                        if (currTsObj == null || (currTsObj instanceof Number && ((Number) currTsObj).longValue() <= 0)) {
                            foundSample.put(FIELD_SS_VS_CURR_EVENT_TS, taskTs);
                        }

                        putSnapshotSamples(foundSample, taskSnapshotDoneAt);
                    } else {
                        Map<String, Object> newSample = new HashMap<>();
                        Map<String, String> newTags = new HashMap<>(querySample.getTags());
                        newTags.put(MetricCons.Tags.F_NODE_ID, nodeId);

                        if (TaskDto.SYNC_TYPE_MIGRATE.equals(syncType)) {
                            newTags.put("nodeType", "database");
                        } else if (TaskDto.SYNC_TYPE_SYNC.equals(syncType)) {
                            newTags.put("nodeType", "table");
                        }

                        newSample.put(MetricCons.F_TAGS, newTags);
                        newSample.put(FIELD_SS_VS_CURR_EVENT_TS, taskTs);
                        if (taskSnapshotDoneAt != null) {
                            newSample.put(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT, taskSnapshotDoneAt);
                        }
                        if (taskSnapshotProgress != null) {
                            newSample.put(MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL, taskSnapshotProgress);
                        }

                        uniqueData.add(newSample);
                    }
                }
            }
        }
    }

    private static void putSnapshotSamples(Map<String, Object> foundSample, Long taskSnapshotDoneAt) {
        Object snapDoneObj = foundSample.get(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT);
        if (taskSnapshotDoneAt != null && (snapDoneObj == null || (snapDoneObj instanceof Number && ((Number) snapDoneObj).longValue() <= 0))) {
            foundSample.put(MetricCons.SS.VS.F_SNAPSHOT_DONE_AT, taskSnapshotDoneAt);
        }

        Object snapTableTotalObj = foundSample.get(MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL);
        if (snapTableTotalObj != null && (snapTableTotalObj == null || (snapTableTotalObj instanceof Number && ((Number) snapTableTotalObj).longValue() <= 0))) {
            foundSample.put(MetricCons.SS.VS.F_SNAPSHOT_TABLE_TOTAL, foundSample.get(MetricCons.SS.VS.F_TABLE_TOTAL));
        }
    }

    private TaskDto getCachedTaskDto(Map<String, TaskDto> cache, String taskId) {
        if (cache.containsKey(taskId)) {
            return cache.get(taskId);
        }

        try {
            TaskDto taskDto = taskService.findByTaskId(new org.bson.types.ObjectId(taskId), "nodeCurrentEventTimestamp", "syncType", "snapshotDoneAt", "currentEventTimestamp", "attrs.milestone");
            if (taskDto != null) {
                cache.put(taskId, taskDto);
                return taskDto;
            }
        } catch (Exception e) {
            log.warn("Query task nodeCurrentEventTimestamp, syncType, snapshotDoneAt and currentEventTimestamp failed, taskId: {}, error: {}", taskId, e.getMessage());
        }

        cache.put(taskId, null);
        return null;
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

        Criteria criteria = Criteria.where(MetricCons.F_DATE);

        boolean typeIsTask = false;
        boolean typeIsNode = false;
        boolean typeIsEngine = false;
        String taskId = "";
        String taskRecordId = "";
        for (Map.Entry<String, String> entry : querySample.getTags().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            addTags2Criteria(criteria, key, value);

            switch (key) {
                case FIELD_TAGS_TYPE -> {
                    if (MetricCons.SampleType.TASK.check(value)) {
                        typeIsTask = true;
                    } else if (MetricCons.SampleType.ENGINE.check(value)) {
                        typeIsEngine = true;
                    } else if (MetricCons.SampleType.NODE.check(value)) {
                        typeIsNode = true;
                    }
                }
                case FIELD_TAGS_TASK_ID -> taskId = value;
                case FIELD_TAGS_TASK_RECORD_ID -> taskRecordId = value;
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
        criteria.and(MetricCons.F_GRANULARITY).is(granularity);
        SortOperation sort;
        long time;
        boolean asc;
        if (padding.equals(INSTANT_PADDING_LEFT)) {
            time = startDate.get().getTime();
            sort = Aggregation.sort(Sort.by(MetricCons.F_DATE).ascending());
            asc = true;
        } else {
            time = endDate.get().getTime();
            sort = Aggregation.sort(Sort.by(MetricCons.F_DATE).descending());
            asc = false;
        }

        MatchOperation match = Aggregation.match(criteria);
        LimitOperation limitOperation = Aggregation.limit(1L);
        AggregationOperation projectionOperation = new AggregationOperation() {
            @Override
            public Document toDocument(AggregationOperationContext context) {
                Document projectFields = new Document();
                projectFields.put(MetricCons.F_ID, " $" + MetricCons.F_TAGS);
                projectFields.put(MetricCons.F_DATE,1);
                projectFields.put(MetricCons.F_TAGS,1);
                projectFields.put(MetricCons.F_SAMPLES,1);
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
                GroupOperation groupOperation = Aggregation.group(PATH_TAGS_NODE_ID).first("$$ROOT").as("firstRecord");;
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
                Number currentEventTimestamp = values.get(FIELD_SS_VS_CURR_EVENT_TS);
                Number snapshotStartAt = values.get(MetricCons.SS.VS.F_SNAPSHOT_START_AT);
                // 按照延迟逻辑,源端无事件时,应该为全量同步开始到现在的时间差
                if (Objects.isNull(currentEventTimestamp) && Objects.nonNull(snapshotStartAt)) {
                    Number maxRep = Math.abs(System.currentTimeMillis() - snapshotStartAt.longValue());
                    values.put(FIELD_SS_VS_REPLICATE_LAG, maxRep);
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
        addTags2Criteria(criteria, tags);
        criteria.and(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);
        MatchOperation match = Aggregation.match(criteria);
        SortOperation sort = Aggregation.sort(Sort.by(Sort.Direction.DESC, MetricCons.F_DATE));
        LimitOperation limit = Aggregation.limit(5);
        UnwindOperation unwind = Aggregation.unwind(MetricCons.F_SAMPLES, false);
        GroupOperation group = Aggregation.group().avg(PATH_SS_VS_INPUT_QPS).as("qps");
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

    protected Map<String, MeasurementEntity> getContinuousSamples(MeasurementQueryParam.MeasurementQuerySample querySample, long start, long end,String granularity) {
        Map<String, MeasurementEntity>data = new HashMap<>();
        if (!StringUtils.equalsAny(querySample.getType(),
                MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_CONTINUOUS)) {
            return data;
        }

        Criteria criteria = Criteria.where(PATH_SS_DATE)
                .gte(new Date(start))
                .lte(new Date(end));
        criteria.and(MetricCons.F_GRANULARITY).is(Granularity.calculateReasonableGranularity(start, end));
        addTags2Criteria(criteria, querySample.getTags());

        List<String> includedFields = new ArrayList<>();
        includedFields.add(MetricCons.F_TAGS);
        includedFields.add(PATH_SS_DATE);
        if (!granularity.equals(Granularity.GRANULARITY_MINUTE)) {
            querySample.getFields().add(MetricCons.SS.VS.F_MAX_INPUT_QPS);
            querySample.getFields().add(MetricCons.SS.VS.F_MAX_OUTPUT_QPS);
            querySample.getFields().add(MetricCons.SS.VS.F_MAX_INPUT_SIZE_QPS);
            querySample.getFields().add(MetricCons.SS.VS.F_MAX_OUTPUT_SIZE_QPS);
            includedFields.add(MetricCons.F_DIGEST);
        }
        for (String field : querySample.getFields()) {
            String keyPath = MetricCons.SS.VS.path(field);
            includedFields.add(keyPath);
        }

        Query query = new Query(criteria);
        query.fields().include(includedFields.toArray(new String[]{}));
        query.with(Sort.by(MetricCons.F_DATE).ascending());
        List<MeasurementEntity> entities = mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
        for (MeasurementEntity entity : entities) {
            String hash = hashTag(entity.getTags());
            if (!data.containsKey(hash)) {
                data.put(hash, new MeasurementEntity(new ArrayList<>(),new ArrayList<>()));
            }
            data.get(hash).getSamples().addAll(entity.getSamples());
            if(Granularity.isCalculateQuantiles(granularity) && null != entity.getDigest()){
                data.get(hash).getDigest().addAll(entity.getDigest());
            }
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
            values.put(MetricCons.F_TAGS, tags);
            data.add(values);
        }

        return data;
    }

    private  List<Map<String, Object>> formatContinuousSamples(String granularity , Map<String,MeasurementEntity>  continuousSamples, List<Long> timeline, long interval) {
        List<Map<String, Object>> data = new ArrayList<>();
        for (Map.Entry<String, MeasurementEntity> entry : continuousSamples.entrySet()) {
            Map<String, String> tags = reverseHashTag(entry.getKey());
            Map<String, Number[]> values = new HashMap<>();

            List<Sample> samples = entry.getValue().getSamples().stream().sorted(Comparator.comparing(Sample::getDate)).collect(Collectors.toList());
            List<TDigestEntity> digests = entry.getValue().getDigest().stream().sorted(Comparator.comparing(TDigestEntity::getDate)).collect(Collectors.toList());

            int timeLineIdx = 0;
            int sampleIdx1 = 0;
            Map<Integer,Sample> finalSample = new HashMap<>();
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
                finalSample.put(timeLineIdx,sample1);

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
                    finalSample.put(timeLineIdx,sample2);
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

            Granularity.quantileCalculation(granularity,finalSample.values().stream().toList(),digests);
            for (Map.Entry<Integer,Sample> sampleEntry: finalSample.entrySet()){
                for(String key : sampleEntry.getValue().getVs().keySet()) {
                    values.putIfAbsent(key, new Number[timeline.size()]);
                    values.get(key)[sampleEntry.getKey()] = sampleEntry.getValue().getVs().get(key);
                }
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
            single.put(MetricCons.F_TAGS, tags);
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
        Criteria criteria = Criteria.where(MetricCons.F_DATE).gte(new Date(start)).lt(new Date(end));
        criteria.and(MetricCons.F_GRANULARITY).is(granularity);
        addTags2Criteria(criteria, queryTags);

        List<String> includedFields = new ArrayList<>();
        includedFields.add(MetricCons.F_DATE);
        includedFields.add(MetricCons.F_TAGS);
        includedFields.add(MetricCons.F_SAMPLES);
        includedFields.add(MetricCons.F_DIGEST);
        includedFields.add(MetricCons.F_GRANULARITY);
        if (Granularity.GRANULARITY_MINUTE.equals(granularity)) {
            includedFields.add(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_QPS_SUM);
            includedFields.add(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_QPS_COUNT);
            includedFields.add(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_SIZE_QPS_SUM);
            includedFields.add(MeasurementEntity.FIELD_DASHBOARD_OUTPUT_SIZE_QPS_COUNT);
        }

        Query query = new Query(criteria);
        query.fields().include(includedFields.toArray(new String[]{}));
        query.with(Sort.by(MetricCons.F_DATE).ascending());

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
        List<WriteModel<Document>> attributedHourRollupWrites = new ArrayList<>();
        for (Map.Entry<String, List<MeasurementEntity>> entry : tagEntities.entrySet()) {
            int idx = 0;

            for (int cnt = 1; cnt <= (end - start) / interval; ++cnt) {
                long innerStart = start + (cnt - 1) * interval;
                long innerEnd = innerStart + interval;

                Long first = null, last = null;
                List<MeasurementEntity> entities = entry.getValue();
                List<Map<String, Object>> samples = new ArrayList<>();
                List<Map<String, Object>> digests = new ArrayList<>();
                Map<Long, MeasurementEntity.DashboardMinuteSummary> minuteSummaries = new HashMap<>();
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
                    if (Granularity.GRANULARITY_MINUTE.equals(granularity)
                            && isAttributedTaskMetric(entity.getTags())) {
                        MeasurementEntity.DashboardMinuteSummary summary = dashboardMinuteSummary(entity, new Date(time));
                        mergeDashboardMinuteSummary(minuteSummaries, summary);
                    }
                    TDigestEntity nextGranularityDigest = new TDigestEntity();
                    nextGranularityDigest.setDate(new Date(time));
                    nextGranularityDigest.setDigest(entity.getDigestBytes());
                    digests.add(nextGranularityDigest.toMap());
                    idx += 1;
                }

                if (samples.isEmpty()) {
                    continue;
                }

                Date nextGranularityDate = Granularity.calculateGranularityDate(nextGranularity, new Date(innerStart));

                Criteria upsertCriteria = Criteria.where(MetricCons.F_GRANULARITY).is(nextGranularity);
                upsertCriteria.and(MetricCons.F_TAGS).is(reverseHashTag(entry.getKey()));
                upsertCriteria.and(MetricCons.F_DATE).is(nextGranularityDate);
                Query upsertQuery = new Query(upsertCriteria);

                Map<String, String> tags = reverseHashTag(entry.getKey());
                if (Granularity.GRANULARITY_MINUTE.equals(granularity) && isAttributedTaskMetric(tags)) {
                    MongoCollection<Document> collection = mongoOperations.getCollection(MeasurementEntity.COLLECTION_NAME);
                    Document existing = collection.find(upsertQuery.getQueryObject())
                            .projection(new Document("_id", 1)).first();
                    Object targetId = existing == null
                            ? attributedTaskHourDocumentId(tags, nextGranularityDate)
                            : existing.get("_id");
                    attributedHourRollupWrites.add(new UpdateOneModel<>(new Document("_id", targetId),
                            attributedTaskHourRollupPipeline(tags, nextGranularityDate, samples, digests,
                                    minuteSummaries.values(), first, last), new UpdateOptions().upsert(true)));
                    continue;
                }

                Document ss = new Document();
                ss.put("$each", samples);
                // TODO(dexter): find a more elegant way to de-duplicate when value with same date arrives
                // each time we call the function, the inner doc is hard to de-duplicate, here we add
                // $slice=200 to protect the document from being to huge, the duplicate data will not
                // affect the data since the generated value will always be the same.
                ss.put("$slice", 200);
                ss.put("$sort", new Document().append(Sample.FIELD_DATE, -1));
                Document digest = new Document();
                digest.put("$each", digests);
                digest.put("$slice", 200);
                digest.put("$sort", new Document().append(TDigestEntity.FIELD_DATE, -1));
                Update update = new Update().push(MetricCons.F_SAMPLES, ss)
                        .push(MetricCons.F_DIGEST, digest)
                        .min(MetricCons.F_FIRST, new Date(first))
                        .max(MetricCons.F_LAST, new Date(last));
                if (null == bulkOperations) {
                    bulkOperations = mongoOperations.bulkOps(BulkOperations.BulkMode.UNORDERED, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
                }
                bulkOperations.upsert(upsertQuery, update);
            }
        }

        if (null != bulkOperations) {
            bulkOperations.execute();
        }
        if (!attributedHourRollupWrites.isEmpty()) {
            bulkWriteAttributedTaskMetrics(attributedHourRollupWrites);
        }
    }

    private boolean isAttributedTaskMetric(Map<String, String> tags) {
        return tags != null && SAMPLE_TYPE_TASK.equals(tags.get(FIELD_TAGS_TYPE))
                && StringUtils.isNotBlank(tags.get(FIELD_TAGS_PHYSICAL_NODE_ID));
    }

    static String attributedTaskHourDocumentId(Map<String, String> tags, Date hourDate) {
        return deterministicMeasurementDocumentId("tapdata-task-metric-hour-v2:", tags, hourDate);
    }

    private static String deterministicMeasurementDocumentId(String prefix, Map<String, String> tags, Date date) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(JsonUtil.toJson(new TreeMap<>(tags)).getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(Long.toString(date.getTime()).getBytes(StandardCharsets.UTF_8));
            return prefix + HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    static List<Bson> attributedTaskHourRollupPipeline(Map<String, String> tags, Date hourDate,
                                                        List<Map<String, Object>> samples,
                                                        List<Map<String, Object>> digests,
                                                        Collection<MeasurementEntity.DashboardMinuteSummary> summaries,
                                                        Long first, Long last) {
        List<Document> summaryDocuments = summaries.stream()
                .filter(Objects::nonNull)
                .map(summary -> new Document("date", summary.getDate())
                        .append("outputQpsSum", summary.getOutputQpsSum())
                        .append("outputQpsCount", summary.getOutputQpsCount())
                        .append("outputSizeQpsSum", summary.getOutputSizeQpsSum())
                        .append("outputSizeQpsCount", summary.getOutputSizeQpsCount()))
                .collect(Collectors.toList());
        Document set = new Document(MetricCons.F_GRANULARITY,
                new Document("$ifNull", Arrays.asList("$" + MetricCons.F_GRANULARITY, Granularity.GRANULARITY_HOUR)))
                .append(MetricCons.F_DATE, new Document("$ifNull", Arrays.asList("$" + MetricCons.F_DATE, hourDate)))
                .append(MetricCons.F_TAGS, new Document("$ifNull", Arrays.asList("$" + MetricCons.F_TAGS, new Document("$literal", tags))))
                .append(MetricCons.F_SAMPLES, replaceArrayEntriesByDate(MetricCons.F_SAMPLES, samples, Sample.FIELD_DATE))
                .append(MetricCons.F_DIGEST, replaceArrayEntriesByDate(MetricCons.F_DIGEST, digests, TDigestEntity.FIELD_DATE))
                .append("dashboardMinuteSummaries", replaceArrayEntriesByDate("dashboardMinuteSummaries", summaryDocuments, "date"))
                .append(MetricCons.F_FIRST, new Document("$min", Arrays.asList(
                        new Document("$ifNull", Arrays.asList("$" + MetricCons.F_FIRST, new Date(first))), new Date(first))))
                .append(MetricCons.F_LAST, new Document("$max", Arrays.asList(
                        new Document("$ifNull", Arrays.asList("$" + MetricCons.F_LAST, new Date(last))), new Date(last))));
        return List.of(new Document("$set", set));
    }

    private static Document replaceArrayEntriesByDate(String field, List<?> incoming, String dateField) {
        Document existing = new Document("$ifNull", Arrays.asList("$" + field, Collections.emptyList()));
        Document newer = new Document("$filter", new Document("input", "$$value")
                .append("as", "existingEntry")
                .append("cond", new Document("$gt", Arrays.asList("$$existingEntry." + dateField, "$$this." + dateField))));
        Document older = new Document("$filter", new Document("input", "$$value")
                .append("as", "existingEntry")
                .append("cond", new Document("$lt", Arrays.asList("$$existingEntry." + dateField, "$$this." + dateField))));
        Document merged = new Document("$concatArrays", Arrays.asList(newer,
                Collections.singletonList("$$this"), older));
        Document bounded = new Document("$slice", Arrays.asList(merged, 200));
        return new Document("$reduce", new Document("input", new Document("$literal", incoming))
                .append("initialValue", existing)
                .append("in", bounded));
    }

    private MeasurementEntity.DashboardMinuteSummary dashboardMinuteSummary(MeasurementEntity entity, Date date) {
        MeasurementEntity.DashboardMinuteSummary summary = new MeasurementEntity.DashboardMinuteSummary();
        summary.setDate(date);
        if (entity.getDashboardOutputQpsCount() != null) {
            summary.setOutputQpsSum(Optional.ofNullable(entity.getDashboardOutputQpsSum()).orElse(0D));
            summary.setOutputQpsCount(entity.getDashboardOutputQpsCount());
        } else {
            MetricSummary qps = summarizeMinuteMetric(entity.getSamples(), MetricCons.SS.VS.F_OUTPUT_QPS);
            summary.setOutputQpsSum(qps.sum);
            summary.setOutputQpsCount(qps.count);
        }
        if (entity.getDashboardOutputSizeQpsCount() != null) {
            summary.setOutputSizeQpsSum(Optional.ofNullable(entity.getDashboardOutputSizeQpsSum()).orElse(0D));
            summary.setOutputSizeQpsCount(entity.getDashboardOutputSizeQpsCount());
        } else {
            MetricSummary sizeQps = summarizeMinuteMetric(entity.getSamples(), MetricCons.SS.VS.F_OUTPUT_SIZE_QPS);
            summary.setOutputSizeQpsSum(sizeQps.sum);
            summary.setOutputSizeQpsCount(sizeQps.count);
        }
        return summary;
    }

    private MetricSummary summarizeMinuteMetric(List<Sample> samples, String metricName) {
        double sum = 0D;
        long count = 0L;
        if (CollectionUtils.isNotEmpty(samples)) {
            for (Sample sample : samples) {
                Number value = sample == null || sample.getVs() == null ? null : sample.getVs().get(metricName);
                if (value != null) {
                    sum += value.doubleValue();
                    count++;
                }
            }
        }
        return new MetricSummary(sum, count);
    }

    private void mergeDashboardMinuteSummary(Map<Long, MeasurementEntity.DashboardMinuteSummary> summaries,
                                             MeasurementEntity.DashboardMinuteSummary incoming) {
        long timestamp = incoming.getDate().getTime();
        MeasurementEntity.DashboardMinuteSummary current = summaries.get(timestamp);
        if (current == null) {
            summaries.put(timestamp, incoming);
            return;
        }
        current.setOutputQpsSum(Optional.ofNullable(current.getOutputQpsSum()).orElse(0D)
                + Optional.ofNullable(incoming.getOutputQpsSum()).orElse(0D));
        current.setOutputQpsCount(Optional.ofNullable(current.getOutputQpsCount()).orElse(0L)
                + Optional.ofNullable(incoming.getOutputQpsCount()).orElse(0L));
        current.setOutputSizeQpsSum(Optional.ofNullable(current.getOutputSizeQpsSum()).orElse(0D)
                + Optional.ofNullable(incoming.getOutputSizeQpsSum()).orElse(0D));
        current.setOutputSizeQpsCount(Optional.ofNullable(current.getOutputSizeQpsCount()).orElse(0L)
                + Optional.ofNullable(incoming.getOutputSizeQpsCount()).orElse(0L));
    }

    @Override
    public void deleteTaskMeasurement(String taskId) {
        if (StringUtils.isEmpty(taskId)) {
            return;
        }
        Query query = Query.query(Criteria.where(PATH_TAGS_TASK_ID).is(taskId));
        DeleteResult result = mongoOperations.remove(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);

        log.info(" taskId :{}  删除了 {} 条记录", taskId, JsonUtil.toJson(result));
    }

    @Override
    public Long[] countEventByTaskRecord(String taskId, String taskRecordId) {
        Criteria criteria = createMinuteCriteria(taskId, taskRecordId, SAMPLE_TYPE_TASK)
            .and(MetricCons.F_DATE).lte(new Date());
        Query query = new Query(criteria);
        query.with(Sort.by(MetricCons.F_DATE).descending());
        MeasurementEntity measurementEntity = mongoOperations.findOne(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
        if (null == measurementEntity || null == measurementEntity.getSamples() || measurementEntity.getSamples().isEmpty()) {
            return null;
        }

        Map<String, Number> vs = measurementEntity.getSamples().get(0).getVs();
        // inputInsertTotal + inputUpdateTotal + inputDeleteTotal + inputDdlTotal + inputOthersTotal
        AtomicReference<Long> inputTotal = new AtomicReference<>(0L);
        AtomicReference<Long> outputTotal = new AtomicReference<>(0L);
        List<String> calList = Lists.of(
            MetricCons.SS.VS.F_INPUT_DDL_TOTAL,
                MetricCons.SS.VS.F_INPUT_DELETE_TOTAL,
                MetricCons.SS.VS.F_INPUT_INSERT_TOTAL,
                MetricCons.SS.VS.F_INPUT_OTHERS_TOTAL,
                MetricCons.SS.VS.F_INPUT_UPDATE_TOTAL,
                MetricCons.SS.VS.F_OUTPUT_DDL_TOTAL,
                MetricCons.SS.VS.F_OUTPUT_DELETE_TOTAL,
                MetricCons.SS.VS.F_OUTPUT_INSERT_TOTAL,
                MetricCons.SS.VS.F_OUTPUT_OTHERS_TOTAL,
                MetricCons.SS.VS.F_OUTPUT_UPDATE_TOTAL
        );
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

        Criteria criteria = createMinuteCriteria(taskId, taskRecordId, SAMPLE_TYPE_TABLE);
        Query query = new Query(criteria);
        query.fields().include(PATH_TAGS_TABLE);
        List<MeasurementEntity> measurementEntities = mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
        if (CollectionUtils.isNotEmpty(measurementEntities)) {
            for (MeasurementEntity measurementEntity : measurementEntities) {
                Map<String, String> tags = measurementEntity.getTags();
                if (tags != null && tags.get(SAMPLE_TYPE_TABLE) != null) {
                    runTables.add(tags.get(SAMPLE_TYPE_TABLE));
                }
            }
        }

        return runTables;
    }

    @Override
    public Page<TableSyncStaticVo> querySyncStatic(TableSyncStaticDto dto, UserDetail userDetail) {
        String taskRecordId = dto.getTaskRecordId();

        Query taskQuery = new Query(Criteria.where(FIELD_TAGS_TASK_RECORD_ID).is(taskRecordId));
        TaskDto taskDto = taskService.findOne(taskQuery, userDetail);
        if (taskDto == null) {
            return new Page<>(0, Lists.of());
        }

        String taskId = taskDto.getId().toString();
        boolean hasTableRenameNode = false;
        if (CollectionUtils.isNotEmpty(taskDto.getDag().getNodes())) {
            hasTableRenameNode = taskDto.getDag().getNodes().stream().anyMatch(n -> n instanceof TableRenameProcessNode);
        }

        Criteria criteria = createMinuteCriteria(taskId, taskRecordId, SAMPLE_TYPE_TABLE);

        Boolean skipErrorTable = dto.getSkipErrorTable();
        if (null != skipErrorTable) {
            String field = MetricCons.SS.VS.path(MetricCons.SS.VS.F_SNAPSHOT_ERROR_SKIPPED);
            if (skipErrorTable) {
                criteria = criteria.and(field).is(1);
            } else {
                criteria = criteria.orOperator(
                    Criteria.where(field).exists(false),
                    Criteria.where(field).is(0)
                );
            }
        }

        querySyncByTableName(dto,criteria);

        TmPageable tmPageable = new TmPageable();
        tmPageable.setPage(dto.getPage());
        tmPageable.setSize(dto.getSize());

        Query query = new Query(criteria);
        long count = mongoOperations.count(query, MeasurementEntity.COLLECTION_NAME);
        if (count == 0) {
            return new Page<>(0, Collections.emptyList());
        }

        query.with(Sort.by(Sort.Direction.DESC, PATH_SS_VS_SNAPSHOT_SYNC_RATE));
        query.with(tmPageable);
        List<MeasurementEntity> measurementEntities = mongoOperations.find(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);

        // get table map from task dag
        AtomicReference<Map<String, String>> tableNameMap = new AtomicReference<>();
        tableNameMap.set(new HashMap<>());
        if (hasTableRenameNode) {
            DatabaseNode targetNode = taskDto.getDag().getTargetNode().getLast();
            List<MetadataInstancesDto> metas = metadataInstancesService.findBySourceIdAndTableNameList(targetNode.getConnectionId(),
                    null, userDetail, taskId);
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
            String originTable = measurementEntity.getTags().get(SAMPLE_TYPE_TABLE);
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
            Optional.ofNullable(vs.getOrDefault(MetricCons.SS.VS.F_SNAPSHOT_INSERT_ROW_TOTAL, 0)).ifPresent(number -> snapshotInsertRowTotal.set(number.longValue()));
            AtomicLong snapshotRowTotal = new AtomicLong(0L);
            Optional.ofNullable(vs.getOrDefault(MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL, 0)).ifPresent(number -> snapshotRowTotal.set(number.longValue()));

            Number snapshotSyncRate = vs.get(MetricCons.SS.VS.F_SNAPSHOT_SYNC_RATE);
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
            if (Optional.ofNullable(vs.get(MetricCons.SS.VS.F_SNAPSHOT_ERROR_SKIPPED))
                .map(Number::intValue)
                .map(i -> 1 == i)
                .orElse(false)) {
                fullSyncStatus = TableSyncStaticVo.FullSyncStatus.ERROR_SKIPPED;
            } else if (syncRate.compareTo(BigDecimal.ONE) >= 0) {
                fullSyncStatus = TableSyncStaticVo.FullSyncStatus.DONE;
                syncRate = BigDecimal.ONE;
            } else if (syncRate.compareTo(BigDecimal.ZERO) == 0) {
                fullSyncStatus = TableSyncStaticVo.FullSyncStatus.NOT_START;
            } else if (syncRate.intValue() == -1) {
                fullSyncStatus = TableSyncStaticVo.FullSyncStatus.COUNTING;
            } else {
                fullSyncStatus = TableSyncStaticVo.FullSyncStatus.ING;
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
//                .sorted(Comparator.comparing(TableSyncStaticVo::getSyncRate).reversed())
                .collect(Collectors.toList()));
    }

    protected void fixSyncRate(List<TableSyncStaticVo> tableSyncStaticVos, TaskDto taskDto) {
        String status = taskDto.getStatus();
        String type = taskDto.getType();
        // task is initial_sync and status is complete, than fix sync rate, Do this for now, and then adjust accordingly
        if (KeyWords.INITIAL_SYNC.equalsIgnoreCase(type) && KeyWords.COMPLETE.equalsIgnoreCase(status)) {
            tableSyncStaticVos.forEach(t -> {
                if (!TableSyncStaticVo.FullSyncStatus.ERROR_SKIPPED.equals(t.getFullSyncStatus())) {
                    t.setFullSyncStatus(TableSyncStaticVo.FullSyncStatus.DONE);
                    t.setSyncRate(BigDecimal.ONE);
                }
            });
        }
    }

    public void querySyncByTableName(TableSyncStaticDto dto, Criteria criteria){
        if(StringUtils.isNotBlank(dto.getTableName())){
            criteria.and(PATH_TAGS_TABLE).regex(dto.getTableName());
        }
    }

	@Override
    public List<SyncStatusStatisticsVo> queryTableSyncStatusStatistics(SyncStatusStatisticsParam param) {
        Criteria criteria = createMinuteCriteria(param.getTaskId(), param.getTaskRecordId(), SAMPLE_TYPE_TABLE);
		AggregationResults<SyncStatusStatisticsVo> aggregationResults = mongoOperations.aggregate(Aggregation.newAggregation(
			Aggregation.match(criteria),
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
        Criteria criteria = Criteria.where(PATH_TAGS_TASK_ID).is(taskId)
                .and(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)
                .and(PATH_TAGS_TYPE).is(SAMPLE_TYPE_TASK);

        Query query = new Query(criteria);
        query.fields().include(MetricCons.F_SAMPLES, MetricCons.F_TAGS);
        query.with(Sort.by(MetricCons.F_DATE).descending());
        return mongoOperations.findOne(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
    }

    @Override
    public Map<String, Sample> findLastMinuteSamplesByTaskIds(List<String> taskIds) {
        Map<String, Sample> result = new HashMap<>();
        List<String> normalizedTaskIds = normalizeTaskIds(taskIds);
        if (normalizedTaskIds.isEmpty()) {
            return result;
        }

        Date queryEnd = new Date();
        Date queryStart = new Date(queryEnd.getTime() - LATEST_SAMPLE_LOOKBACK_MILLIS);
        forEachTaskIdBatch(normalizedTaskIds, taskIdBatch -> {
            Criteria criteria = Criteria.where(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)
                    .and(PATH_TAGS_TYPE).is(SAMPLE_TYPE_TASK)
                    .and(PATH_TAGS_TASK_ID).in(taskIdBatch)
                    .and(MetricCons.F_DATE).gte(queryStart).lte(queryEnd);
            Aggregation aggregation = Aggregation.newAggregation(
                    Aggregation.match(criteria),
                    Aggregation.sort(Sort.by(Sort.Order.asc(PATH_TAGS_TASK_ID), Sort.Order.desc(MetricCons.F_DATE))),
                    Aggregation.group(PATH_TAGS_TASK_ID).first("$$ROOT").as("latestRecord"),
                    Aggregation.replaceRoot().withValueOf("$latestRecord"),
                    Aggregation.project(MetricCons.F_TAGS, MetricCons.F_SAMPLES))
                    .withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
            mongoOperations.aggregate(aggregation, MeasurementEntity.COLLECTION_NAME, MeasurementEntity.class)
                    .getMappedResults()
                    .forEach(entity -> extractLatestSample(entity)
                            .ifPresent(sample -> result.put(entity.getTags().get(FIELD_TAGS_TASK_ID), sample)));
        });
        return result;
    }

    private Optional<Sample> extractLatestSample(MeasurementEntity entity) {
        if (entity == null || entity.getTags() == null || StringUtils.isBlank(entity.getTags().get(FIELD_TAGS_TASK_ID))
                || CollectionUtils.isEmpty(entity.getSamples())) {
            return Optional.empty();
        }
        return entity.getSamples().stream()
                .filter(Objects::nonNull)
                .filter(sample -> sample.getDate() != null)
                .max(Comparator.comparing(Sample::getDate));
    }

    @Override
    public TaskMetricsTrendVo aggregateTaskMetricsByTaskIds(List<String> taskIds, long startAt, long endAt) {
        TaskMetricsTrendVo result = new TaskMetricsTrendVo();
        List<String> normalizedTaskIds = normalizeTaskIds(taskIds);
        if (normalizedTaskIds.isEmpty() || startAt >= endAt) {
            return result;
        }

        TaskMetricsRangeProfile rangeProfile = resolveTaskMetricsRangeProfile(startAt, endAt);
        long interval = rangeProfile.interval;
        long bucketStart = startAt - startAt % interval;
        long bucketEnd = endAt - endAt % interval;
        long queryStart = bucketStart;

        // taskId -> bucket -> list of sample values (outputQps, outputSizeQps)
        Map<String, Map<Long, List<double[]>>> taskBucketSamples = new HashMap<>();
        forEachTaskIdBatch(normalizedTaskIds, taskIdBatch -> collectTaskMetricsBatch(taskIdBatch, rangeProfile, queryStart,
                startAt, endAt, interval, taskBucketSamples));

        // 每个 taskId 在每个 bucket 内取平均值，再跨 taskId 求和
        Map<Long, Double> outputQpsMap = new HashMap<>();
        Map<Long, Double> outputSizeQpsMap = new HashMap<>();
        for (Map.Entry<String, Map<Long, List<double[]>>> taskEntry : taskBucketSamples.entrySet()) {
            for (Map.Entry<Long, List<double[]>> bucketEntry : taskEntry.getValue().entrySet()) {
                long bucket = bucketEntry.getKey();
                List<double[]> sampleValues = bucketEntry.getValue();
                double avgOutputQps = sampleValues.stream().mapToDouble(v -> v[0]).average().orElse(0D);
                double avgOutputSizeQps = sampleValues.stream().mapToDouble(v -> v[1]).average().orElse(0D);
                outputQpsMap.merge(bucket, avgOutputQps, Double::sum);
                outputSizeQpsMap.merge(bucket, avgOutputSizeQps, Double::sum);
            }
        }

        Double lastOutputQps = null;
        Double lastOutputSizeQps = null;
        for (long cursor = bucketStart; cursor <= bucketEnd; cursor += interval) {
            result.getTs().add(cursor / 1000L);
            Double outputQps = outputQpsMap.get(cursor);
            if (outputQps == null) {
                outputQps = lastOutputQps;
            } else {
                lastOutputQps = outputQps;
            }
            Double outputSizeQps = outputSizeQpsMap.get(cursor);
            if (outputSizeQps == null) {
                outputSizeQps = lastOutputSizeQps;
            } else {
                lastOutputSizeQps = outputSizeQps;
            }
            result.getOutputQps().add(Optional.ofNullable(outputQps).orElse(0D));
            result.getOutputSizeQps().add(Optional.ofNullable(outputSizeQps).orElse(0D));
        }
        return result;
    }

    @Override
    public TaskMetricsTrendVo aggregateTaskMetricsByTaskIdsV2(List<String> taskIds, long startAt, long endAt, String physicalNodeId) {
        TaskMetricsTrendVo result = new TaskMetricsTrendVo();
        List<String> normalizedTaskIds = normalizeTaskIds(taskIds);
        if (normalizedTaskIds.isEmpty() || startAt >= endAt) {
            result.setStatus("no_data");
            result.setTraceability("complete");
            result.setCoverage("unknown");
            return result;
        }

        long rangeMillis = endAt - startAt;
        long intervalMillis = rangeMillis <= 5L * 60L * 1000L ? 5L * 1000L
                : rangeMillis <= 60L * 60L * 1000L ? 60L * 1000L : 60L * 60L * 1000L;
        long intervalSeconds = intervalMillis / 1000L;
        result.setIntervalSeconds(intervalSeconds);

        Map<String, Map<Long, TaskMetricBucket>> metrics = new HashMap<>();
        long hourMillis = Granularity.getGranularityMillisInterval(Granularity.GRANULARITY_HOUR);
        long firstCompleteHour = ceilToInterval(startAt, hourMillis);
        long lastCompleteHour = endAt - endAt % hourMillis;
        if (rangeMillis <= hourMillis || firstCompleteHour >= lastCompleteHour) {
            collectTaskMetricsV2(normalizedTaskIds, Granularity.GRANULARITY_MINUTE, startAt, endAt,
                    intervalMillis, physicalNodeId, metrics);
        } else {
            collectTaskMetricsV2(normalizedTaskIds, Granularity.GRANULARITY_MINUTE, startAt, firstCompleteHour,
                    intervalMillis, physicalNodeId, metrics);
            collectTaskMetricsV2(normalizedTaskIds, Granularity.GRANULARITY_HOUR, firstCompleteHour, lastCompleteHour,
                    intervalMillis, physicalNodeId, metrics);
            collectTaskMetricsV2(normalizedTaskIds, Granularity.GRANULARITY_MINUTE, lastCompleteHour, endAt,
                    intervalMillis, physicalNodeId, metrics);
        }

        long firstBucket = startAt - startAt % intervalMillis;
        int timelineSize = 0;
        int bucketsWithValues = 0;
        boolean hasUnattributedData = false;
        boolean hasAttributableData = false;
        boolean hasSelectedNodeData = false;
        Map<Long, Double> outputQpsByBucket = new HashMap<>();
        Map<Long, Double> outputSizeQpsByBucket = new HashMap<>();
        Map<Long, Set<Long>> coveredMinutesByBucket = new HashMap<>();
        Set<Long> incompleteSelectedBuckets = new HashSet<>();
        Set<Long> partiallyCoveredSelectedBuckets = new HashSet<>();
        for (Map<Long, TaskMetricBucket> taskBuckets : metrics.values()) {
            for (Map.Entry<Long, TaskMetricBucket> bucketEntry : taskBuckets.entrySet()) {
                TaskMetricBucket bucket = bucketEntry.getValue();
                if (intervalMillis == hourMillis && bucket.preAggregatedHour) {
                    hasUnattributedData |= bucket.hasUnattributedData;
                    hasAttributableData |= bucket.hasAttributableData;
                    hasSelectedNodeData |= bucket.hasSelectedNodeData;
                    if (bucket.partiallyCovered) {
                        partiallyCoveredSelectedBuckets.add(bucketEntry.getKey());
                    }
                    if (bucket.incompleteSelected) {
                        incompleteSelectedBuckets.add(bucketEntry.getKey());
                    }
                    coveredMinutesByBucket.computeIfAbsent(bucketEntry.getKey(), ignored -> new HashSet<>())
                            .addAll(bucket.coveredMinutes);
                    if (bucket.sampleCount > 0) {
                        outputQpsByBucket.merge(bucketEntry.getKey(), bucket.outputQpsSum / bucket.sampleCount, Double::sum);
                    }
                    if (bucket.outputSizeSampleCount > 0) {
                        outputSizeQpsByBucket.merge(bucketEntry.getKey(), bucket.outputSizeQpsSum / bucket.outputSizeSampleCount, Double::sum);
                    }
                    continue;
                }
                if (intervalMillis == hourMillis && !bucket.hourlyMinutes.isEmpty()) {
                    HourlyTaskBucketSummary summary = summarizeHourlyTaskBucket(bucket, physicalNodeId);
                    hasUnattributedData |= summary.hasUnattributedData;
                    hasAttributableData |= summary.hasAttributableData;
                    hasSelectedNodeData |= summary.hasSelectedNodeData;
                    if (summary.partiallyCovered) {
                        partiallyCoveredSelectedBuckets.add(bucketEntry.getKey());
                    }
                    if (summary.incompleteSelected) {
                        incompleteSelectedBuckets.add(bucketEntry.getKey());
                    }
                    coveredMinutesByBucket.computeIfAbsent(bucketEntry.getKey(), ignored -> new HashSet<>())
                            .addAll(summary.coveredMinutes);
                    if (summary.outputQps != null) {
                        outputQpsByBucket.merge(bucketEntry.getKey(), summary.outputQps, Double::sum);
                    }
                    if (summary.outputSizeQps != null) {
                        outputSizeQpsByBucket.merge(bucketEntry.getKey(), summary.outputSizeQps, Double::sum);
                    }
                    continue;
                }
                if (bucket.sampleCount == 0 && bucket.outputSizeSampleCount == 0) {
                    continue;
                }
                hasUnattributedData |= bucket.unattributedSampleCount > 0;
                hasAttributableData |= bucket.byPhysicalNode.values().stream().anyMatch(node -> node.sampleCount > 0);
                MetricAccumulator selected = StringUtils.isBlank(physicalNodeId) ? null : bucket.byPhysicalNode.get(physicalNodeId);
                if (selected != null && selected.sampleCount > 0) {
                    hasSelectedNodeData = true;
                }
                boolean selectedNode = StringUtils.isNotBlank(physicalNodeId);
                if (selectedNode && bucket.sampleCount > 0) {
                    if (bucket.unattributedSampleCount > 0) {
                        incompleteSelectedBuckets.add(bucketEntry.getKey());
                        continue;
                    }
                    if (selected == null || selected.sampleCount == 0) {
                        partiallyCoveredSelectedBuckets.add(bucketEntry.getKey());
                    }
                }
                if (bucket.sampleCount > 0) {
                    Double qps = selectedNode ? nodeBucketContribution(bucket, physicalNodeId)
                            : bucket.outputQpsSum / bucket.sampleCount;
                    if (qps != null) {
                        outputQpsByBucket.merge(bucketEntry.getKey(), qps, Double::sum);
                    }
                }
                if (bucket.outputSizeSampleCount > 0) {
                    if (selectedNode && bucket.unattributedOutputSizeSampleCount > 0) {
                        incompleteSelectedBuckets.add(bucketEntry.getKey());
                    } else if (selectedNode && (selected == null || selected.outputSizeSampleCount == 0)) {
                        partiallyCoveredSelectedBuckets.add(bucketEntry.getKey());
                    } else {
                        double sizeQps = selectedNode
                                ? selected.outputSizeQpsSum / bucket.outputSizeSampleCount
                                : bucket.outputSizeQpsSum / bucket.outputSizeSampleCount;
                        outputSizeQpsByBucket.merge(bucketEntry.getKey(), sizeQps, Double::sum);
                    }
                }
            }
        }

        for (long cursor = firstBucket; cursor < endAt; cursor += intervalMillis) {
            timelineSize++;
            result.getTs().add(cursor / 1000L);
            Double outputQps = incompleteSelectedBuckets.contains(cursor) ? null : outputQpsByBucket.get(cursor);
            Double outputSizeQps = incompleteSelectedBuckets.contains(cursor) ? null : outputSizeQpsByBucket.get(cursor);
            result.getOutputQps().add(outputQps);
            result.getOutputSizeQps().add(outputSizeQps);
            if (outputQps != null) {
                bucketsWithValues++;
            }
        }

        result.setHasUnattributedData(hasUnattributedData);
        result.setHasSelectedNodeData(hasSelectedNodeData);
        result.setStatus(bucketsWithValues > 0 ? "ok" : "no_data");
        boolean hasRelevantAttributableData = StringUtils.isBlank(physicalNodeId) ? hasAttributableData : hasSelectedNodeData;
        result.setTraceability(!hasUnattributedData ? "complete" : hasRelevantAttributableData ? "partial" : "untraceable");
        boolean completeMinuteCoverage = true;
        if (intervalMillis == hourMillis) {
            Map<Long, Integer> expectedMinuteCounts = expectedMinuteCounts(startAt, endAt, intervalMillis);
            completeMinuteCoverage = expectedMinuteCounts.entrySet().stream().allMatch(entry ->
                    coveredMinutesByBucket.getOrDefault(entry.getKey(), Collections.emptySet()).size() >= entry.getValue());
        }
        result.setCoverage(bucketsWithValues == 0 ? "unknown"
                : bucketsWithValues == timelineSize && completeMinuteCoverage
                && incompleteSelectedBuckets.isEmpty() && partiallyCoveredSelectedBuckets.isEmpty()
                ? "complete" : "partial");
        return result;
    }

    private Map<Long, Integer> expectedMinuteCounts(long startAt, long endAt, long intervalMillis) {
        Map<Long, Integer> expected = new HashMap<>();
        long minuteMillis = Granularity.getGranularityMillisInterval(Granularity.GRANULARITY_MINUTE);
        for (long minuteStart = startAt - startAt % minuteMillis; minuteStart < endAt; minuteStart += minuteMillis) {
            if (minuteStart + minuteMillis <= startAt) {
                continue;
            }
            long bucketStart = minuteStart - minuteStart % intervalMillis;
            expected.merge(bucketStart, 1, Integer::sum);
        }
        return expected;
    }

    private void collectTaskMetricsV2(List<String> taskIds, String granularity, long startAt, long endAt,
                                      long intervalMillis, String physicalNodeId,
                                      Map<String, Map<Long, TaskMetricBucket>> metrics) {
        if (startAt >= endAt) {
            return;
        }
        forEachTaskIdBatch(taskIds, taskIdBatch -> {
            if (Granularity.GRANULARITY_HOUR.equals(granularity) && intervalMillis == 3_600_000L) {
                for (long windowStart = startAt; windowStart < endAt; windowStart += 3_600_000L) {
                    long windowEnd = Math.min(endAt, windowStart + 3_600_000L);
                    Map<String, Map<Long, TaskMetricBucket>> windowMetrics = new HashMap<>();
                    collectTaskMetricsV2Batch(taskIdBatch, granularity, windowStart, windowEnd,
                            intervalMillis, windowMetrics);
                    mergeHourlyWindowMetrics(windowMetrics, metrics, physicalNodeId);
                }
            } else {
                collectTaskMetricsV2Batch(taskIdBatch, granularity, startAt, endAt,
                        intervalMillis, metrics);
            }
        });
    }

    private void collectTaskMetricsV2Batch(List<String> taskIdBatch, String granularity, long startAt, long endAt,
                                           long intervalMillis,
                                           Map<String, Map<Long, TaskMetricBucket>> metrics) {
        long sourceGranularityMillis = Granularity.getGranularityMillisInterval(granularity);
        long queryStart = startAt - sourceGranularityMillis;
        long queryEnd = endAt + sourceGranularityMillis;
        Map<String, DirtyTaskMinuteSummary> dirtyMinutes = Granularity.GRANULARITY_HOUR.equals(granularity)
                ? collectDirtyTaskMinuteSummaries(taskIdBatch, startAt, endAt)
                : Collections.emptyMap();
            Criteria criteria = Criteria.where(MetricCons.F_GRANULARITY).is(granularity)
                    .and(PATH_TAGS_TYPE).is(SAMPLE_TYPE_TASK)
                    .and(PATH_TAGS_TASK_ID).in(taskIdBatch)
                    .and(MetricCons.F_DATE).gte(new Date(queryStart)).lt(new Date(queryEnd));
            Query query = new Query(criteria);
            query.fields().include(MetricCons.F_TAGS, MetricCons.F_SAMPLES);
            if (Granularity.GRANULARITY_HOUR.equals(granularity)) {
                query.fields().include("dashboardMinuteSummaries");
            }
            query.with(Sort.by(MetricCons.F_DATE).ascending());
            Map<String, HourlySourceSnapshot> processedHourlyMinutes = new HashMap<>();
            try (Stream<MeasurementEntity> stream = mongoOperations.stream(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME)) {
                stream.forEach(entity -> {
                    if (entity == null || entity.getTags() == null || CollectionUtils.isEmpty(entity.getSamples())) {
                        return;
                    }
                    String taskId = entity.getTags().get(FIELD_TAGS_TASK_ID);
                    if (StringUtils.isBlank(taskId)) {
                        return;
                    }
                    String physicalId = entity.getTags().get(FIELD_TAGS_PHYSICAL_NODE_ID);
                    DashboardMinuteSummaryIndex summaryIndex =
                            Granularity.GRANULARITY_HOUR.equals(granularity)
                                    ? dashboardMinuteSummariesByTime(entity.getDashboardMinuteSummaries())
                                    : DashboardMinuteSummaryIndex.empty();
                    for (Sample sample : entity.getSamples()) {
                        if (sample == null || sample.getDate() == null) {
                            continue;
                        }
                        if (Granularity.GRANULARITY_HOUR.equals(granularity)) {
                            long sampleTime = sample.getDate().getTime();
                            if (sampleTime < startAt || sampleTime >= endAt) {
                                continue;
                            }
                            String minuteKey = taskMetricMinuteKey(entity.getTags(), sampleTime);
                            if (dirtyMinutes.containsKey(minuteKey)) {
                                continue;
                            }
                            if (summaryIndex.conflictingDates.contains(sampleTime)) {
                                markHourlyMinuteConflict(metrics, taskId, sampleTime, intervalMillis);
                                continue;
                            }
                            MeasurementEntity.DashboardMinuteSummary summary = summaryIndex.byTime.get(sampleTime);
                            HourlySourceSnapshot snapshot = hourlySourceSnapshot(sample, summary);
                            HourlySourceSnapshot previous = processedHourlyMinutes.putIfAbsent(minuteKey, snapshot);
                            if (previous != null) {
                                if (!previous.equals(snapshot)) {
                                    markHourlyMinuteConflict(metrics, taskId, sampleTime, intervalMillis);
                                }
                                continue;
                            }
                            collectTaskMetricV2AggregatedSample(sample, summary, taskId, physicalId,
                                    startAt, endAt, intervalMillis, metrics);
                        } else {
                            collectTaskMetricV2Sample(sample, taskId, physicalId, startAt, endAt, intervalMillis, metrics);
                        }
                    }
                });
            }
            for (DirtyTaskMinuteSummary dirtyMinute : dirtyMinutes.values()) {
                if (dirtyMinute.conflicting) {
                    markHourlyMinuteConflict(metrics, dirtyMinute.taskId, dirtyMinute.dateMillis, intervalMillis);
                    continue;
                }
                collectTaskMetricV2Contribution(dirtyMinute.taskId, dirtyMinute.physicalNodeId,
                        dirtyMinute.dateMillis, startAt, endAt, intervalMillis,
                        dirtyMinute.outputQpsSum, dirtyMinute.outputQpsCount,
                        dirtyMinute.outputSizeQpsSum, dirtyMinute.outputSizeQpsCount, metrics);
            }
    }

    private void mergeHourlyWindowMetrics(Map<String, Map<Long, TaskMetricBucket>> windowMetrics,
                                          Map<String, Map<Long, TaskMetricBucket>> metrics,
                                          String physicalNodeId) {
        windowMetrics.forEach((taskId, buckets) -> buckets.forEach((bucketStart, sourceBucket) -> {
            HourlyTaskBucketSummary summary = summarizeHourlyTaskBucket(sourceBucket, physicalNodeId);
            TaskMetricBucket target = metrics.computeIfAbsent(taskId, ignored -> new HashMap<>())
                    .computeIfAbsent(bucketStart, ignored -> new TaskMetricBucket());
            target.preAggregatedHour = true;
            target.hasUnattributedData |= summary.hasUnattributedData;
            target.hasAttributableData |= summary.hasAttributableData;
            target.hasSelectedNodeData |= summary.hasSelectedNodeData;
            target.partiallyCovered |= summary.partiallyCovered;
            target.incompleteSelected |= summary.incompleteSelected;
            target.coveredMinutes.addAll(summary.coveredMinutes);
            if (summary.outputQps != null) {
                target.outputQpsSum += summary.outputQps;
                target.sampleCount++;
            }
            if (summary.outputSizeQps != null) {
                target.outputSizeQpsSum += summary.outputSizeQps;
                target.outputSizeSampleCount++;
            }
        }));
    }

    private HourlyTaskBucketSummary summarizeHourlyTaskBucket(TaskMetricBucket bucket, String physicalNodeId) {
        HourlyTaskBucketSummary result = new HourlyTaskBucketSummary();
        boolean selectedNode = StringUtils.isNotBlank(physicalNodeId);
        boolean hasSelectedQpsEvidence = !selectedNode || bucket.hourlyMinutes.values().stream()
                .map(minute -> minute.byPhysicalNode.get(physicalNodeId))
                .filter(Objects::nonNull)
                .anyMatch(node -> node.outputQps.hasData());
        boolean hasSelectedSizeEvidence = !selectedNode || bucket.hourlyMinutes.values().stream()
                .map(minute -> minute.byPhysicalNode.get(physicalNodeId))
                .filter(Objects::nonNull)
                .anyMatch(node -> node.outputSizeQps.hasData());
        double qpsSum = 0D;
        int qpsMinuteCount = 0;
        double sizeSum = 0D;
        int sizeMinuteCount = 0;

        for (Map.Entry<Long, HourlyTaskMinuteBucket> minuteEntry : bucket.hourlyMinutes.entrySet()) {
            HourlyTaskMinuteBucket minute = minuteEntry.getValue();
            HourlyMetricValue qps = summarizeHourlyMetric(minute, true, physicalNodeId, hasSelectedQpsEvidence);
            HourlyMetricValue size = summarizeHourlyMetric(minute, false, physicalNodeId, hasSelectedSizeEvidence);
            result.hasUnattributedData |= qps.hasUnattributedData || size.hasUnattributedData;
            result.hasAttributableData |= qps.hasAttributableData || size.hasAttributableData;
            result.hasSelectedNodeData |= qps.hasSelectedNodeData || size.hasSelectedNodeData;
            result.partiallyCovered |= qps.partial || size.partial;
            result.incompleteSelected |= selectedNode && hasSelectedQpsEvidence && qps.partial && !qps.hasData;
            if (qps.hasData && qps.value != null) {
                qpsSum += qps.value;
                qpsMinuteCount++;
                result.coveredMinutes.add(minuteEntry.getKey());
            }
            if (size.hasData && size.value != null) {
                sizeSum += size.value;
                sizeMinuteCount++;
            }
        }
        if (hasSelectedQpsEvidence && qpsMinuteCount > 0) {
            result.outputQps = qpsSum / qpsMinuteCount;
        }
        if (hasSelectedSizeEvidence && sizeMinuteCount > 0) {
            result.outputSizeQps = sizeSum / sizeMinuteCount;
        }
        if (selectedNode && !hasSelectedQpsEvidence && (qpsMinuteCount > 0 || !bucket.hourlyMinutes.isEmpty())) {
            result.partiallyCovered = true;
        }
        return result;
    }

    private HourlyMetricValue summarizeHourlyMetric(HourlyTaskMinuteBucket minute, boolean outputQps,
                                                    String physicalNodeId, boolean hasSelectedEvidence) {
        if (outputQps ? minute.outputQpsConflict : minute.outputSizeQpsConflict) {
            return new HourlyMetricValue(null, false, hasUnattributed(minute, outputQps),
                    hasAttributable(minute, outputQps), false, true);
        }

        long weightedCount = 0L;
        double weightedSum = 0D;
        int unweightedSequences = 0;
        String unweightedNodeId = null;
        Double unweightedValue = null;
        boolean hasUnattributed = false;
        boolean hasAttributable = false;
        boolean hasSelectedData = false;
        boolean hasLegacyAndWeightedMix = false;

        for (Map.Entry<String, HourlyNodeMinuteMetric> entry : minute.byPhysicalNode.entrySet()) {
            HourlyMetricAccumulator accumulator = outputQps ? entry.getValue().outputQps : entry.getValue().outputSizeQps;
            if (accumulator.conflict) {
                return new HourlyMetricValue(null, false, hasUnattributed(minute, outputQps),
                        hasAttributable(minute, outputQps), false, true);
            }
            if (accumulator.weightedCount > 0 && accumulator.hasUnweightedValue) {
                hasLegacyAndWeightedMix = true;
            }
            weightedCount += accumulator.weightedCount;
            weightedSum += accumulator.weightedSum;
            if (accumulator.hasUnweightedValue) {
                unweightedSequences++;
                unweightedNodeId = entry.getKey();
                unweightedValue = accumulator.unweightedValue;
            }
            if (StringUtils.isBlank(entry.getKey())) {
                hasUnattributed |= accumulator.hasData();
            } else {
                hasAttributable |= accumulator.hasData();
            }
            if (StringUtils.isNotBlank(physicalNodeId) && physicalNodeId.equals(entry.getKey())
                    && accumulator.hasData()) {
                hasSelectedData = true;
            }
        }

        if (hasLegacyAndWeightedMix || unweightedSequences > 1
                || (unweightedSequences > 0 && weightedCount > 0)) {
            return new HourlyMetricValue(null, false, hasUnattributed, hasAttributable,
                    hasSelectedData, true);
        }
        if (unweightedSequences == 1) {
            if (!StringUtils.isNotBlank(physicalNodeId)) {
                return new HourlyMetricValue(unweightedValue, true, hasUnattributed,
                        hasAttributable, false, false);
            }
            if (StringUtils.isBlank(unweightedNodeId)) {
                if (Double.compare(unweightedValue, 0D) == 0) {
                    return new HourlyMetricValue(0D, true, true, hasAttributable,
                            false, true);
                }
                return new HourlyMetricValue(null, false, true, hasAttributable,
                        false, true);
            }
            if (!physicalNodeId.equals(unweightedNodeId)) {
                return new HourlyMetricValue(hasSelectedEvidence ? 0D : null,
                        hasSelectedEvidence, false, true, false, true);
            }
            return new HourlyMetricValue(unweightedValue, true, false, true,
                    true, false);
        }
        if (weightedCount <= 0) {
            return new HourlyMetricValue(null, false, hasUnattributed, hasAttributable,
                    hasSelectedData, false);
        }
        if (!StringUtils.isNotBlank(physicalNodeId)) {
            return new HourlyMetricValue(weightedSum / weightedCount, true,
                    hasUnattributed, hasAttributable, false, false);
        }
        if (hasUnattributed) {
            return new HourlyMetricValue(null, false, true, hasAttributable,
                    hasSelectedData, true);
        }
        HourlyNodeMinuteMetric selected = minute.byPhysicalNode.get(physicalNodeId);
        double selectedSum = selected == null ? 0D
                : (outputQps ? selected.outputQps.weightedSum : selected.outputSizeQps.weightedSum);
        return new HourlyMetricValue(hasSelectedEvidence ? selectedSum / weightedCount : null,
                hasSelectedEvidence, false, hasAttributable, hasSelectedData,
                !hasSelectedData);
    }

    private boolean hasUnattributed(HourlyTaskMinuteBucket minute, boolean outputQps) {
        HourlyNodeMinuteMetric node = minute.byPhysicalNode.get(null);
        return node != null && (outputQps ? node.outputQps : node.outputSizeQps).hasData();
    }

    private boolean hasAttributable(HourlyTaskMinuteBucket minute, boolean outputQps) {
        return minute.byPhysicalNode.entrySet().stream()
                .filter(entry -> StringUtils.isNotBlank(entry.getKey()))
                .map(entry -> outputQps ? entry.getValue().outputQps : entry.getValue().outputSizeQps)
                .anyMatch(HourlyMetricAccumulator::hasData);
    }

    private void markHourlyMinuteConflict(Map<String, Map<Long, TaskMetricBucket>> metrics,
                                          String taskId, long timestamp, long intervalMillis) {
        long bucketStart = timestamp - timestamp % intervalMillis;
        long minuteStart = timestamp - timestamp % 60_000L;
        TaskMetricBucket bucket = metrics.computeIfAbsent(taskId, ignored -> new HashMap<>())
                .computeIfAbsent(bucketStart, ignored -> new TaskMetricBucket());
        HourlyTaskMinuteBucket minute = bucket.hourlyMinutes.computeIfAbsent(minuteStart,
                ignored -> new HourlyTaskMinuteBucket());
        minute.outputQpsConflict = true;
        minute.outputSizeQpsConflict = true;
    }

    private HourlySourceSnapshot hourlySourceSnapshot(Sample sample,
                                                       MeasurementEntity.DashboardMinuteSummary summary) {
        if (summary != null) {
            return new HourlySourceSnapshot(summary.getOutputQpsSum(), summary.getOutputQpsCount(),
                    summary.getOutputSizeQpsSum(), summary.getOutputSizeQpsCount(), true);
        }
        Map<String, Number> values = sample.getVs();
        Number qps = values == null ? null : values.get(MetricCons.SS.VS.F_OUTPUT_QPS);
        Number size = values == null ? null : values.get(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS);
        return new HourlySourceSnapshot(qps == null ? null : qps.doubleValue(), qps == null ? 0L : 1L,
                size == null ? null : size.doubleValue(), size == null ? 0L : 1L, false);
    }

    private boolean sameDashboardMinuteSummary(MeasurementEntity.DashboardMinuteSummary left,
                                               MeasurementEntity.DashboardMinuteSummary right) {
        return Objects.equals(left.getOutputQpsSum(), right.getOutputQpsSum())
                && Objects.equals(left.getOutputQpsCount(), right.getOutputQpsCount())
                && Objects.equals(left.getOutputSizeQpsSum(), right.getOutputSizeQpsSum())
                && Objects.equals(left.getOutputSizeQpsCount(), right.getOutputSizeQpsCount());
    }

    private Map<String, DirtyTaskMinuteSummary> collectDirtyTaskMinuteSummaries(List<String> taskIdBatch,
                                                                                long startAt, long endAt) {
        Map<String, DirtyTaskMinuteSummary> dirtyMinutes = new HashMap<>();
        Criteria criteria = Criteria.where(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)
                .and(MeasurementEntity.FIELD_DASHBOARD_ROLLUP_DIRTY).is(true)
                .and(PATH_TAGS_TYPE).is(SAMPLE_TYPE_TASK)
                .and(PATH_TAGS_TASK_ID).in(taskIdBatch)
                .and(MetricCons.F_DATE).gte(new Date(startAt)).lt(new Date(endAt));
        Query query = new Query(criteria);
        query.fields().include(MetricCons.F_DATE, MetricCons.F_TAGS,
                MeasurementEntity.FIELD_DASHBOARD_OUTPUT_QPS_SUM,
                MeasurementEntity.FIELD_DASHBOARD_OUTPUT_QPS_COUNT,
                MeasurementEntity.FIELD_DASHBOARD_OUTPUT_SIZE_QPS_SUM,
                MeasurementEntity.FIELD_DASHBOARD_OUTPUT_SIZE_QPS_COUNT);
        query.with(Sort.by(MetricCons.F_DATE).ascending());
        try (Stream<MeasurementEntity> stream = mongoOperations.stream(query, MeasurementEntity.class,
                MeasurementEntity.COLLECTION_NAME)) {
            stream.forEach(entity -> {
                if (entity == null || entity.getTags() == null || entity.getDate() == null) {
                    return;
                }
                String taskId = entity.getTags().get(FIELD_TAGS_TASK_ID);
                if (StringUtils.isBlank(taskId)) {
                    return;
                }
                String key = taskMetricMinuteKey(entity.getTags(), entity.getDate().getTime());
                DirtyTaskMinuteSummary summary = dirtyMinutes.computeIfAbsent(key,
                        ignored -> new DirtyTaskMinuteSummary(taskId,
                                entity.getTags().get(FIELD_TAGS_PHYSICAL_NODE_ID), entity.getDate().getTime()));
                summary.add(entity);
            });
        }
        return dirtyMinutes;
    }

    private DashboardMinuteSummaryIndex dashboardMinuteSummariesByTime(
            List<MeasurementEntity.DashboardMinuteSummary> summaries) {
        if (CollectionUtils.isEmpty(summaries)) {
            return DashboardMinuteSummaryIndex.empty();
        }
        Map<Long, MeasurementEntity.DashboardMinuteSummary> byTime = new HashMap<>();
        Set<Long> conflictingDates = new HashSet<>();
        summaries.stream().filter(Objects::nonNull).filter(summary -> summary.getDate() != null).forEach(summary -> {
            long timestamp = summary.getDate().getTime();
            MeasurementEntity.DashboardMinuteSummary existing = byTime.putIfAbsent(timestamp, summary);
            if (existing != null && !sameDashboardMinuteSummary(existing, summary)) {
                conflictingDates.add(timestamp);
            }
        });
        return new DashboardMinuteSummaryIndex(byTime, conflictingDates);
    }

    private String taskMetricMinuteKey(Map<String, String> tags, long minuteDate) {
        return hashTag(tags) + "@" + minuteDate;
    }

    private void collectTaskMetricV2AggregatedSample(Sample sample,
                                                     MeasurementEntity.DashboardMinuteSummary summary,
                                                     String taskId, String physicalNodeId,
                                                     long startAt, long endAt, long intervalMillis,
                                                     Map<String, Map<Long, TaskMetricBucket>> metrics) {
        if (sample.getVs() == null) {
            return;
        }
        Map<String, Number> values = sample.getVs();
        Number outputQpsCount;
        Number outputQpsSum;
        Number outputSizeQpsCount;
        Number outputSizeQpsSum;
        boolean weighted = summary != null;
        if (summary != null) {
            outputQpsCount = Optional.ofNullable(summary.getOutputQpsCount()).orElse(0L);
            outputQpsSum = Optional.ofNullable(summary.getOutputQpsSum()).orElse(0D);
            outputSizeQpsCount = Optional.ofNullable(summary.getOutputSizeQpsCount()).orElse(0L);
            outputSizeQpsSum = Optional.ofNullable(summary.getOutputSizeQpsSum()).orElse(0D);
        } else {
            Number outputQps = values.get(MetricCons.SS.VS.F_OUTPUT_QPS);
            outputQpsCount = outputQps == null ? 0L : 1L;
            outputQpsSum = outputQps;
            Number outputSizeQps = values.get(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS);
            outputSizeQpsCount = outputSizeQps == null ? 0L : 1L;
            outputSizeQpsSum = outputSizeQps;
        }
        collectTaskMetricV2Contribution(taskId, physicalNodeId, sample.getDate().getTime(), startAt, endAt,
                intervalMillis, outputQpsSum, outputQpsCount.longValue(),
                outputSizeQpsSum, outputSizeQpsCount.longValue(), weighted, metrics);
    }

    private void collectTaskMetricV2Sample(Sample sample, String taskId, String physicalNodeId,
                                           long startAt, long endAt, long intervalMillis,
                                           Map<String, Map<Long, TaskMetricBucket>> metrics) {
        if (sample == null || sample.getDate() == null || sample.getVs() == null) {
            return;
        }
        long timestamp = sample.getDate().getTime();
        if (timestamp < startAt || timestamp >= endAt) {
            return;
        }
        Number outputQps = sample.getVs().get(MetricCons.SS.VS.F_OUTPUT_QPS);
        Number outputSizeQps = sample.getVs().get(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS);
        collectTaskMetricV2Contribution(taskId, physicalNodeId, timestamp, startAt, endAt, intervalMillis,
                outputQps, outputQps == null ? 0L : 1L,
                outputSizeQps, outputSizeQps == null ? 0L : 1L, metrics);
    }

    private void collectTaskMetricV2Contribution(String taskId, String physicalNodeId, long timestamp,
                                                 long startAt, long endAt, long intervalMillis,
                                                 Number outputQpsSum, long outputQpsCount,
                                                 Number outputSizeQpsSum, long outputSizeQpsCount,
                                                 Map<String, Map<Long, TaskMetricBucket>> metrics) {
        collectTaskMetricV2Contribution(taskId, physicalNodeId, timestamp, startAt, endAt, intervalMillis,
                outputQpsSum, outputQpsCount, outputSizeQpsSum, outputSizeQpsCount, true, metrics);
    }

    private void collectTaskMetricV2Contribution(String taskId, String physicalNodeId, long timestamp,
                                                 long startAt, long endAt, long intervalMillis,
                                                 Number outputQpsSum, long outputQpsCount,
                                                 Number outputSizeQpsSum, long outputSizeQpsCount,
                                                 boolean weighted,
                                                 Map<String, Map<Long, TaskMetricBucket>> metrics) {
        if (timestamp < startAt || timestamp >= endAt || (outputQpsCount <= 0 && outputSizeQpsCount <= 0)) {
            return;
        }
        long bucketStart = timestamp - timestamp % intervalMillis;
        TaskMetricBucket bucket = metrics.computeIfAbsent(taskId, key -> new HashMap<>())
                .computeIfAbsent(bucketStart, key -> new TaskMetricBucket());
        if (intervalMillis == 3_600_000L) {
            long minuteStart = timestamp - timestamp % 60_000L;
            HourlyTaskMinuteBucket minute = bucket.hourlyMinutes.computeIfAbsent(minuteStart,
                    ignored -> new HourlyTaskMinuteBucket());
            HourlyNodeMinuteMetric node = minute.byPhysicalNode.computeIfAbsent(physicalNodeId,
                    ignored -> new HourlyNodeMinuteMetric());
            if (outputQpsCount > 0) {
                if (weighted) {
                    node.outputQps.addWeighted(outputQpsSum, outputQpsCount);
                } else {
                    node.outputQps.addUnweighted(outputQpsSum);
                }
            }
            if (outputSizeQpsCount > 0) {
                if (weighted) {
                    node.outputSizeQps.addWeighted(outputSizeQpsSum, outputSizeQpsCount);
                } else {
                    node.outputSizeQps.addUnweighted(outputSizeQpsSum);
                }
            }
            return;
        }
        bucket.sampleCount += outputQpsCount;
        bucket.outputQpsSum += outputQpsSum == null ? 0D : outputQpsSum.doubleValue();
        bucket.outputSizeSampleCount += outputSizeQpsCount;
        bucket.outputSizeQpsSum += outputSizeQpsSum == null ? 0D : outputSizeQpsSum.doubleValue();
        if (StringUtils.isBlank(physicalNodeId)) {
            bucket.unattributedSampleCount += outputQpsCount;
            bucket.unattributedOutputSizeSampleCount += outputSizeQpsCount;
        } else {
            MetricAccumulator node = bucket.byPhysicalNode.computeIfAbsent(physicalNodeId, key -> new MetricAccumulator());
            node.sampleCount += outputQpsCount;
            node.outputQpsSum += outputQpsSum == null ? 0D : outputQpsSum.doubleValue();
            node.outputSizeSampleCount += outputSizeQpsCount;
            node.outputSizeQpsSum += outputSizeQpsSum == null ? 0D : outputSizeQpsSum.doubleValue();
        }
    }

    private long ceilToInterval(long value, long interval) {
        long remainder = value % interval;
        return remainder == 0 ? value : value + interval - remainder;
    }

    static Double nodeBucketContribution(TaskMetricBucket bucket, String physicalNodeId) {
        if (bucket == null || bucket.sampleCount == 0 || bucket.unattributedSampleCount > 0) {
            return null;
        }
        MetricAccumulator selected = bucket.byPhysicalNode.get(physicalNodeId);
        return selected == null ? null : selected.outputQpsSum / bucket.sampleCount;
    }

    static class TaskMetricBucket {
        private long sampleCount;
        private long unattributedSampleCount;
        private long outputSizeSampleCount;
        private long unattributedOutputSizeSampleCount;
        private double outputQpsSum;
        private double outputSizeQpsSum;
        private final Map<String, MetricAccumulator> byPhysicalNode = new HashMap<>();
        private final Map<Long, HourlyTaskMinuteBucket> hourlyMinutes = new HashMap<>();
        private boolean preAggregatedHour;
        private boolean hasUnattributedData;
        private boolean hasAttributableData;
        private boolean hasSelectedNodeData;
        private boolean partiallyCovered;
        private boolean incompleteSelected;
        private final Set<Long> coveredMinutes = new HashSet<>();
    }

    static class MetricAccumulator {
        private long sampleCount;
        private long outputSizeSampleCount;
        private double outputQpsSum;
        private double outputSizeQpsSum;
    }

    private static class HourlyTaskMinuteBucket {
        private final Map<String, HourlyNodeMinuteMetric> byPhysicalNode = new HashMap<>();
        private boolean outputQpsConflict;
        private boolean outputSizeQpsConflict;
    }

    private static class HourlyNodeMinuteMetric {
        private final HourlyMetricAccumulator outputQps = new HourlyMetricAccumulator();
        private final HourlyMetricAccumulator outputSizeQps = new HourlyMetricAccumulator();
    }

    private static class HourlyMetricAccumulator {
        private double weightedSum;
        private long weightedCount;
        private Double unweightedValue;
        private boolean hasUnweightedValue;
        private boolean conflict;

        private void addWeighted(Number sum, long count) {
            weightedSum += sum == null ? 0D : sum.doubleValue();
            weightedCount += count;
        }

        private void addUnweighted(Number value) {
            if (value == null) {
                return;
            }
            if (hasUnweightedValue) {
                conflict = true;
                return;
            }
            unweightedValue = value.doubleValue();
            hasUnweightedValue = true;
        }

        private boolean hasData() {
            return weightedCount > 0 || hasUnweightedValue;
        }
    }

    private record HourlyMetricValue(Double value, boolean hasData, boolean hasUnattributedData,
                                     boolean hasAttributableData, boolean hasSelectedNodeData,
                                     boolean partial) {
    }

    private static class HourlyTaskBucketSummary {
        private Double outputQps;
        private Double outputSizeQps;
        private boolean hasUnattributedData;
        private boolean hasAttributableData;
        private boolean hasSelectedNodeData;
        private boolean partiallyCovered;
        private boolean incompleteSelected;
        private final Set<Long> coveredMinutes = new HashSet<>();
    }

    private record HourlySourceSnapshot(Double outputQps, Long outputQpsCount,
                                         Double outputSizeQps, Long outputSizeQpsCount,
                                         boolean weighted) {
    }

    private static class DashboardMinuteSummaryIndex {
        private final Map<Long, MeasurementEntity.DashboardMinuteSummary> byTime;
        private final Set<Long> conflictingDates;

        private DashboardMinuteSummaryIndex(Map<Long, MeasurementEntity.DashboardMinuteSummary> byTime,
                                            Set<Long> conflictingDates) {
            this.byTime = byTime;
            this.conflictingDates = conflictingDates;
        }

        private static DashboardMinuteSummaryIndex empty() {
            return new DashboardMinuteSummaryIndex(Collections.emptyMap(), Collections.emptySet());
        }
    }

    private record MetricSummary(double sum, long count) {
    }

    static class DirtyTaskMinuteSummary {
        private final String taskId;
        private final String physicalNodeId;
        private final long dateMillis;
        private double outputQpsSum;
        private long outputQpsCount;
        private double outputSizeQpsSum;
        private long outputSizeQpsCount;
        private boolean initialized;
        private boolean conflicting;

        DirtyTaskMinuteSummary(String taskId, String physicalNodeId, long dateMillis) {
            this.taskId = taskId;
            this.physicalNodeId = physicalNodeId;
            this.dateMillis = dateMillis;
        }

        private void add(MeasurementEntity entity) {
            double nextOutputQpsSum = Optional.ofNullable(entity.getDashboardOutputQpsSum()).orElse(0D);
            long nextOutputQpsCount = Optional.ofNullable(entity.getDashboardOutputQpsCount()).orElse(0L);
            double nextOutputSizeQpsSum = Optional.ofNullable(entity.getDashboardOutputSizeQpsSum()).orElse(0D);
            long nextOutputSizeQpsCount = Optional.ofNullable(entity.getDashboardOutputSizeQpsCount()).orElse(0L);
            if (initialized) {
                conflicting |= Double.compare(outputQpsSum, nextOutputQpsSum) != 0
                        || outputQpsCount != nextOutputQpsCount
                        || Double.compare(outputSizeQpsSum, nextOutputSizeQpsSum) != 0
                        || outputSizeQpsCount != nextOutputSizeQpsCount;
                return;
            }
            outputQpsSum = nextOutputQpsSum;
            outputQpsCount = nextOutputQpsCount;
            outputSizeQpsSum = nextOutputSizeQpsSum;
            outputSizeQpsCount = nextOutputSizeQpsCount;
            initialized = true;
        }
    }

    private void collectTaskMetricsBatch(List<String> taskIdBatch, TaskMetricsRangeProfile rangeProfile, long queryStart,
                                         long startAt, long endAt, long interval,
                                         Map<String, Map<Long, List<double[]>>> taskBucketSamples) {
        Criteria criteria = Criteria.where(MetricCons.F_GRANULARITY).is(rangeProfile.queryGranularity)
                .and(PATH_TAGS_TYPE).is(SAMPLE_TYPE_TASK)
                .and(PATH_TAGS_TASK_ID).in(taskIdBatch)
                .and(MetricCons.F_DATE).gte(new Date(queryStart)).lte(new Date(endAt));
        Query query = new Query(criteria);
        query.fields().include(MetricCons.F_TAGS, MetricCons.F_DATE, MetricCons.F_GRANULARITY,
                PATH_SS_DATE,
                MetricCons.SS.VS.path(MetricCons.SS.VS.F_OUTPUT_QPS),
                MetricCons.SS.VS.path(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS));
        query.with(Sort.by(MetricCons.F_DATE).ascending());

        try (Stream<MeasurementEntity> stream = mongoOperations.stream(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME)) {
            stream.forEach(entity -> {
                if (entity == null || entity.getTags() == null) {
                    return;
                }
                String taskId = entity.getTags().get(FIELD_TAGS_TASK_ID);
                if (StringUtils.isBlank(taskId)) {
                    return;
                }
                collectTaskMetricsEntity(entity, rangeProfile, startAt, endAt, interval, taskId, taskBucketSamples);
            });
        }
    }

    private List<String> normalizeTaskIds(List<String> taskIds) {
        if (CollectionUtils.isEmpty(taskIds)) {
            return Collections.emptyList();
        }
        return new ArrayList<>(taskIds.stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    private void forEachTaskIdBatch(List<String> taskIds, Consumer<List<String>> batchConsumer) {
        for (int start = 0; start < taskIds.size(); start += TASK_ID_BATCH_SIZE) {
            batchConsumer.accept(taskIds.subList(start, Math.min(start + TASK_ID_BATCH_SIZE, taskIds.size())));
        }
    }

    private void collectTaskMetricsEntity(MeasurementEntity entity, TaskMetricsRangeProfile rangeProfile, long startAt, long endAt, long interval,
                                           String taskId, Map<String, Map<Long, List<double[]>>> taskBucketSamples) {
        if (CollectionUtils.isEmpty(entity.getSamples())) {
            return;
        }
        if (Granularity.GRANULARITY_MINUTE.equals(rangeProfile.queryGranularity)) {
            for (Sample sample : entity.getSamples()) {
                collectTaskMetricsSample(sample, startAt, endAt, interval, taskId, taskBucketSamples);
            }
            return;
        }

        Sample aggregateSample = new Sample();
        aggregateSample.setDate(Optional.ofNullable(entity.getDate()).orElse(entity.getSamples().get(0).getDate()));
        aggregateSample.setVs(entity.averageValues());
        collectTaskMetricsSample(aggregateSample, startAt, endAt, interval, taskId, taskBucketSamples);
    }

    private void collectTaskMetricsSample(Sample sample, long startAt, long endAt, long interval,
                                          String taskId, Map<String, Map<Long, List<double[]>>> taskBucketSamples) {
        if (sample == null || sample.getDate() == null || sample.getVs() == null) {
            return;
        }
        long ts = sample.getDate().getTime();
        if (ts < startAt || ts > endAt) {
            return;
        }
        long bucket = ts - ts % interval;
        double outputQps = Optional.ofNullable(sample.getVs().get(MetricCons.SS.VS.F_OUTPUT_QPS)).map(Number::doubleValue).orElse(0D);
        double outputSizeQps = Optional.ofNullable(sample.getVs().get(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS)).map(Number::doubleValue).orElse(0D);
        taskBucketSamples
                .computeIfAbsent(taskId, k -> new HashMap<>())
                .computeIfAbsent(bucket, k -> new ArrayList<>())
                .add(new double[]{outputQps, outputSizeQps});
    }

    private TaskMetricsRangeProfile resolveTaskMetricsRangeProfile(long startAt, long endAt) {
        long range = endAt - startAt;
        if (range <= 5L * 60L * 1000L) {
            return new TaskMetricsRangeProfile(Granularity.GRANULARITY_MINUTE, Granularity.getTimelineMillisInterval(Granularity.GRANULARITY_MINUTE));
        }
        if (range <= 60L * 60L * 1000L) {
            return new TaskMetricsRangeProfile(Granularity.GRANULARITY_MINUTE, Granularity.getGranularityMillisInterval(Granularity.GRANULARITY_MINUTE));
        }
        if (range <= 24L * 60L * 60L * 1000L) {
            return new TaskMetricsRangeProfile(Granularity.GRANULARITY_HOUR, Granularity.getGranularityMillisInterval(Granularity.GRANULARITY_HOUR));
        }
        return new TaskMetricsRangeProfile(Granularity.GRANULARITY_DAY, Granularity.getGranularityMillisInterval(Granularity.GRANULARITY_DAY));
    }

    private static class TaskMetricsRangeProfile {
        private final String queryGranularity;
        private final long interval;

        private TaskMetricsRangeProfile(String queryGranularity, long interval) {
            this.queryGranularity = queryGranularity;
            this.interval = interval;
        }
    }


    @Override
    public void queryTableMeasurement(String taskId, TableStatusInfoDto tableStatusInfoDto) {
        Criteria criteria = Criteria.where(PATH_TAGS_TASK_ID).is(taskId)
                .and(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE)
                .and(PATH_TAGS_TYPE).is(SAMPLE_TYPE_TASK);
        Query query = new Query(criteria);
        query.fields().include(MetricCons.F_SAMPLES, MetricCons.F_TAGS);
        query.with(Sort.by(MetricCons.F_LAST).descending());
        MeasurementEntity measurementEntity = mongoOperations.findOne(query, MeasurementEntity.class, "AgentMeasurementV2");
        if (measurementEntity == null) {
            return;
        }
        List<Sample> samples = measurementEntity.getSamples();
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(samples)) {
            Sample sample = samples.get(0);
            Long cdcDelayTime = null;
            Date lastData = null;
            if (sample.getVs().get(FIELD_SS_VS_REPLICATE_LAG) != null) {
                cdcDelayTime = Long.valueOf(sample.getVs().get(FIELD_SS_VS_REPLICATE_LAG).toString());
            }
            tableStatusInfoDto.setCdcDelayTime(cdcDelayTime);
            if (sample.getVs().get(FIELD_SS_VS_CURR_EVENT_TS) != null) {
                long LastDataChangeTime = sample.getVs().get(FIELD_SS_VS_CURR_EVENT_TS).longValue();
                if (LastDataChangeTime != 0) {
                    lastData = new Date(LastDataChangeTime);
                }
            }
            tableStatusInfoDto.setLastDataChangeTime(lastData);
        }

    }

    @Override
    public void cleanRemovedTableMeasurement(String taskId, String taskRecordId, String tableName) {
        Criteria criteria = createMinuteCriteria(taskId, taskRecordId, SAMPLE_TYPE_TABLE)
            .and(FIELD_TAGS_TABLE).is(tableName);

        Query query = new Query(criteria);
        mongoOperations.remove(query, MeasurementEntity.class, MeasurementEntity.COLLECTION_NAME);
    }

    protected Criteria createMinuteCriteria(String taskId, String taskRecordId, String sampleType) {
        return Criteria.where(PATH_TAGS_TASK_ID).is(taskId)
            .and(PATH_TAGS_TASK_RECORD_ID).is(taskRecordId)
            .and(PATH_TAGS_TYPE).is(sampleType)
            .and(MetricCons.F_GRANULARITY).is(Granularity.GRANULARITY_MINUTE);
    }

    protected void addTags2Criteria(Criteria criteria, Map<String, String> tags) {
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            addTags2Criteria(criteria, entry.getKey(), entry.getValue());
        }
    }

    protected void addTags2Criteria(Criteria criteria, String key, String value) {
        String keyPath = MetricCons.Tags.path(key);
        criteria.and(keyPath).is(value);
    }
}
