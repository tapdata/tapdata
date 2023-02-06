package com.tapdata.tm.behavior.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.behavior.BehaviorCode;
import com.tapdata.tm.behavior.dto.BehaviorDto;
import com.tapdata.tm.behavior.entity.BehaviorEntity;
import com.tapdata.tm.behavior.repository.BehaviorRepository;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.service.DataFlowService;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightDto;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/6/21 下午2:45
 */
@Service
@Slf4j
public class BehaviorService extends BaseService<BehaviorDto, BehaviorEntity, ObjectId, BehaviorRepository> {

    private Map<String, BehaviorEntity> cacheForInsight = new ConcurrentHashMap<>();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");

    @Autowired
    private DataFlowService dataFlowService;

    public BehaviorService(@NonNull BehaviorRepository repository) {
        super(repository, BehaviorDto.class, BehaviorEntity.class);
    }

    @Override
    protected void beforeSave(BehaviorDto dto, UserDetail userDetail) {

    }

    /**
     * 每个10秒刷新 缓存数据到数据库
     */
    @Scheduled(fixedDelay = 10000L)
    private void flushCacheToDB() {

        BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
        AtomicInteger counter = new AtomicInteger();
        Set<String> ids = cacheForInsight.keySet();
        int size = ids.size();
        for (String id : ids) {

            BehaviorEntity behaviorDto = cacheForInsight.get(id);
            Criteria criteria = Criteria.where("dataFlowId").is(behaviorDto.getDataFlowId()).and("code").is(behaviorDto.getCode())
                    .and("period").is(behaviorDto.getPeriod());
            Query query = Query.query(criteria);
            Update update = repository.buildUpdateSet(behaviorDto);
            bulkOperations.upsert(query, update);
            counter.getAndIncrement();

            if (counter.get() >= 1000) {
                bulkOperations.execute();
                counter.set(0);
                if ( counter.get() < size) {
                    bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
                }
            }
        }

        if (counter.get() > 0) {
            bulkOperations.execute();
        }

        long expiration = System.currentTimeMillis() - 60*1000; // 清理掉超过1分钟不更新的缓存
        cacheForInsight.entrySet().stream().filter(a -> a.getValue().getLastUpdateTime() < expiration)
                .forEach(e -> {
                    cacheForInsight.remove(e.getKey());
                });

    }

    /**
     * Record user behavior
     * @param behaviorDto
     * @param userDetail
     */
    public void trace(BehaviorEntity behaviorDto, UserDetail userDetail) {
        Assert.notNull(userDetail, "Parameter userDetail can't be empty");
        Assert.notNull(behaviorDto, "Parameter behaviorEntity can't be empty");

        try {
            behaviorDto.setUserId(userDetail.getUserId());
            behaviorDto.setExternalUserId(userDetail.getExternalUserId());
            repository.save(behaviorDto, userDetail);
        } catch (Exception e) {
            log.error("trace task behavior failed", e);
        }
    }

    /**
     * 记录任务统计
     *  FlowEngine 调用统计接口非常频繁，这里采用内存缓存 + 批量写入数据库
     * @param dataFlowInsightDto
     * @param userDetail
     */
    @Deprecated
    public void trace(DataFlowInsightDto dataFlowInsightDto, UserDetail userDetail) {
        Assert.notNull(userDetail, "Parameter userDetail can't be empty");
        Assert.notNull(dataFlowInsightDto, "Parameter dataFlowInsightDto can't be empty");

        try {
            BehaviorEntity behaviorDto = null;
            if (cacheForInsight.containsKey(dataFlowInsightDto.getDataFlowId())) {
                behaviorDto = cacheForInsight.get(dataFlowInsightDto.getDataFlowId());
                behaviorDto.setAttrs(dataFlowInsightDto.getStatsData());
                behaviorDto.setCounter(behaviorDto.getCounter() + 1);
            } else {
                behaviorDto = new BehaviorEntity();
                behaviorDto.setUserId(dataFlowInsightDto.getUserId());
                behaviorDto.setDataFlowId(dataFlowInsightDto.getDataFlowId());
                behaviorDto.setAttrs(dataFlowInsightDto.getStatsData());
                behaviorDto.setCode(BehaviorCode.statsForDataFlowInsight.name());
                cacheForInsight.put(dataFlowInsightDto.getDataFlowId(), behaviorDto);

                Query query = Query.query(Criteria.where("_id").is(new ObjectId(dataFlowInsightDto.getDataFlowId())));
                query.fields().include("agentId");
                DataFlowDto dataFlow = dataFlowService.findOne(query);
                if (dataFlow != null) {
                    behaviorDto.setAgentId(dataFlow.getAgentId());
                }
            }

            repository.applyUserDetail(behaviorDto, userDetail);
            repository.beforeCreateEntity(behaviorDto, userDetail);

            behaviorDto.setPeriod(sdf.format(new Date()));
            behaviorDto.setLastUpdateTime(System.currentTimeMillis());
        } catch (Exception e) {
            log.error("trace task behavior failed", e);
        }
    }

    /**
     * 记录任务行为
     * @param dataFlowId
     * @param userDetail
     * @param behaviorCode
     */
    @Deprecated
    public void trace(String dataFlowId, UserDetail userDetail, BehaviorCode behaviorCode) {

        Assert.notNull(dataFlowId, "Parameter dataFlowId can't be empty");
        Assert.notNull(userDetail, "Parameter userDetail can't be empty");
        Assert.notNull(behaviorCode, "Parameter behaviorCode can't be empty");

        try {
            BehaviorEntity behaviorDto = new BehaviorEntity();
            behaviorDto.setDataFlowId(dataFlowId);
            behaviorDto.setCode(behaviorCode.name());

            behaviorDto.setExternalUserId(userDetail.getExternalUserId());
            repository.applyUserDetail(behaviorDto, userDetail);
            repository.beforeCreateEntity(behaviorDto, userDetail);

            Criteria criteria = Criteria.where("dataFlowId").is(dataFlowId).and("code").is(behaviorDto.getCode());
            repository.upsert(Query.query(criteria), behaviorDto, userDetail);
        } catch (Exception e) {
            log.info("trace task{} behavior failed", dataFlowId, e);
        }

    }

    /**
     * 记录任务行为
     * @param dataFlowDto
     * @param userDetail
     * @param behaviorCode
     */
    @Deprecated
    public void trace(DataFlowDto dataFlowDto, UserDetail userDetail, BehaviorCode behaviorCode) {

        Assert.notNull(dataFlowDto, "Parameter dataFlowDto can't be empty");
        Assert.notNull(userDetail, "Parameter userDetail can't be empty");
        Assert.notNull(behaviorCode, "Parameter behaviorCode can't be empty");

        try {
            BehaviorDto behaviorDto = new BehaviorDto();
            behaviorDto.setAgentId(dataFlowDto.getAgentId());
            if (dataFlowDto.getId() != null) {
                behaviorDto.setDataFlowId(dataFlowDto.getId().toHexString());
            }

            behaviorDto.setCode(behaviorCode.name());
            behaviorDto.setExternalUserId(userDetail.getExternalUserId());

            Map<String, Object> attrs = new HashMap<>();
            attrs.put("name", dataFlowDto.getName());
            attrs.put("status", dataFlowDto.getStatus());
            attrs.put("executeMode", dataFlowDto.getExecuteMode());
            attrs.put("category", dataFlowDto.getCategory());
            attrs.put("setting", dataFlowDto.getSetting());
            attrs.put("dataSourceModel", dataFlowDto.getDataSourceModel());

            behaviorDto.setAttrs(attrs);

            this.save(behaviorDto, userDetail);
        } catch (Exception e) {
            log.error("trace task{} behavior failed", dataFlowDto.getId(), e);
        }

    }
}
