package com.tapdata.tm.log.service;

import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.log.dto.LogDto;
import com.tapdata.tm.log.entity.LogEntity;
import com.tapdata.tm.log.entity.LogEntityElastic;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

/**
 * @Author:
 * @Date: 2021/09/14
 * @Description:
 */
@Service
@RequiredArgsConstructor
@Slf4j
@Profile("dfs")
public class LogServiceElastic {

    @Value("${task.log.expireDay:7}")
    private int expireDay;

    @Value("${task.log.indexName:logs}")
    private String indexName;

    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    private IndexCoordinates getIndexCoordinates() {
        if (StringUtils.isEmpty(indexName)) {
            return elasticsearchRestTemplate.getIndexCoordinatesFor(LogEntityElastic.class);
        } else {
            return IndexCoordinates.of(indexName);
        }
    }

    public void save(LogDto logDto) {
        LogEntityElastic log=new LogEntityElastic();
        BeanUtil.copyProperties(logDto,log);
        elasticsearchRestTemplate.save(log, getIndexCoordinates());
    }


    public LogDto save(LogDto logDto, UserDetail userDetail) {

        if (logDto.getContextMap() == null) {
            return logDto;
        } else {
            if (logDto.getContextMap() instanceof Map) {
                if (!((Map<?, ?>)logDto.getContextMap()).containsKey("dataFlowId")) {
                    return logDto;
                }
            } else {
                return logDto;
            }
        }

        LogEntityElastic log = new LogEntityElastic();
        BeanUtil.copyProperties(logDto,log);

        log.setCreateAt(new Date());
        log.setUserId(userDetail.getUserId());
        log.setCreateUser(userDetail.getUsername());
        log.setLastUpdAt(new Date());
        log.setLastUpdBy(userDetail.getUserId());
        elasticsearchRestTemplate.save(log, getIndexCoordinates());

        return logDto;
    }

    private CriteriaQuery prepareFind(Filter filter, UserDetail userDetail) {
        if (filter == null) {
            filter = new Filter();
        }

        Criteria criteria = new Criteria();
        Where where = filter.getWhere();
        // apply data flow id filter
        Object dataFlowFilter = where.get("contextMap.dataFlowId");
        if (!(dataFlowFilter instanceof Map) || ((Map) dataFlowFilter).get("$eq") == null || StringUtils.isBlank(((Map) dataFlowFilter).get("$eq").toString())) {
            return null;
        }
        Criteria dataFlowIdCriteria = new Criteria("contextMap.dataFlowId").is(((Map) dataFlowFilter).get("$eq").toString());
        criteria.and(dataFlowIdCriteria);
        // apply user detail filter
        if (userDetail != null) {
            Criteria userIdCriteria = new Criteria("userId").is(userDetail.getUserId());
            criteria.and(userIdCriteria);
        }
        // apply level filter
        Object levelFilter = where.get("level");
        List<Object> levels;
        if (!(levelFilter instanceof Map) || ((Map) levelFilter).isEmpty() || !(((Map) levelFilter).get("$in") instanceof List)) {
            levels = Arrays.asList("INFO", "WARN", "ERROR");
        } else {
            levels = (List<Object>) (((Map) levelFilter).get("$in"));
        }
        Criteria levelCriteria = new Criteria("level.keyword").in(levels);
        criteria.and(levelCriteria);

        // apply other search filter
        if ((where.get("$or") instanceof List)) {
            Criteria orCriteria = new Criteria();
            for(Object item : (List) where.get("$or")) {
                if (item instanceof Map) {
                    for (Object key : ((Map) item).keySet()) {
                        if (((Map) item).get(key) instanceof Map) {
                            if (((Map) ((Map) item).get(key)).get("$regex") != null) {Criteria criteriaItem = new Criteria(key.toString()).expression(((Map) ((Map) item).get(key)).get("$regex").toString());
                                orCriteria = orCriteria.or(criteriaItem);
                            }
                        }
                    }
                }

            }
            criteria.subCriteria(orCriteria);
        }

        // get the query
        CriteriaQuery query = new CriteriaQuery(criteria);
        // add sort, ignore the key to order since the generated id in elasticsearch is not time related
        // here we forced to use "date" to order the document
        Sort sort = null;
        if (filter.getOrder() != null && StringUtils.isNotBlank(filter.getOrder().toString())) {
            String order = filter.getOrder().toString();
            String[] orderList = order.split(" ");
            if (orderList.length == 2) {
                switch (orderList[1].toUpperCase()) {
                    case "ASC":
                        sort = Sort.by("millis").ascending().and(Sort.by("_id").ascending());
                        break;
                    default:
                        sort = Sort.by("millis").descending().and(Sort.by("_id").descending());
                }
            }
        }

        // apply the pagination request
        query.setPageable(new TmPageable(filter.getSkip(), filter.getLimit(), sort));
        // add limit
        query.setMaxResults(filter.getLimit());

        return query;
    }

    public CriteriaQuery find(String dataFlowId, String order, Long limit) {
        Filter filter = new Filter();
        filter.setWhere(new Where());
        filter.getWhere().put("contextMap.dataFlowId", new HashMap<String, String>(){{put("$eq", dataFlowId);}});

        filter.setOrder(order);
        if (limit != null) {
            filter.setLimit(limit.intValue());
        }

        return prepareFind(filter, null);
    }

    public SearchHits<LogEntityElastic> find(CriteriaQuery query) {
        return elasticsearchRestTemplate.search(query, LogEntityElastic.class, getIndexCoordinates());
    }

    public Page<LogDto> find(Filter filter, UserDetail userDetail) {
        List<LogDto> logs = new ArrayList<>();

        CriteriaQuery query = prepareFind(filter, userDetail);
        if (query == null) {
            return new Page<>(0, logs);
        }

        SearchHits<LogEntityElastic> searchHits = elasticsearchRestTemplate.search(query, LogEntityElastic.class, getIndexCoordinates());

        searchHits.stream().iterator().forEachRemaining(searchHit -> {
            LogEntityElastic entity = searchHit.getContent();
            try {
                LogDto dto = LogDto.class.getDeclaredConstructor().newInstance();
                BeanUtils.copyProperties(entity, dto);
                logs.add(dto);
            } catch (Exception e) {
                log.error("Convert dto " + LogDto.class + " failed.", e);
            }
        });

        return new Page<>(searchHits.getTotalHits(), logs);
    }

    public void export(Number start, Number end, String dataFlowId, OutputStream outputStream) {
        if (start == null) {
            start = new Date().getTime() - 1000 * 60 * 60 * 24; // 默认取一天
        }

        Criteria criteria = new Criteria("millis").greaterThanEqual(start);
        if (end != null) {
            criteria.lessThanEqual(end);
        }

        criteria.and(new Criteria("contextMap.dataFlowId").is(dataFlowId));

        Query query = new CriteriaQuery(criteria);
        query.addSort(Sort.by("millis").ascending());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        AtomicInteger count = new AtomicInteger(0);

        SearchHits<LogEntityElastic> searchHits = elasticsearchRestTemplate.search(query, LogEntityElastic.class, getIndexCoordinates());

        searchHits.stream().iterator().forEachRemaining(searchHit -> {
            StringBuffer sb = new StringBuffer();

            LogEntityElastic log = searchHit.getContent();
            sb.append("[").append(log.getLevel()).append("]");
            if (log.getDate() instanceof Long || log.getMillis() instanceof Long) {
                sb.append(" ").append(sdf.format(new Date(
                        (Long)(log.getMillis() != null ? log.getMillis() : log.getDate())
                )));
            } else {
                sb.append(" [invalid log datetime]");
            }
            sb.append(" ").append(log.getThreadName());
            sb.append(" ").append(log.getLoggerName());
            sb.append(" ").append(log.getMessage());

            sb.append("\n");
            count.addAndGet(1);

            try {
                outputStream.write(sb.toString().getBytes());
            } catch (IOException e) {
                throw new BizException("Export.IOError", e);
            }
        });

        if (count.get() == 0) {
            try {
                outputStream.write(("Can't find any logs by query " + query).getBytes());
            } catch (IOException e) {
                throw new BizException("Export.IOError", e);
            }
        }
    }

    /**
     * Trigger at 0:00 every day
     */
    @Scheduled(cron = "${task.log.cron:0 0 0 * * ?}")
    @SchedulerLock(name="cleanUpLogs", lockAtLeastFor = "PT10M", lockAtMostFor = "PT10M")
    public void deleteExpiredLogs() {

        //IndexCoordinates index = IndexCoordinates.of("logs-leon");
        IndexCoordinates index = getIndexCoordinates();

        Query query = new NativeSearchQueryBuilder()
                .addAggregation(
                        terms("groupByDataFlowId")
                                .field("contextMap.dataFlowId.keyword")
                                // TODO: Paginated queries should be used
                                .size(Integer.MAX_VALUE)
                                .subAggregation(
                                max("maxMillis").field("millis")))
                .withMaxResults(0)
                .build();

        SearchHits<Map> result = elasticsearchRestTemplate.search(query, Map.class, index);

        Map<String, Long[]> dataFlowIdAndLastLogAt = new HashMap<>();
        if (result.hasAggregations() && result.getAggregations() != null) {
            Aggregation groupTerms = result.getAggregations().get("groupByDataFlowId");
            if (groupTerms instanceof ParsedStringTerms) {
                List<? extends Terms.Bucket> buckets = ((ParsedStringTerms) groupTerms).getBuckets();


                buckets.forEach(bucket -> {
                    String dataFlowId = bucket.getKeyAsString();
                    long docCount = bucket.getDocCount();
                    Aggregation maxMillisAggr = bucket.getAggregations().get("maxMillis");
                    double maxMillis = 0;
                    if (maxMillisAggr instanceof ParsedMax) {
                        maxMillis = ((ParsedMax) maxMillisAggr).getValue();
                    }
                    dataFlowIdAndLastLogAt.put(dataFlowId, new Long[]{Double.valueOf(maxMillis).longValue(), docCount});
                });
            }
        }

        if (dataFlowIdAndLastLogAt.isEmpty()) {
            log.warn("Not found any data flow logs.");
            return;
        }

        long expiredMillisTime = expireDay * 24 * 60 * 60 * 1000;
        int deleted = 0;
        for (String dataFlowId : dataFlowIdAndLastLogAt.keySet()) {

            Long[] data = dataFlowIdAndLastLogAt.get(dataFlowId);
            long deletedMillisTime = data[0] - expiredMillisTime;

            if (deletedMillisTime <= 0) {
                continue;
            }

            Criteria c = Criteria.where("contextMap.dataFlowId").is(dataFlowId)
                    .and(Criteria.where("millis").lessThanEqual(deletedMillisTime));
            query = new CriteriaQuery(c);

            ByQueryResponse deleteResult = elasticsearchRestTemplate.delete(query, LogEntityElastic.class, index);
            deleted += deleteResult.getDeleted();
            log.debug("Clean up {} row of data flow {} expired logs.", deleteResult.getDeleted(), dataFlowId);
        }

        log.info("Total clean up {} row of expired logs", deleted);
    }

    public void deleteLogsByDataFlowId(String dataFlowId) {
       try {
           Criteria c = Criteria.where("contextMap.dataFlowId").is(dataFlowId);
           CriteriaQuery query = new CriteriaQuery(c);

           ByQueryResponse deleteResult = elasticsearchRestTemplate.delete(query, LogEntityElastic.class, getIndexCoordinates());
           deleteResult.getDeleted();
           log.debug("Clean up {} row of data flow {} expired logs.", deleteResult.getDeleted(), dataFlowId);
       } catch (Exception e) {
            log.error("Clean up data flow logs fail", e);
       }
    }
}
