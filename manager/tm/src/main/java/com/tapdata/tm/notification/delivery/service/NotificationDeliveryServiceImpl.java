package com.tapdata.tm.notification.delivery.service;

import com.tapdata.tm.notification.delivery.constant.DeliveryStatus;
import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import com.tapdata.tm.notification.delivery.entity.NotificationDelivery;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
public class NotificationDeliveryServiceImpl implements NotificationDeliveryService {
    private final MongoTemplate mongoTemplate;

    public NotificationDeliveryServiceImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void record(List<NotificationDelivery> deliveries) {
        if (CollectionUtils.isEmpty(deliveries)) {
            return;
        }
        Date now = new Date();
        deliveries.forEach(d -> {
            if (d.getCreateAt() == null) {
                d.setCreateAt(now);
            }
            d.setLastUpdAt(now);
        });
        try {
            mongoTemplate.insertAll(deliveries);
        } catch (Exception e) {
            log.warn("Record notification deliveries failed, size: {}", deliveries.size(), e);
        }
    }

    @Override
    public List<NotificationDelivery> findByAlarmEventId(String alarmEventId) {
        if (alarmEventId == null) {
            return Collections.emptyList();
        }
        Query query = Query.query(Criteria.where("alarmEventId").is(alarmEventId))
                .with(Sort.by(Sort.Direction.DESC, "createTime"));
        return mongoTemplate.find(query, NotificationDelivery.class);
    }

    @Override
    public List<NotificationDelivery> findByMessageIds(Collection<String> messageIds) {
        if (CollectionUtils.isEmpty(messageIds)) {
            return Collections.emptyList();
        }
        Query query = Query.query(Criteria.where("messageId").in(messageIds))
                .with(Sort.by(Sort.Direction.DESC, "createTime"));
        return mongoTemplate.find(query, NotificationDelivery.class);
    }

    @Override
    public Map<String, DeliverySummaryVo> summarizeByAlarmEventIds(Collection<String> alarmEventIds) {
        List<String> ids = alarmEventIds == null ? Collections.emptyList()
                : alarmEventIds.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
        if (ids.isEmpty()) {
            return Collections.emptyMap();
        }
        Query query = Query.query(Criteria.where("alarmEventId").in(ids));
        query.fields().include("alarmEventId", "channel", "status", "sentAt", "createTime");
        Map<String, List<NotificationDelivery>> grouped = mongoTemplate.find(query, NotificationDelivery.class).stream()
                .collect(Collectors.groupingBy(NotificationDelivery::getAlarmEventId));
        Map<String, DeliverySummaryVo> result = new HashMap<>();
        grouped.forEach((id, list) -> result.put(id, summarize(list)));
        return result;
    }

    @Override
    public DeliverySummaryVo summarize(Collection<NotificationDelivery> deliveries) {
        DeliverySummaryVo summary = new DeliverySummaryVo();
        if (CollectionUtils.isEmpty(deliveries)) {
            return summary;
        }
        for (NotificationDelivery d : deliveries) {
            if (d.getChannel() == null || d.getStatus() == null) {
                continue;
            }
            DeliverySummaryVo.ChannelSummary channel = summary.getByChannel()
                    .computeIfAbsent(d.getChannel(), c -> new DeliverySummaryVo.ChannelSummary());
            if (!summary.getChannels().contains(d.getChannel())) {
                summary.getChannels().add(d.getChannel());
            }
            summary.setTotal(summary.getTotal() + 1);
            channel.setTotal(channel.getTotal() + 1);
            if (d.getStatus() == DeliveryStatus.SUCCESS) {
                summary.setSuccess(summary.getSuccess() + 1);
                channel.setSuccess(channel.getSuccess() + 1);
            } else if (d.getStatus() == DeliveryStatus.FAILED) {
                summary.setFailed(summary.getFailed() + 1);
                channel.setFailed(channel.getFailed() + 1);
            } else {
                summary.setSkipped(summary.getSkipped() + 1);
                channel.setSkipped(channel.getSkipped() + 1);
            }
            Date time = d.getSentAt() != null ? d.getSentAt() : d.getCreateAt();
            if (time != null && (summary.getLatestDeliveryTime() == null || time.after(summary.getLatestDeliveryTime()))) {
                summary.setLatestDeliveryTime(time);
            }
        }
        summary.setStatus(overall(summary));
        return summary;
    }

    static String overall(DeliverySummaryVo summary) {
        if (summary.getTotal() == 0) {
            return DeliverySummaryVo.NONE;
        }
        if (summary.getFailed() == 0) {
            return summary.getSuccess() > 0 ? DeliverySummaryVo.SUCCESS : DeliverySummaryVo.SKIPPED;
        }
        return summary.getSuccess() > 0 ? DeliverySummaryVo.PARTIAL_FAILED : DeliverySummaryVo.FAILED;
    }
}
