package com.tapdata.tm.alarm.service;

import com.tapdata.tm.alarm.constant.AlarmObjectType;
import com.tapdata.tm.alarm.dto.AlarmEventQuery;
import com.tapdata.tm.alarm.dto.AlarmEventStatsVo;
import com.tapdata.tm.alarm.dto.AlarmEventVo;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import com.tapdata.tm.notification.delivery.service.NotificationDeliveryService;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.service.ModuleAccess;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 告警事件查询：筛选、可见范围、对象快照与投递概览。摘要渲染委托 {@link AlarmService#renderSummary}。
 */
@Slf4j
@Service
public class AlarmEventQueryService {
    public static final String NOTIFIED_NONE = "NONE";
    public static final String NOTIFIED_ANY = "NOTIFIED";
    private static final List<String> EMPTY = Arrays.asList(null, "");
    private static final List<Level> SEVERE = Arrays.asList(Level.CRITICAL, Level.EMERGENCY);

    private final MongoTemplate mongoTemplate;
    private final TaskService taskService;
    private final InspectService inspectService;
    private final AlarmService alarmService;
    private final NotificationDeliveryService deliveryService;
    private final ModuleAccess moduleAccess;
    private final UserLogService userLogService;

    public AlarmEventQueryService(MongoTemplate mongoTemplate, TaskService taskService, InspectService inspectService,
                                  AlarmService alarmService, NotificationDeliveryService deliveryService,
                                  ModuleAccess moduleAccess, UserLogService userLogService) {
        this.mongoTemplate = mongoTemplate;
        this.taskService = taskService;
        this.inspectService = inspectService;
        this.alarmService = alarmService;
        this.deliveryService = deliveryService;
        this.moduleAccess = moduleAccess;
        this.userLogService = userLogService;
    }

    public Page<AlarmEventVo> list(AlarmEventQuery q, UserDetail user, Locale locale) {
        moduleAccess.require(user, DataPermissionEnumsName.V2_ALARM_EVENT);
        AlarmEventQuery query = q == null ? new AlarmEventQuery() : q;
        Criteria criteria = criteria(query, user);
        if (criteria == null) {
            return Page.empty();
        }
        int page = Math.max(1, Objects.requireNonNullElse(query.getPage(), 1));
        int size = Math.min(500, Math.max(1, Objects.requireNonNullElse(query.getSize(), 20)));
        Sort sort = Sort.by(Sort.Direction.DESC, "lastOccurrenceTime");

        if (StringUtils.isNotBlank(query.getNotified())) {
            // 投递状态不在告警记录上，先取候选 ID 再按投递汇总过滤
            Query idQuery = new Query(criteria).with(sort);
            idQuery.fields().include("_id");
            List<String> ids = mongoTemplate.find(idQuery, AlarmInfo.class).stream()
                    .map(a -> a.getId().toHexString()).collect(Collectors.toList());
            Map<String, DeliverySummaryVo> summaries = deliveryService.summarizeByAlarmEventIds(ids);
            List<String> matched = ids.stream()
                    .filter(id -> matchesNotified(summaries.get(id), query.getNotified()))
                    .collect(Collectors.toList());
            int from = Math.min((page - 1) * size, matched.size());
            List<ObjectId> pageIds = matched.subList(from, Math.min(from + size, matched.size())).stream()
                    .map(ObjectId::new).collect(Collectors.toList());
            List<AlarmInfo> infos = pageIds.isEmpty() ? Collections.emptyList()
                    : mongoTemplate.find(Query.query(Criteria.where("_id").in(pageIds)).with(sort), AlarmInfo.class);
            return Page.page(toVos(infos, locale, summaries), matched.size());
        }

        Query dataQuery = new Query(criteria);
        long total = mongoTemplate.count(dataQuery, AlarmInfo.class);
        if (total == 0) {
            return Page.empty();
        }
        dataQuery.with(sort).skip((long) (page - 1) * size).limit(size);
        List<AlarmInfo> infos = mongoTemplate.find(dataQuery, AlarmInfo.class);
        Map<String, DeliverySummaryVo> summaries = deliveryService.summarizeByAlarmEventIds(
                infos.stream().map(a -> a.getId().toHexString()).collect(Collectors.toList()));
        return Page.page(toVos(infos, locale, summaries), total);
    }

    public AlarmEventVo detail(String id, UserDetail user, Locale locale) {
        moduleAccess.require(user, DataPermissionEnumsName.V2_ALARM_EVENT);
        ObjectId objectId = MongoUtils.toObjectId(id);
        AlarmInfo info = objectId == null ? null : mongoTemplate.findById(objectId, AlarmInfo.class);
        if (info == null || !visible(info, user)) {
            throw new BizException("Alarm.NotFound");
        }
        String key = info.getId().toHexString();
        return toVos(Collections.singletonList(info), locale,
                deliveryService.summarizeByAlarmEventIds(Collections.singletonList(key))).get(0);
    }

    public AlarmEventStatsVo stats(UserDetail user) {
        moduleAccess.require(user, DataPermissionEnumsName.V2_ALARM_EVENT);
        Criteria scope = visibility(user);
        if (scope == null) {
            return new AlarmEventStatsVo();
        }
        Date today = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
        long alarming = count(scope, Criteria.where("status").is(AlarmStatusEnum.ING.name()));
        long severe = count(scope, Criteria.where("status").is(AlarmStatusEnum.ING.name()), Criteria.where("level").in(names(SEVERE)));
        long todayNew = count(scope, Criteria.where("firstOccurrenceTime").gte(today));
        long todayRecovered = count(scope, Criteria.where("status").is(AlarmStatusEnum.RECOVER.name()),
                Criteria.where("recoveryTime").gte(today));
        return new AlarmEventStatsVo(alarming, severe, todayNew, todayRecovered);
    }

    private long count(Criteria scope, Criteria... more) {
        List<Criteria> all = new ArrayList<>();
        all.add(scope);
        all.addAll(Arrays.asList(more));
        return mongoTemplate.count(new Query(new Criteria().andOperator(all.toArray(new Criteria[0]))), AlarmInfo.class);
    }

    /** 返回 null 表示用户无可见告警 */
    Criteria criteria(AlarmEventQuery q, UserDetail user) {
        Criteria scope = visibility(user);
        if (scope == null) {
            return null;
        }
        List<Criteria> and = new ArrayList<>();
        and.add(scope);
        if (CollectionUtils.isNotEmpty(q.getStatus())) {
            and.add(Criteria.where("status").in(names(q.getStatus())));
        }
        if (CollectionUtils.isNotEmpty(q.getLevel())) {
            and.add(Criteria.where("level").in(names(q.getLevel())));
        }
        if (CollectionUtils.isNotEmpty(q.getMetric())) {
            and.add(Criteria.where("metric").in(names(q.getMetric())));
        }
        if (CollectionUtils.isNotEmpty(q.getObjectType())) {
            and.add(new Criteria().orOperator(q.getObjectType().stream().map(AlarmEventQueryService::objectTypeCriteria)
                    .toArray(Criteria[]::new)));
        }
        if (StringUtils.isNotBlank(q.getKeyword())) {
            String keyword = q.getKeyword().trim();
            Pattern pattern = Pattern.compile(Pattern.quote(keyword), Pattern.CASE_INSENSITIVE);
            List<Criteria> or = new ArrayList<>(Arrays.asList(
                    Criteria.where("name").regex(pattern), Criteria.where("node").regex(pattern),
                    Criteria.where("taskId").is(keyword), Criteria.where("nodeId").is(keyword),
                    Criteria.where("inspectId").is(keyword), Criteria.where("connectionId").is(keyword),
                    Criteria.where("agentId").is(keyword), Criteria.where("serverId").is(keyword),
                    Criteria.where("apiId").is(keyword)));
            and.add(new Criteria().orOperator(or.toArray(new Criteria[0])));
        }
        if (StringUtils.isNotBlank(q.getAgentId())) {
            and.add(Criteria.where("agentId").is(q.getAgentId()));
        }
        if (q.getStart() != null || q.getEnd() != null) {
            Criteria time = Criteria.where("lastOccurrenceTime");
            if (q.getStart() != null) {
                time.gte(new Date(q.getStart()));
            }
            if (q.getEnd() != null) {
                time.lte(new Date(q.getEnd()));
            }
            and.add(time);
        }
        return new Criteria().andOperator(and.toArray(new Criteria[0]));
    }

    static Criteria objectTypeCriteria(AlarmObjectType type) {
        List<String> special = Arrays.asList(AlarmTypeEnum.API_SERVER_ALARM.name(), AlarmTypeEnum.DATASOURCE_MONITOR_ALARM.name());
        Criteria noInspect = Criteria.where("inspectId").in(EMPTY);
        switch (type) {
            case INSPECT:
                return Criteria.where("inspectId").nin(EMPTY);
            case API_SERVER:
                return new Criteria().andOperator(noInspect, new Criteria().orOperator(
                        Criteria.where("type").is(AlarmTypeEnum.API_SERVER_ALARM.name()),
                        Criteria.where("serverId").nin(EMPTY), Criteria.where("apiId").nin(EMPTY)));
            case CONNECTION:
                return new Criteria().andOperator(noInspect, Criteria.where("type").ne(AlarmTypeEnum.API_SERVER_ALARM.name()),
                        Criteria.where("serverId").in(EMPTY), Criteria.where("apiId").in(EMPTY),
                        new Criteria().orOperator(Criteria.where("type").is(AlarmTypeEnum.DATASOURCE_MONITOR_ALARM.name()),
                                new Criteria().andOperator(Criteria.where("taskId").in(EMPTY), Criteria.where("connectionId").nin(EMPTY))));
            case TASK:
            case NODE:
                return new Criteria().andOperator(noInspect, Criteria.where("type").nin(special),
                        Criteria.where("serverId").in(EMPTY), Criteria.where("apiId").in(EMPTY),
                        Criteria.where("taskId").nin(EMPTY),
                        type == AlarmObjectType.NODE ? Criteria.where("nodeId").nin(EMPTY) : Criteria.where("nodeId").in(EMPTY));
            case AGENT:
            default:
                return new Criteria().andOperator(noInspect, Criteria.where("type").nin(special),
                        Criteria.where("serverId").in(EMPTY), Criteria.where("apiId").in(EMPTY),
                        Criteria.where("taskId").in(EMPTY), Criteria.where("connectionId").in(EMPTY));
        }
    }

    /** 枚举按名称存储，查询条件直接使用名称，避免依赖驱动编码 */
    static List<String> names(List<? extends Enum<?>> values) {
        return values.stream().filter(Objects::nonNull).map(Enum::name).collect(Collectors.toList());
    }

    static boolean matchesNotified(DeliverySummaryVo summary, String notified) {
        String status = summary == null ? DeliverySummaryVo.NONE : summary.getStatus();
        switch (notified) {
            case NOTIFIED_NONE:
                return DeliverySummaryVo.NONE.equals(status);
            case NOTIFIED_ANY:
                return !DeliverySummaryVo.NONE.equals(status);
            default:
                return notified.equals(status);
        }
    }

    /**
     * 关闭告警：需要关闭按钮权限，只能关闭可见范围内的告警；关闭只表示结束跟踪，写入操作审计。
     */
    public void close(List<String> ids, UserDetail user) {
        moduleAccess.require(user, DataPermissionEnumsName.V2_ALARM_EVENT_ACK_CLOSE);
        if (CollectionUtils.isEmpty(ids)) {
            throw new BizException("IllegalArgument", "ids");
        }
        for (String id : ids) {
            ObjectId objectId = MongoUtils.toObjectId(id);
            AlarmInfo info = objectId == null ? null : mongoTemplate.findById(objectId, AlarmInfo.class);
            if (info == null || !visible(info, user)) {
                throw new BizException("Alarm.NotFound");
            }
        }
        alarmService.close(ids.toArray(new String[0]), user);
        for (String id : ids) {
            userLogService.addUserLog(Modular.ALARM, Operation.CLOSE, user, id, id);
        }
    }

    /**
     * 有告警模块权限可见全部告警；云版仍仅可见有权限的任务和校验任务的告警。
     */
    Criteria visibility(UserDetail user) {
        if (moduleAccess.allData(user, DataPermissionEnumsName.V2_ALARM_EVENT)) {
            return new Criteria();
        }
        Query taskQuery = new Query();
        taskQuery.fields().include("_id");
        List<String> taskIds = taskService.findAllDto(taskQuery, user).stream()
                .map(t -> t.getId().toHexString()).collect(Collectors.toList());
        Query inspectQuery = new Query();
        inspectQuery.fields().include("_id");
        List<String> inspectIds = inspectService.findAllDto(inspectQuery, user).stream()
                .map(i -> i.getId().toHexString()).collect(Collectors.toList());
        if (taskIds.isEmpty() && inspectIds.isEmpty()) {
            return null;
        }
        return new Criteria().orOperator(Criteria.where("taskId").in(taskIds), Criteria.where("inspectId").in(inspectIds));
    }

    boolean visible(AlarmInfo info, UserDetail user) {
        if (moduleAccess.allData(user, DataPermissionEnumsName.V2_ALARM_EVENT)) {
            return true;
        }
        Criteria scope = visibility(user);
        return scope != null && mongoTemplate.exists(Query.query(new Criteria().andOperator(
                Criteria.where("_id").is(info.getId()), scope)), AlarmInfo.class);
    }

    List<AlarmEventVo> toVos(List<AlarmInfo> infos, Locale locale, Map<String, DeliverySummaryVo> summaries) {
        if (CollectionUtils.isEmpty(infos)) {
            return new ArrayList<>();
        }
        List<String> taskIds = infos.stream().map(AlarmInfo::getTaskId).filter(StringUtils::isNotBlank)
                .distinct().collect(Collectors.toList());
        Map<String, TaskDto> tasks = taskIds.isEmpty() ? Collections.emptyMap()
                : taskService.findAllTasksByIds(taskIds).stream().filter(t -> !t.is_deleted())
                .collect(Collectors.toMap(t -> t.getId().toHexString(), Function.identity(), (a, b) -> a));
        List<String> inspectIds = infos.stream().map(AlarmInfo::getInspectId).filter(StringUtils::isNotBlank)
                .distinct().collect(Collectors.toList());
        Set<String> inspects = inspectIds.isEmpty() ? Collections.emptySet()
                : inspectService.findAllByIds(inspectIds).stream()
                .filter(i -> i.getIs_deleted() == null || !i.getIs_deleted())
                .map(BaseDto::getId).map(ObjectId::toHexString).collect(Collectors.toSet());
        Map<String, DeliverySummaryVo> delivery = summaries == null ? Collections.emptyMap() : summaries;
        return infos.stream().map(info -> toVo(info, locale, tasks, inspects, delivery)).collect(Collectors.toList());
    }

    private AlarmEventVo toVo(AlarmInfo info, Locale locale, Map<String, TaskDto> tasks, Set<String> inspects,
                              Map<String, DeliverySummaryVo> delivery) {
        AlarmEventVo vo = new AlarmEventVo();
        String id = info.getId().toHexString();
        AlarmObjectType type = AlarmObjectType.of(info);
        vo.setId(id);
        vo.setLevel(info.getLevel());
        vo.setStatus(info.getStatus());
        vo.setMetric(info.getMetric());
        vo.setSummary(render(info, locale));
        vo.setObjectType(type);
        vo.setTaskId(info.getTaskId());
        vo.setTaskName(info.getName());
        vo.setNodeId(info.getNodeId());
        vo.setNodeName(info.getNode());
        vo.setAgentId(info.getAgentId());
        vo.setConnectionId(info.getConnectionId());
        vo.setInspectId(info.getInspectId());
        vo.setServerId(info.getServerId());
        vo.setApiId(info.getApiId());
        vo.setCurrentValue(info.getCurrentValue());
        vo.setThreshold(info.getThreshold());
        vo.setFirstOccurrenceTime(info.getFirstOccurrenceTime());
        vo.setLastOccurrenceTime(info.getLastOccurrenceTime());
        vo.setOccurrenceCount(info.occurrenceCount());
        vo.setRecoveryTime(info.getRecoveryTime());
        vo.setCloseTime(info.getCloseTime());
        vo.setCloseBy(info.getCloseBy());
        vo.setArchived(Boolean.TRUE.equals(info.getArchived()));
        vo.setLastNotifyTime(info.getLastNotifyTime());
        vo.setDeliverySummary(delivery.getOrDefault(id, new DeliverySummaryVo()));
        TaskDto task = StringUtils.isBlank(info.getTaskId()) ? null : tasks.get(info.getTaskId());
        if (task != null) {
            vo.setSyncType(task.getSyncType());
        }
        switch (type) {
            case INSPECT:
                vo.setObjectId(info.getInspectId());
                vo.setObjectName(info.getName());
                vo.setObjectDeleted(!inspects.contains(info.getInspectId()));
                break;
            case TASK:
                vo.setObjectId(info.getTaskId());
                vo.setObjectName(info.getName());
                vo.setObjectDeleted(task == null);
                break;
            case NODE:
                vo.setObjectId(info.getNodeId());
                vo.setObjectName(StringUtils.defaultIfBlank(info.getNode(), info.getName()));
                vo.setObjectDeleted(task == null);
                break;
            case API_SERVER:
                vo.setObjectId(StringUtils.defaultIfBlank(info.getApiId(), info.getServerId()));
                vo.setObjectName(info.getName());
                break;
            case CONNECTION:
                vo.setObjectId(info.getConnectionId());
                vo.setObjectName(info.getName());
                break;
            case AGENT:
            default:
                vo.setObjectId(info.getAgentId());
                vo.setObjectName(StringUtils.defaultIfBlank(info.getName(), info.getAgentId()));
                break;
        }
        return vo;
    }

    private String render(AlarmInfo info, Locale locale) {
        try {
            String summary = alarmService.renderSummary(info, locale);
            return StringUtils.isNotBlank(summary) ? summary : info.getSummary();
        } catch (Exception e) {
            log.debug("Render alarm summary failed, id: {}", info.getId(), e);
            return info.getSummary();
        }
    }
}
