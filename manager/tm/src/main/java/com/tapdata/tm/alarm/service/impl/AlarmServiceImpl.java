package com.tapdata.tm.alarm.service.impl;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Assert;
import com.google.common.collect.Maps;
import com.tapdata.tm.Settings.dto.MailAccountDto;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.alarm.constant.AlarmMailTemplate;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.dto.*;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.alarmrule.service.AlarmRuleService;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.TmPageable;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MessageMetadata;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.constant.SystemEnum;
import com.tapdata.tm.message.entity.MessageEntity;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MailUtils;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author jiuyetx
 * @date 2022/9/7
 */
@Service
@Setter(onMethod_ = {@Autowired})
public class AlarmServiceImpl implements AlarmService {

    private MongoTemplate mongoTemplate;
    private TaskService taskService;
    private AlarmSettingService alarmSettingService;
    private AlarmRuleService alarmRuleService;
    private MessageService messageService;
    private SettingsService settingsService;
    private UserService userService;

    @Override
    public void save(AlarmInfo info) {
        Criteria criteria = Criteria.where("taskId").is(info.getTaskId()).and("metric").is(info.getMetric())
                .and("level").is(info.getLevel());
        if (StringUtils.isNotBlank(info.getNodeId())) {
            criteria.and("nodeId").is(info.getNodeId());
        }
        Query query = new Query(criteria);
        AlarmInfo one = mongoTemplate.findOne(query, AlarmInfo.class);
        if (Objects.nonNull(one)) {
            one.setSummary(info.getSummary());
            one.setTally(one.getTally() + 1);
            one.setLastUpdAt(DateUtil.date());
            one.setLastOccurrenceTime(DateUtil.date());
            one.setStatus(info.getStatus());

            mongoTemplate.save(one);
        } else {
            info.setFirstOccurrenceTime(DateUtil.date());
            mongoTemplate.insert(info);
        }
    }

    @Override
    public boolean checkOpen(String taskId, String nodeId, AlarmKeyEnum key, NotifyEnum type) {
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        return checkOpen(taskDto, nodeId, key, type);
    }

    private boolean checkOpen(TaskDto taskDto, String nodeId, AlarmKeyEnum key, NotifyEnum type) {
        boolean openTask = false;
        if (Objects.nonNull(taskDto) && CollectionUtils.isNotEmpty(taskDto.getAlarmSettings())) {
            List<AlarmSettingDto> alarmSettingDtos = getAlarmSettingDtos(taskDto, nodeId);
            if (CollectionUtils.isNotEmpty(alarmSettingDtos)) {
                openTask = alarmSettingDtos.stream().anyMatch(t ->
                        t.getKey().equals(key) && t.isOpen() && t.getNotify().contains(type));
            }
        }

        boolean openSys = false;
        List<AlarmSettingDto> all = alarmSettingService.findAll();
        if (CollectionUtils.isNotEmpty(all)) {
            openSys = all.stream().anyMatch(t ->
                    t.getKey().equals(key) && t.isOpen() && t.getNotify().contains(type));
        }

        return openTask && openSys;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private static List<AlarmSettingDto> getAlarmSettingDtos(TaskDto taskDto, String nodeId) {
        List<AlarmSettingDto> alarmSettingDtos = Lists.newArrayList();
        alarmSettingDtos.addAll(taskDto.getAlarmSettings());

        if (Objects.nonNull(nodeId)) {
            for (Node node : taskDto.getDag().getNodes()) {
                if (node.getId().equals(nodeId)) {
                    alarmSettingDtos.addAll(node.getAlarmSettings());
                    break;
                }
            }
        } else {
            taskDto.getDag().getNodes().forEach(node -> {
                if (CollectionUtils.isNotEmpty(node.getAlarmSettings())) {
                    alarmSettingDtos.addAll(node.getAlarmSettings());
                }
            });
        }


        return alarmSettingDtos;
    }

    private AlarmSettingDto getAlarmSettingByKey(TaskDto taskDto, String nodeId, AlarmKeyEnum key) {
        List<AlarmSettingDto> alarmSettingDtos = getAlarmSettingDtos(taskDto, nodeId);
        Assert.notEmpty(alarmSettingDtos);
        return alarmSettingDtos.stream().filter(t -> key.equals(t.getKey())).findAny().orElse(null);
    }

    @Override
    public List<AlarmRuleDto> findAllRule(String taskId) {
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        return getAlarmRuleDtos(taskDto);
    }

    @Nullable
    private ArrayList<AlarmRuleDto> getAlarmRuleDtos(TaskDto taskDto) {
        if (Objects.nonNull(taskDto)) {
            List<AlarmRuleDto> ruleDtos = Lists.newArrayList();
            ruleDtos.addAll(taskDto.getAlarmRules());

            taskDto.getDag().getNodes().forEach(node -> {
                if (node instanceof DatabaseNode) {
                    ruleDtos.addAll(((DatabaseNode) node).getAlarmRules());
                } else if (node instanceof TableRenameProcessNode) {
                    ruleDtos.addAll(((TableRenameProcessNode) node).getAlarmRules());
                }
            });
            if (CollectionUtils.isNotEmpty(ruleDtos)) {
                Map<AlarmKeyEnum, AlarmRuleDto> collect = ruleDtos.stream()
                        .collect(Collectors.toMap(AlarmRuleDto::getKey, Function.identity(), (e1, e2) -> e1));

                List<AlarmRuleDto> alarmRuleDtos = alarmRuleService.findAll();
                if (CollectionUtils.isNotEmpty(alarmRuleDtos)) {
                    alarmRuleDtos.forEach(t -> {
                        if (!collect.containsKey(t.getKey())) {
                            collect.remove(t.getKey());
                        }
                    });
                }

                return new ArrayList<>(collect.values());
            }
        }
        return null;
    }

    @Override
    public void notifyAlarm() {
        Criteria criteria = Criteria.where("status").ne(AlarmStatusEnum.CLOESE);
        criteria.orOperator(Criteria.where("lastNotifyTime").is(null), Criteria.where("lastNotifyTime").lte(DateUtil.date()));
        Query needNotifyQuery = new Query(criteria);
        List<AlarmInfo> alarmInfos = mongoTemplate.find(needNotifyQuery, AlarmInfo.class);

        if (CollectionUtils.isEmpty(alarmInfos)) {
            return;
        }

        List<String> taskIds = alarmInfos.stream().map(AlarmInfo::getTaskId).collect(Collectors.toList());
        List<TaskDto> tasks = taskService.findAllTasksByIds(taskIds);
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        Map<String, TaskDto> taskDtoMap = tasks.stream().collect(Collectors.toMap(t -> t.getId().toHexString(), Function.identity(), (e1, e2) -> e1));

        alarmInfos.forEach(info -> {
            TaskDto taskDto = taskDtoMap.get(info.getTaskId());

            CompletableFuture.runAsync(() -> sendMessage(info, taskDto));

            CompletableFuture.runAsync(() -> sendMail(info, taskDto));

        });

    }

    private void sendMessage(AlarmInfo info, TaskDto taskDto) {
        if (checkOpen(taskDto, info.getNodeId(), info.getMetric(), NotifyEnum.SYSTEM)) {
            String taskId = taskDto.getId().toHexString();

            MessageEntity messageEntity = new MessageEntity();
            messageEntity.setLevel(info.getLevel().name());
            messageEntity.setAgentId(taskDto.getAgentId());
            messageEntity.setServerName(taskDto.getAgentId());
            messageEntity.setMsg(MsgTypeEnum.ALARM.getValue());
            String title = StringUtils.replace(info.getSummary(),"$taskName", info.getName());
            messageEntity.setTitle(title);
//                messageEntity.setSourceId();
            MessageMetadata metadata = new MessageMetadata(taskDto.getName(), taskId);
            messageEntity.setMessageMetadata(metadata);
            messageEntity.setSystem(SystemEnum.MIGRATION.getValue());
            messageEntity.setCreateAt(new Date());
            messageEntity.setLastUpdAt(new Date());
            messageEntity.setUserId(taskDto.getUserId());
            messageEntity.setRead(false);
            messageService.addMessage(messageEntity);

            // update alarmInfo date
            AlarmSettingDto setting = getAlarmSettingByKey(taskDto, info.getNodeId(), info.getMetric());
            DateTime lastNotifyTime = DateUtil.offset(DateUtil.date(), parseDateUnit(setting.getUnit()), setting.getInterval());
            info.setLastNotifyTime(lastNotifyTime);
            mongoTemplate.save(info);
        }
    }

    private void sendMail(AlarmInfo info, TaskDto taskDto) {
        if (checkOpen(taskDto, info.getNodeId(), info.getMetric(), NotifyEnum.EMAIL)) {
            String title;
            String content;
            switch (info.getMetric()) {
                case TASK_STATUS_STOP:
                    boolean manual = info.getSummary().contains("已被用户");
                    title = manual ? MessageFormat.format(AlarmMailTemplate.TASK_STATUS_STOP_MANUAL_TITLE, info.getName())
                            : MessageFormat.format(AlarmMailTemplate.TASK_STATUS_STOP_ERROR_TITLE, info.getName());

                    String userName = "";
                    if (Objects.nonNull(info.getCloseBy())) {
                        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(info.getCloseBy()));
                        userName = userDetail.getUsername();
                    }

                    content = manual ? MessageFormat.format(AlarmMailTemplate.TASK_STATUS_STOP_MANUAL, info.getName(), info.getFirstOccurrenceTime(), userName)
                            : MessageFormat.format(AlarmMailTemplate.TASK_STATUS_STOP_ERROR, info.getName(), info.getFirstOccurrenceTime());

                    MailAccountDto mailAccount = getMailAccount();
                    MailUtils.sendHtmlEmail(mailAccount, mailAccount.getReceivers(), title, content);

                    break;
                case TASK_STATUS_ERROR:

                    break;
                default:

            }

        }
    }

    private DateField parseDateUnit(DateUnit dateUnit) {
        if (Objects.isNull(dateUnit)) {
            return DateField.MILLISECOND;
        }

        if (dateUnit == DateUnit.MS) {
            return DateField.MILLISECOND;
        } else if (dateUnit == DateUnit.SECOND) {
            return DateField.SECOND;
        } else if (dateUnit == DateUnit.MINUTE) {
            return DateField.MINUTE;
        } else if (dateUnit == DateUnit.HOUR) {
            return DateField.HOUR;
        } else if (dateUnit == DateUnit.WEEK) {
            return DateField.WEEK_OF_MONTH;
        }

        return DateField.MILLISECOND;
    }

    @Override
    public void close(String[] ids, UserDetail userDetail) {
        List<ObjectId> collect = Arrays.stream(ids).map(MongoUtils::toObjectId).collect(Collectors.toList());

        Query query = new Query(Criteria.where("_id").in(collect));
        Update update = new Update().set("status", AlarmStatusEnum.CLOESE.name())
                .set("closeTime", DateUtil.date())
                .set("closeBy", userDetail.getUserId())
                ;
        mongoTemplate.updateFirst(query, update, AlarmInfo.class);
    }

    @Override
    public Page<AlarmListInfoVo> list(String status, Long start, Long end, String keyword, Integer page, Integer size, UserDetail userDetail) {
        TmPageable pageable = new TmPageable();
        pageable.setPage(page);
        pageable.setSize(size);

        Query query = new Query();
        long count = mongoTemplate.count(query, AlarmInfo.class);

        List<AlarmInfo> alarmInfos = mongoTemplate.find(query.with(pageable), AlarmInfo.class);

        List<AlarmListInfoVo> collect = alarmInfos.stream()
                .map(t -> AlarmListInfoVo.builder()
                        .id(t.getId().toHexString())
                        .level(t.getLevel())
                        .status(t.getStatus())
                        .name(t.getName())
                        .summary(StringUtils.replace(t.getSummary(), "$taskName", t.getName()))
                        .firstOccurrenceTime(t.getFirstOccurrenceTime())
                        .lastOccurrenceTime(t.getLastOccurrenceTime())
                        .taskId(t.getTaskId())
                        .metric(t.getMetric())
                .build()).collect(Collectors.toList());

        return new Page<>(count, collect);
    }

    @Override
    public TaskAlarmInfoVo listByTask(AlarmListReqDto dto) {
        String taskId = dto.getTaskId();
        AlarmStatusEnum status = dto.getStatus();
        Level level = dto.getLevel();
        String nodeId = dto.getNodeId();

        Criteria criteria = Criteria.where("taskId").is(taskId);
        if (Objects.nonNull(status)) {
            criteria.and("status").is(status.name());
        } else {
            criteria.and("status").ne(AlarmStatusEnum.CLOESE.name());
        }
        if (Objects.nonNull(level)) {
            criteria.and("level").is(level.name());
        }
        if (Objects.nonNull(nodeId)) {
            criteria.and("nodeId").is(nodeId);
        }
        Query query = new Query(criteria);
        List<AlarmInfo> alarmInfos = mongoTemplate.find(query, AlarmInfo.class);

        Map<String, Integer> nodeNumMap = Maps.newHashMap();
        List<AlarmListInfoVo> collect = Lists.newArrayList();
        alarmInfos.forEach( t -> {
            AlarmListInfoVo build = AlarmListInfoVo.builder()
                    .id(t.getId().toHexString())
                    .level(t.getLevel())
                    .status(t.getStatus())
                    .name(t.getName())
                    .summary(StringUtils.replace(t.getSummary(), "$taskName", t.getName()))
                    .firstOccurrenceTime(t.getFirstOccurrenceTime())
                    .lastOccurrenceTime(t.getLastOccurrenceTime())
                    .taskId(t.getTaskId())
                    .metric(t.getMetric())
                    .build();

            collect.add(build);

            String nId = t.getNodeId();
            if (Objects.nonNull(nId)) {
                if (nodeNumMap.containsKey(nId)) {
                    nodeNumMap.put(nId, nodeNumMap.get(nId) + 1);
                } else {
                    nodeNumMap.put(nId, 1);
                }
            }
        });

        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        List<TaskAlarmNodeInfoVo> taskAlarmNodeInfoVos = taskDto.getDag().getNodes().stream().map(t ->
                TaskAlarmNodeInfoVo.builder()
                        .nodeId(t.getId())
                        .nodeName(t.getName())
                        .num(nodeNumMap.get(t.getId()))
                        .build()
        ).collect(Collectors.toList());

        long alert = alarmInfos.stream().filter(t -> !AlarmStatusEnum.CLOESE.equals(t.getStatus())
                && Lists.of(Level.NORMAL, Level.WARNING).contains(t.getLevel())).count();
        long error = alarmInfos.stream().filter(t -> !AlarmStatusEnum.CLOESE.equals(t.getStatus())
                && Lists.of(Level.CRITICAL, Level.EMERGENCY).contains(t.getLevel())).count();

        return TaskAlarmInfoVo.builder()
                .nodeInfos(taskAlarmNodeInfoVos)
                .alarmList(collect)
                .alarmNum(new AlarmNumVo(alert, error))
                .build();
    }

    private MailAccountDto getMailAccount() {
        List<Settings> all = settingsService.findAll();
        Map<String, Settings> collect = all.stream().collect(Collectors.toMap(Settings::getKey, Function.identity(), (e1, e2) -> e1));

        String host = (String) collect.get("smtp.server.host").getValue();
        Integer port = (Integer) collect.get("smtp.server.port").getValue();
        String from = (String) collect.get("email.send.address").getValue();
        String user = (String) collect.get("smtp.server.user").getValue();
        Object pwd = collect.get("smtp.server.password").getValue();
        String password = Objects.nonNull(pwd) ? pwd.toString() : null;
        String receivers = (String) collect.get("email.receivers").getValue();
        String[] split = receivers.split(",");

        return MailAccountDto.builder().host(host).port(port).from(from).user(user).pass(password).receivers(Arrays.asList(split)).build();
    }

}
