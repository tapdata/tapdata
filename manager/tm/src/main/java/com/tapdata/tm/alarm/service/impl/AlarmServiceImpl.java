package com.tapdata.tm.alarm.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

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
            BeanUtil.copyProperties(info, one);
            one.setTally(one.getTally() + 1);
            one.setLastUpdAt(DateUtil.date());

            mongoTemplate.save(one);
        } else {
            mongoTemplate.insert(info);
        }
    }

    @Override
    public boolean checkOpen(String taskId, String key, String type) {
        boolean openTask = false;
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
        if (Objects.nonNull(taskDto)) {
            List<AlarmSettingDto> alarmSettingDtos = Lists.newArrayList();
            alarmSettingDtos.addAll(taskDto.getAlarmSettings());

            taskDto.getDag().getNodes().forEach(node -> {
                if (node instanceof DatabaseNode) {
                    alarmSettingDtos.addAll(((DatabaseNode) node).getAlarmSettings());
                } else if (node instanceof TableRenameProcessNode) {
                    alarmSettingDtos.addAll(((TableRenameProcessNode) node).getAlarmSettings());
                }
            });
            if (CollectionUtils.isNotEmpty(alarmSettingDtos)) {
                openTask = alarmSettingDtos.stream().anyMatch(t ->
                        t.getKey().equals(key) && "SYS".equals(type) ? t.isSystemNotify() : t.isEmailNotify());
            }
        }


        boolean openSys = false;
        List<AlarmSettingDto> all = alarmSettingService.findAll();
        if (CollectionUtils.isNotEmpty(all)) {
            openSys = all.stream().anyMatch(t ->
                    t.getKey().equals(key) && "SYS".equals(type) ? t.isSystemNotify() : t.isEmailNotify());
        }

        return openTask && openSys;
    }
}
