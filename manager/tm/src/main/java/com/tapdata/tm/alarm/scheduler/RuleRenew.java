package com.tapdata.tm.alarm.scheduler;

import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dexter
 */
@Component
@Setter(onMethod_ = {@Autowired})
public class RuleRenew {
    private static String KEY_FORMAT = "%s-%s";

    @Getter
    private final Map<String, Rule> executableRules = new HashMap<>();

    private AlarmService alarmService;

    public void renewTaskAlertRule(SyncTaskStatusDto dto) {
        if (!StringUtils.equalsAny(dto.getTaskStatus(),
                TaskDto.STATUS_RUNNING, TaskDto.STATUS_STOP,
                TaskDto.STATUS_ERROR, TaskDto.STATUS_COMPLETE,
                TaskDto.STATUS_STOPPING
        )) return;

        List<Rule> rules =  alarmService.findAllRuleWithMoreInfo(dto.getTaskId());
        // TODO(dexter): removed rules can not be found
        for (Rule rule : rules) {
            String key = getTaskRuleKey(dto.getTaskId(), rule);
            switch (dto.getTaskStatus()) {
                case TaskDto.STATUS_RUNNING:

                    executableRules.put(key, rule);
                    break;
                case TaskDto.STATUS_STOP:
                case TaskDto.STATUS_ERROR:
                case TaskDto.STATUS_COMPLETE:
                case TaskDto.STATUS_STOPPING:
                    executableRules.remove(key);
                    break;
                default:
            }
        }


        System.out.println();
    }

    private String getTaskRuleKey(String taskId, Rule rule) {
        return String.format(KEY_FORMAT, rule.getKey().name(), taskId);
    }


}
