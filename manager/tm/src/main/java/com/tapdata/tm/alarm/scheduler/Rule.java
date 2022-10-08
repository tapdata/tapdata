package com.tapdata.tm.alarm.scheduler;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Dexter
 */
@Slf4j
@Data
public class Rule {
    private AlarmKeyEnum key;
    private int point;
    private int equalsFlag;
    private int ms;

    // these two attributes should be computed by key
    private String metric;
    private Map<String, String> tags;

    public static Rule fromAlarmRuleDto(AlarmRuleDto dto) {
        Rule rule = new Rule();
        rule.setKey(dto.getKey());
        rule.setPoint(dto.getPoint());
        rule.setEqualsFlag(dto.getEqualsFlag());
        rule.setMs(dto.getMs());

        return rule;
    }

    public static Rule fromAlarmRuleDto(AlarmRuleDto dto, TaskDto task) {
        Rule rule = fromAlarmRuleDto(dto);


        return rule;
    }

    public boolean shouldExec() {
        return StringUtils.equals(key.getType(), AlarmKeyEnum.Constant.TYPE_METRIC);
    }

    public String getMetric() {
        if (!shouldExec()) {
            throw new RuntimeException(String.format("should not get metric for rule with type %s", key.getType()));
        }

        if (null != metric) {
            return metric;
        }

        computeMetricAndTags();
        return metric;
    }

    public Map<String, String> getTags() {
        if (!shouldExec()) {
            throw new RuntimeException(String.format("should not get tags for rule with type %s", key.getType()));
        }

        if (null != tags) {
            return tags;
        }

        computeMetricAndTags();
        return tags;
    }

    private void computeMetricAndTags() {
        if (!shouldExec()) {
            throw new RuntimeException(String.format("should not compute metric and tags for rule with type %s", key.getType()));
        }

        switch (key) {
            case SYSTEM_FLOW_EGINGE_DOWN:
                this.metric = "";
                this.tags = new HashMap<String, String>(){{

                }};
                break;
            case TASK_INCREMENT_DELAY:
                this.metric = "";
                this.tags = new HashMap<String, String>(){{

                }};
                break;
            case DATANODE_CANNOT_CONNECT:
                this.metric = "";
                this.tags = new HashMap<String, String>(){{

                }};
                break;
            case DATANODE_HTTP_CONNECT_CONSUME:
                this.metric = "";
                this.tags = new HashMap<String, String>(){{

                }};
                break;
            case DATANODE_TCP_CONNECT_CONSUME:
                this.metric = "";
                this.tags = new HashMap<String, String>(){{

                }};
                break;
            case DATANODE_AVERAGE_HANDLE_CONSUME:
                this.metric = "";
                this.tags = new HashMap<String, String>(){{

                }};
                break;
            case PROCESSNODE_AVERAGE_HANDLE_CONSUME:
                this.metric = "";
                this.tags = new HashMap<String, String>(){{

                }};
                break;
            default:
                log.warn("Unknown type of rule key {}, may be missed in process, please have a check.", key.name());
        }

    }
}
